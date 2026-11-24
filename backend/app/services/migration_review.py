"""迁移复核待办领域服务。

批流合并每次实际写入后把回执推到这里，形成迁移复核待办。值班员在 V2
新定义上线后，可依据回执核对同号订单在不同商户下的落库情况，以及旧批次
实际采用的是哪一版主键规则。待办按回执幂等，相同批次并发恢复不会重复建单。
"""
from __future__ import annotations

from typing import Any

from app.key_versioning import write_receipt_ledger
from app.services.base import BaseModuleService
from app.store import store


class MigrationReviewService(BaseModuleService):
    MODULE = "migration_review"
    ENTITY = "迁移复核待办"
    OPERATION_TERM = "核对"
    LIST_FIELDS = [
        "待办标识", "目标湖表", "关联回执", "批次号", "主键版本",
        "同号订单", "核对状态", "创建时间",
    ]
    REQUIRED_FIELDS = ["待办标识", "目标湖表", "关联回执"]
    STATUS_ORDER = ["待核对", "核对中", "已核对", "已关闭"]
    ACTION_RULES = {"开始核对": "核对中", "确认核对": "已核对", "关闭待办": "已关闭"}
    NEGATIVE_ACTIONS = ["关闭待办"]

    def _find_by_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if str(row.get("关联回执")) == receipt_id:
                return row
        return None

    def domain_accept_receipt(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """接收批流合并的实际写入回执，幂等创建 / 更新复核待办。"""
        receipt_id = str(receipt.get("receipt_id", ""))
        if not receipt_id:
            return {"ok": False, "message": "回执缺少 receipt_id，无法创建复核待办"}

        rows = store.rows(self.MODULE)
        existing = self._find_by_receipt(receipt_id)
        duplicate_orders = sorted({
            str(key) for key in receipt.get("keys", [])
        })
        if existing is None:
            entry = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "待办标识": f"MR-{receipt_id}",
                "目标湖表": receipt.get("table_id"),
                "关联回执": receipt_id,
                "批次号": receipt.get("batch_no"),
                "主键版本": receipt.get("key_version"),
                "同号订单": ",".join(duplicate_orders),
                "写入条数": receipt.get("write_count"),
                "待回查条数": receipt.get("unresolved_count"),
                "校验和": receipt.get("checksum"),
                "恢复写入": bool(receipt.get("duplicate_recovery")),
                "核对状态": "待核对",
                "创建时间": "2026-11-24",
                "status": "待核对",
                "pending": True,
                "abnormal": False,
            }
            rows.append(entry)
            created = True
        else:
            entry = existing
            existing.update({
                "主键版本": receipt.get("key_version"),
                "同号订单": ",".join(duplicate_orders),
                "写入条数": receipt.get("write_count"),
                "待回查条数": receipt.get("unresolved_count"),
                "校验和": receipt.get("checksum"),
                "恢复写入": bool(receipt.get("duplicate_recovery")),
            })
            created = False

        return {
            "ok": True,
            "created": created,
            "message": "复核待办已创建" if created else "回执已接收，复核待办幂等更新",
            "entry": entry,
        }

    def domain_trace_batch(self, pipeline_id: str, batch_no: int) -> dict[str, Any]:
        """值班员追查某批次实际使用了哪一版主键（依据写入回执）。"""
        receipt = write_receipt_ledger.find(pipeline_id, batch_no)
        if receipt is None:
            return {
                "ok": False,
                "message": f"管道 {pipeline_id} 批次 {batch_no} 尚无写入回执",
            }
        todo = self._find_by_receipt(receipt.receipt_id)
        return {
            "ok": True,
            "receipt": receipt.to_dict(),
            "review_todo": todo,
            "actual_key_version": receipt.key_version,
            "key_format": receipt.key_format,
        }

    def domain_pending_reviews(self, table_id: str | None = None) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        if table_id:
            rows = [row for row in rows if str(row.get("目标湖表")) == table_id]
        pending = [row for row in rows if row.get("status") == "待核对"]
        return {
            "ok": True,
            "total": len(rows),
            "pending": len(pending),
            "items": pending,
        }
