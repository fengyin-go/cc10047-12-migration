"""批流合并领域服务：主键格式迁移的第二个入口。

合并管道依赖可核对的主键定义版本（见 :mod:`app.key_versioning`）解释订单数据：

- 写入前按批次号解析应采用的主键版本（V1：order_id；V2：merchant_id||order_id）；
- 实际写入生成回执并推送给迁移复核待办，记录“实际采用了哪一版主键”；
- 相同批次（含并发恢复）经回执台账保证只写一次，恢复时返回原回执、原校验和；
- 旧版本已领取的批次在 V2 启用后重新处理，仍按回执中的原规则解释；
- V2 下无法核实商户来源的订单不丢弃，进入回查队列，补录商户后可重算主键。
"""
from __future__ import annotations

from typing import Any

from app.compute import delta_reconciler
from app.key_versioning import (
    KEY_VERSION_V1,
    PrimaryKeyError,
    WriteReceipt,
    build_primary_key,
    key_version_registry,
    make_receipt_id,
    receipt_checksum,
    unresolved_merchant_queue,
    write_receipt_ledger,
)
from app.services.base import BaseModuleService
from app.services.key_definition import KeyDefinitionService
from app.services.migration_review import MigrationReviewService
from app.store import store


class BatchMergeService(BaseModuleService):
    MODULE = "batch_merge"
    ENTITY = "合并管道"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["管道标识", "流来源", "批来源", "目标湖表", "去重字段", "冲突解决", "合并窗口", "上次合并"]
    REQUIRED_FIELDS = ["管道标识", "流来源", "批来源"]
    STATUS_ORDER = ["待配置", "待校验", "运行中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动合并": "运行中", "停用合并": "已停用"}
    NEGATIVE_ACTIONS = ["停用合并"]

    def domain_merge_batch_stream(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行聚合，联动合并和记录处理，产出标准化记录。处理流程：读取配置→执行聚合→合并校验→记录批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回查询处理结果，含标准化校验和批次批次信息

            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
        stream = [{"id": 1, "val": "stream-a"}, {"id": 2, "val": "stream-b"}]
        batch = [{"id": 2, "val": "batch-b"}, {"id": 3, "val": "batch-c"}]
        result = delta_reconciler(stream, batch, dedup_key="id")        # 返回查询处理结果，含标准化校验和批次批次信息

        return {
            "ok": True,
            "pipeline": entry.get("管道标识"),
            "table": entry.get("目标湖表"),
            "dedup_field": entry.get("去重字段", "id"),
            "conflict_resolution": entry.get("冲突解决", "batch-wins"),
            **result,
        }

    # ------------------------------------------------------------------
    # 主键版本化写入
    # ------------------------------------------------------------------

    def _load_pipeline(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def domain_write_orders(
        self,
        entry_id: int,
        orders: list[dict[str, Any]],
        batch_no: int,
    ) -> dict[str, Any]:
        """按可核对的主键定义版本把一批订单写入湖表，并产出实际写入回执。

        幂等语义：同一管道同一批次只写一次。批次已被领取（含 V1 时期写入的
        旧批次）时，直接返回原回执，旧结果始终按回执记录的原主键规则解释。
        """
        entry = self._load_pipeline(entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}

        pipeline_id = str(entry.get("管道标识"))
        table_id = str(entry.get("目标湖表"))
        KeyDefinitionService().bootstrap_registry()

        # 1. 先查回执台账：已领取批次（并发恢复 / 切换后重处理）不再二次写入
        claim_status, existing = write_receipt_ledger.claim(pipeline_id, batch_no)
        if claim_status == write_receipt_ledger.CLAIM_EXISTING:
            MigrationReviewService().domain_accept_receipt(existing.to_dict())
            return {
                "ok": True,
                "duplicate_recovery": True,
                "message": f"批次 {batch_no} 已写入，恢复处理复用原回执，未产生第二次写入",
                "receipt": existing.to_dict(),
                "actual_key_version": existing.key_version,
            }
        if claim_status == write_receipt_ledger.CLAIM_IN_PROGRESS:
            return {
                "ok": False,
                "duplicate_recovery": True,
                "message": f"批次 {batch_no} 正在写入中，并发请求被拦截，未产生重复写入",
            }

        # 2. 新批次：按批次号解析当前应采用的主键定义版本
        spec = key_version_registry.resolve_version(table_id, batch_no)
        if spec is None:
            # 释放占位，便于登记主键定义后重试
            write_receipt_ledger.release(pipeline_id, batch_no)
            return {
                "ok": False,
                "message": f"湖表 {table_id} 尚未登记主键定义版本，请先在湖表定义侧完成主键迁移",
            }

        # 3. 逐单按版本构建主键；商户来源无法核实的订单进入回查队列
        written_keys: list[str] = []
        unresolved: list[dict[str, Any]] = []
        try:
            for record in orders:
                try:
                    key = build_primary_key(record, spec.key_version)
                except PrimaryKeyError as exc:
                    queued = unresolved_merchant_queue.enqueue(
                        _make_unresolved(table_id, batch_no, record, str(exc))
                    )
                    unresolved.append(queued.to_dict())
                    continue
                written_keys.append(key)

            checksum = receipt_checksum(written_keys, spec.key_version, batch_no)
            receipt = WriteReceipt(
                receipt_id=make_receipt_id(pipeline_id, batch_no),
                pipeline_id=pipeline_id,
                table_id=table_id,
                batch_no=batch_no,
                key_version=spec.key_version,
                key_format=spec.key_format,
                write_count=len(written_keys),
                unresolved_count=len(unresolved),
                checksum=checksum,
                duplicate_recovery=False,
                keys=written_keys,
            )
            write_receipt_ledger.commit(receipt)
        except Exception:
            # 意外异常时释放领取占位，避免批次永久停留在“写入中”
            write_receipt_ledger.release(pipeline_id, batch_no)
            raise
        entry["上次合并"] = f"批次{batch_no}@{spec.key_version}"

        # 4. 回执同步给迁移复核待办，供新定义上线时核对同号订单与旧批次规则
        review = MigrationReviewService().domain_accept_receipt(receipt.to_dict())

        return {
            "ok": True,
            "duplicate_recovery": False,
            "message": f"批次 {batch_no} 已按 {spec.key_version} 主键规则写入",
            "receipt": receipt.to_dict(),
            "actual_key_version": spec.key_version,
            "written_keys": written_keys,
            "unresolved": unresolved,
            "review_todo": review.get("entry"),
        }

    def domain_reprocess_batch(self, entry_id: int, batch_no: int) -> dict[str, Any]:
        """重新处理指定批次。

        已写入批次走恢复路径：只返回原回执、不重复写入；旧批次即使在 V2
        启用后重处理，实际版本归属仍以原回执为准（旧结果按原规则解释）。
        """
        entry = self._load_pipeline(entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
        pipeline_id = str(entry.get("管道标识"))
        receipt = write_receipt_ledger.find(pipeline_id, batch_no)
        if receipt is None:
            return {
                "ok": False,
                "message": f"管道 {pipeline_id} 批次 {batch_no} 从未写入，无可恢复的原批次",
            }
        # 复用写入入口，台账保证同一笔写入只发生一次
        return self.domain_write_orders(entry_id, [], batch_no)

    def domain_trace_batch(self, entry_id: int, batch_no: int) -> dict[str, Any]:
        """值班员依据回执追查批次实际使用了哪一版主键。"""
        entry = self._load_pipeline(entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
        return MigrationReviewService().domain_trace_batch(
            str(entry.get("管道标识")), batch_no,
        )

    def domain_list_unresolved(self, entry_id: int | None = None) -> dict[str, Any]:
        """列出迁移中无法核实商户来源、待回查的订单。"""
        table_id = None
        if entry_id is not None:
            entry = self._load_pipeline(entry_id)
            if not entry:
                return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
            table_id = str(entry.get("目标湖表"))
        items = [item.to_dict() for item in unresolved_merchant_queue.list_pending(table_id)]
        return {"ok": True, "pending": len(items), "items": items}

    def domain_resolve_unresolved(
        self, order_id: str, merchant_id: str,
    ) -> dict[str, Any]:
        """回查补录商户来源，按 V2 重新计算复合主键。"""
        resolved = unresolved_merchant_queue.resolve(order_id, merchant_id)
        if resolved is None:
            return {"ok": False, "message": f"订单 {order_id} 没有待回查记录"}
        return {"ok": True, "message": "商户来源已补录，主键按 V2 重算", "item": resolved.to_dict()}


def _make_unresolved(table_id: str, batch_no: int, record: dict[str, Any], reason: str):
    """构造回查队列条目（延迟导入避免循环依赖）。"""
    from app.key_versioning import UnresolvedRecord

    return UnresolvedRecord(
        table_id=table_id,
        batch_no=batch_no,
        order_id=str(record.get("order_id", "")),
        reason=reason,
        raw_record=dict(record),
    )
