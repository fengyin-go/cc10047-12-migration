"""主键定义版本领域服务。

把湖表的主键规则沉淀为可核对的版本（V1：仅 order_id；V2：merchant_id + order_id），
旧版本在新版本启用后仍可查询原规则。湖表定义与批流合并两个入口都通过
:mod:`app.key_versioning` 的共享注册表消费同一份版本定义。
"""
from __future__ import annotations

from typing import Any

from app.key_versioning import (
    DEFAULT_EFFECTIVE_BATCH,
    KEY_FORMAT_TEMPLATE,
    KEY_VERSION_V1,
    KEY_VERSION_V2,
    KeyDefinitionSpec,
    V1_KEY_COLUMNS,
    V2_KEY_COLUMNS,
    key_version_registry,
)
from app.services.base import BaseModuleService
from app.store import store


def _version_columns(key_version: str) -> list[str]:
    if key_version == KEY_VERSION_V1:
        return list(V1_KEY_COLUMNS)
    if key_version == KEY_VERSION_V2:
        return list(V2_KEY_COLUMNS)
    return []


class KeyDefinitionService(BaseModuleService):
    MODULE = "key_definition"
    ENTITY = "主键定义版本"
    OPERATION_TERM = "登记"
    LIST_FIELDS = [
        "定义标识", "目标湖表", "主键版本", "主键列", "键格式模板",
        "兼容模式", "生效批次", "登记时间",
    ]
    REQUIRED_FIELDS = ["定义标识", "目标湖表", "主键版本"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用版本": "已启用", "停用版本": "已停用"}
    NEGATIVE_ACTIONS = ["停用版本"]

    # ------------------------------------------------------------------
    # 与共享注册表同步
    # ------------------------------------------------------------------

    def bootstrap_registry(self) -> int:
        """把已登记的版本记录同步进共享注册表，返回同步条数。

        服务首次被调用时执行一次，保证种子数据 / 重启后注册表仍可核对。
        """
        count = 0
        for row in store.rows(self.MODULE):
            table_id = str(row.get("目标湖表", ""))
            key_version = str(row.get("主键版本", ""))
            if not table_id or not key_version:
                continue
            spec = self._spec_from_row(table_id, key_version, row)
            key_version_registry.register(spec)
            count += 1
        return count

    def _spec_from_row(
        self, table_id: str, key_version: str, row: dict[str, Any],
    ) -> KeyDefinitionSpec:
        effective_batch = int(row.get("生效批次") or DEFAULT_EFFECTIVE_BATCH)
        return KeyDefinitionSpec(
            table_id=table_id,
            key_version=key_version,
            key_columns=_version_columns(key_version) or str(row.get("主键列", "")).split(","),
            key_format=str(row.get("键格式模板") or KEY_FORMAT_TEMPLATE.get(key_version, "")),
            compatibility_mode=str(row.get("兼容模式") or "存量兼容"),
            effective_batch=effective_batch,
            status=str(row.get("status") or "草稿"),
        )

    # ------------------------------------------------------------------
    # 动作流转：启用 / 停用同步到共享注册表
    # ------------------------------------------------------------------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        table_id = str(entry.get("目标湖表", ""))
        key_version = str(entry.get("主键版本", ""))
        if table_id and key_version:
            key_version_registry.register(self._spec_from_row(table_id, key_version, entry))
        return entry, message

    # ------------------------------------------------------------------
    # 领域方法
    # ------------------------------------------------------------------

    def domain_register_version(
        self,
        table_id: str,
        key_version: str,
        *,
        effective_batch: int = DEFAULT_EFFECTIVE_BATCH,
        compatibility_mode: str = "存量兼容",
    ) -> dict[str, Any]:
        """登记一个可核对的主键定义版本并写入共享注册表。"""
        if key_version not in (KEY_VERSION_V1, KEY_VERSION_V2):
            return {"ok": False, "message": f"不支持的主键版本：{key_version}（仅支持 V1/V2）"}

        rows = store.rows(self.MODULE)
        existing = next(
            (row for row in rows
             if str(row.get("目标湖表")) == table_id
             and str(row.get("主键版本")) == key_version),
            None,
        )
        definition_id = (
            str(existing.get("定义标识"))
            if existing
            else f"KD-{table_id}-{key_version}"
        )
        spec = KeyDefinitionSpec(
            table_id=table_id,
            key_version=key_version,
            key_columns=_version_columns(key_version),
            key_format=KEY_FORMAT_TEMPLATE[key_version],
            compatibility_mode=compatibility_mode,
            effective_batch=effective_batch,
            status="已启用",
        )
        key_version_registry.register(spec)

        if existing is None:
            entry = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "定义标识": definition_id,
                "目标湖表": table_id,
                "主键版本": key_version,
                "主键列": ",".join(spec.key_columns),
                "键格式模板": spec.key_format,
                "兼容模式": compatibility_mode,
                "生效批次": effective_batch,
                "登记时间": "2026-11-24",
                "status": "已启用",
                "pending": False,
                "abnormal": False,
            }
            rows.append(entry)
        else:
            entry = existing
            existing.update({
                "主键列": ",".join(spec.key_columns),
                "键格式模板": spec.key_format,
                "兼容模式": compatibility_mode,
                "生效批次": effective_batch,
                "status": "已启用",
                "pending": False,
            })

        return {
            "ok": True,
            "message": f"湖表 {table_id} 主键定义 {key_version} 已登记并可核对",
            "definition": spec.to_dict(),
            "entry": entry,
        }

    def domain_list_versions(self, table_id: str) -> dict[str, Any]:
        """列出湖表全部主键定义版本——旧版本（V1）启用 V2 后仍可查原规则。"""
        self.bootstrap_registry()
        specs = [spec.to_dict() for spec in key_version_registry.list_definitions(table_id)]
        active = key_version_registry.active_version(table_id)
        return {
            "ok": True,
            "table_id": table_id,
            "versions": specs,
            "active_version": active.key_version if active else None,
        }

    def domain_get_definition(self, table_id: str, key_version: str) -> dict[str, Any]:
        """按版本号回查某一版主键规则（含已停用的历史版本）。"""
        self.bootstrap_registry()
        spec = key_version_registry.get_definition(table_id, key_version)
        if spec is None:
            return {"ok": False, "message": f"湖表 {table_id} 不存在主键定义版本 {key_version}"}
        return {"ok": True, "definition": spec.to_dict()}

    def domain_resolve_for_batch(self, table_id: str, batch_no: int) -> dict[str, Any]:
        """解析某批次写入实际应采用的主键定义版本。"""
        self.bootstrap_registry()
        spec = key_version_registry.resolve_version(table_id, batch_no)
        if spec is None:
            return {"ok": False, "message": f"湖表 {table_id} 尚未登记任何主键定义版本"}
        return {
            "ok": True,
            "table_id": table_id,
            "batch_no": batch_no,
            "resolved_version": spec.key_version,
            "definition": spec.to_dict(),
        }
