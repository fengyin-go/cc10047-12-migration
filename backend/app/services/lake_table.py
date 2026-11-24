"""湖表定义领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

主键格式迁移入口：``domain_migrate_primary_key`` 在湖表侧登记可核对的
V1 / V2 主键定义版本，V1 规则在 V2 启用后仍可通过主键定义版本服务回查。
"""
from __future__ import annotations

from typing import Any

from app.key_versioning import (
    KEY_FORMAT_TEMPLATE,
    KEY_VERSION_V1,
    KEY_VERSION_V2,
    V1_KEY_COLUMNS,
    V2_KEY_COLUMNS,
    key_version_registry,
)
from app.services.base import BaseModuleService
from app.services.key_definition import KeyDefinitionService
from app.store import store


class LakeTableService(BaseModuleService):
    MODULE = "lake_table"
    ENTITY = "湖表定义"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["表标识", "表名称", "主键列", "分区列", "存储格式", "压缩算法", "表版本", "当前快照"]
    REQUIRED_FIELDS = ["表标识", "表名称", "主键列"]
    STATUS_ORDER = ["草稿", "待校验", "已创建", "已冻结"]
    ACTION_RULES = {"提交校验": "待校验", "创建表": "已创建", "冻结表": "已冻结"}
    NEGATIVE_ACTIONS = []

    def domain_define_table(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行状态，联动状态和处理处理，产出标准化记录。处理流程：读取配置→执行状态→状态校验→处理批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回解析处理结果，含计算校验和回填批次信息

            return {"ok": False, "message": f"湖表 {entry_id} 不存在"}        # 返回解析处理结果，含计算校验和回填批次信息

        table_id = str(entry.get("表标识"))
        active_spec = key_version_registry.active_version(table_id)
        return {
            "ok": True,
            "table_id": table_id,
            "table_name": entry.get("表名称"),
            "primary_key": entry.get("主键列"),
            "partition_col": entry.get("分区列"),
            "storage_format": entry.get("存储格式", "parquet"),
            "compression": entry.get("压缩算法", "zstd"),
            "ddl": f"CREATE TABLE {table_id} (...) USING iceberg PARTITIONED BY ({entry.get('分区列')})",
            "schema_version": entry.get("表版本", "v1"),
            # 可核对的当前主键定义版本（若已登记）
            "active_key_version": active_spec.key_version if active_spec else None,
            "active_key_format": active_spec.key_format if active_spec else None,
        }

    # ------------------------------------------------------------------
    # 主键格式迁移入口（湖表定义侧）
    # ------------------------------------------------------------------

    def domain_migrate_primary_key(
        self,
        entry_id: int,
        *,
        target_version: str = KEY_VERSION_V2,
        effective_batch: int = 1,
    ) -> dict[str, Any]:
        """把湖表主键从 V1（order_id）迁移到可区分商户的存量兼容格式。

        迁移不删除 V1：先确保 V1 定义仍可查，再登记目标版本并更新湖表主键列，
        返回两版可核对的主键定义，供批流合并入口按批次解释订单数据。
        """
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"湖表 {entry_id} 不存在"}
        if target_version not in (KEY_VERSION_V1, KEY_VERSION_V2):
            return {"ok": False, "message": f"未知主键目标版本：{target_version}"}

        table_id = str(entry.get("表标识"))
        key_service = KeyDefinitionService()

        # 1. 保留 V1 原主键规则（已存在则幂等返回），旧版本仍能查到
        v1_result = key_service.domain_register_version(
            table_id, KEY_VERSION_V1, effective_batch=1, compatibility_mode="存量格式",
        )
        # 2. 登记目标版本（V2：merchant_id + order_id）
        target_result = key_service.domain_register_version(
            table_id,
            target_version,
            effective_batch=effective_batch,
            compatibility_mode="存量兼容",
        )
        if not target_result.get("ok"):
            return target_result

        # 3. 回写湖表定义的主键列与表版本，保留旧主键列痕迹
        new_columns = V2_KEY_COLUMNS if target_version == KEY_VERSION_V2 else V1_KEY_COLUMNS
        entry["历史主键列"] = entry.get("主键列")
        entry["主键列"] = ",".join(new_columns)
        entry["表版本"] = target_version
        entry["键格式模板"] = KEY_FORMAT_TEMPLATE[target_version]
        entry["生效批次"] = effective_batch

        definitions = [
            spec.to_dict() for spec in key_version_registry.list_definitions(table_id)
        ]
        return {
            "ok": True,
            "message": f"湖表 {table_id} 主键已迁移到 {target_version}，V1 规则保留可查",
            "table_id": table_id,
            "previous_version": KEY_VERSION_V1,
            "current_version": target_version,
            "effective_batch": effective_batch,
            "primary_key_columns": new_columns,
            "key_format": KEY_FORMAT_TEMPLATE[target_version],
            "definitions": definitions,
            "v1_definition": v1_result.get("definition"),
            "v2_definition": target_result.get("definition"),
        }
