"""主键格式迁移专用路由。

把湖表定义与批流合并两个入口的迁移动作暴露为可核对的 HTTP 端点：
主键定义版本登记/回查、版本化订单写入、批次恢复与回执追查、商户来源回查。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.batch_merge import BatchMergeService
from app.services.key_definition import KeyDefinitionService
from app.services.lake_table import LakeTableService

router = APIRouter(prefix="/api/migration", tags=["主键格式迁移"])


class OrderWritePayload(BaseModel):
    orders: list[dict[str, Any]] = Field(default_factory=list)


class ResolvePayload(BaseModel):
    order_id: str
    merchant_id: str


# ------------------------------------------------------------------
# 入口一：湖表定义 —— 主键定义版本
# ------------------------------------------------------------------

@router.post("/tables/{entry_id}/migrate-primary-key")
def migrate_primary_key(
    entry_id: int, target_version: str = "V2", effective_batch: int = 1,
) -> dict[str, Any]:
    service: LakeTableService = service_for("lake_table")  # type: ignore[assignment]
    return service.domain_migrate_primary_key(
        entry_id, target_version=target_version, effective_batch=effective_batch,
    )


@router.post("/tables/{table_id}/key-definitions/{key_version}")
def register_key_definition(
    table_id: str, key_version: str, effective_batch: int = 1,
    compatibility_mode: str = "存量兼容",
) -> dict[str, Any]:
    service: KeyDefinitionService = service_for("key_definition")  # type: ignore[assignment]
    return service.domain_register_version(
        table_id,
        key_version,
        effective_batch=effective_batch,
        compatibility_mode=compatibility_mode,
    )


@router.get("/tables/{table_id}/key-definitions")
def list_key_definitions(table_id: str) -> dict[str, Any]:
    service: KeyDefinitionService = service_for("key_definition")  # type: ignore[assignment]
    return service.domain_list_versions(table_id)


@router.get("/tables/{table_id}/key-definitions/{key_version}")
def get_key_definition(table_id: str, key_version: str) -> dict[str, Any]:
    service: KeyDefinitionService = service_for("key_definition")  # type: ignore[assignment]
    return service.domain_get_definition(table_id, key_version)


@router.get("/tables/{table_id}/key-definitions/resolve/{batch_no}")
def resolve_key_for_batch(table_id: str, batch_no: int) -> dict[str, Any]:
    service: KeyDefinitionService = service_for("key_definition")  # type: ignore[assignment]
    return service.domain_resolve_for_batch(table_id, batch_no)


# ------------------------------------------------------------------
# 入口二：批流合并 —— 版本化写入、恢复、回执追查、回查
# ------------------------------------------------------------------

@router.post("/pipelines/{entry_id}/orders/batches/{batch_no}")
def write_orders(entry_id: int, batch_no: int, payload: OrderWritePayload) -> dict[str, Any]:
    service: BatchMergeService = service_for("batch_merge")  # type: ignore[assignment]
    return service.domain_write_orders(entry_id, payload.orders, batch_no)


@router.post("/pipelines/{entry_id}/batches/{batch_no}/reprocess")
def reprocess_batch(entry_id: int, batch_no: int) -> dict[str, Any]:
    service: BatchMergeService = service_for("batch_merge")  # type: ignore[assignment]
    return service.domain_reprocess_batch(entry_id, batch_no)


@router.get("/pipelines/{entry_id}/batches/{batch_no}/trace")
def trace_batch(entry_id: int, batch_no: int) -> dict[str, Any]:
    service: BatchMergeService = service_for("batch_merge")  # type: ignore[assignment]
    return service.domain_trace_batch(entry_id, batch_no)


@router.get("/pipelines/{entry_id}/unresolved")
def list_unresolved(entry_id: int) -> dict[str, Any]:
    service: BatchMergeService = service_for("batch_merge")  # type: ignore[assignment]
    return service.domain_list_unresolved(entry_id)


@router.post("/unresolved/resolve")
def resolve_unresolved(payload: ResolvePayload) -> dict[str, Any]:
    service: BatchMergeService = service_for("batch_merge")  # type: ignore[assignment]
    return service.domain_resolve_unresolved(payload.order_id, payload.merchant_id)
