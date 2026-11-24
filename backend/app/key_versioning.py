"""湖表主键定义版本化核心域。

支持两代可核对的主键定义：

- V1：存量格式，主键仅由 ``order_id`` 构成（``order_id``）；
- V2：存量兼容格式，主键可区分 ``merchant_id`` 与 ``order_id``
  （``merchant_id||order_id``）。

模块同时维护三类可核对资产，供湖表定义与批流合并两个入口共用：

1. ``KeyVersionRegistry``：主键定义版本注册表，所有版本（含已停用的 V1）
   都保留并可按版本号 / 生效批次回查；
2. ``WriteReceiptLedger``：实际写入回执台账，按批次幂等记录实际采用的
   主键版本，相同批次并发恢复只写一次；
3. ``UnresolvedMerchantQueue``：迁移中无法核实商户来源的数据回查队列。

全部实现只依赖标准库，可在多线程服务端安全调用。
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import asdict, dataclass, field
from typing import Any

# ------------------------------------------------------------------
# 主键定义版本常量
# ------------------------------------------------------------------

KEY_VERSION_V1 = "V1"
KEY_VERSION_V2 = "V2"

#: 版本顺序：用于同一生效批次上比较“当前版本”
VERSION_ORDER = [KEY_VERSION_V1, KEY_VERSION_V2]

#: V1 存量主键列：仅订单号
V1_KEY_COLUMNS = ["order_id"]
#: V2 兼容主键列：商户 + 订单号
V2_KEY_COLUMNS = ["merchant_id", "order_id"]

#: 版本 → 键格式模板（可核对的主键定义版本）
KEY_FORMAT_TEMPLATE = {
    KEY_VERSION_V1: "{order_id}",
    KEY_VERSION_V2: "{merchant_id}||{order_id}",
}

#: V1 从平台第一个批次起即生效
DEFAULT_EFFECTIVE_BATCH = 1


class PrimaryKeyError(ValueError):
    """主键构建过程中的可预期错误（如 V2 缺少商户来源）。"""


@dataclass(frozen=True)
class KeyDefinitionSpec:
    """可核对的主键定义版本。"""

    table_id: str
    key_version: str
    key_columns: list[str]
    key_format: str
    compatibility_mode: str
    effective_batch: int
    status: str = "已启用"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_primary_key(record: dict[str, Any], key_version: str) -> str:
    """按指定主键版本为一条订单记录构建主键。

    - V1 只读取 ``order_id``，与商户来源无关，保留存量行为；
    - V2 同时要求 ``merchant_id`` 与 ``order_id``，商户来源无法核实时
      抛出 :class:`PrimaryKeyError`，由调用方进入回查队列。
    """
    order_id = str(record.get("order_id", "")).strip()
    if not order_id:
        raise PrimaryKeyError("订单记录缺少 order_id，无法构建主键")

    if key_version == KEY_VERSION_V1:
        return order_id

    if key_version == KEY_VERSION_V2:
        merchant_id = str(record.get("merchant_id", "")).strip()
        if not merchant_id:
            raise PrimaryKeyError(
                f"订单 {order_id} 无法核实商户来源（merchant_id 为空），"
                "不能按 V2 构建复合主键"
            )
        return f"{merchant_id}||{order_id}"

    raise PrimaryKeyError(f"未知主键定义版本：{key_version}")


class KeyVersionRegistry:
    """主键定义版本注册表（按湖表隔离）。

    旧版本（如 V1）在新版本启用后仍完整保留，仍能查到原主键规则；
    每个版本带生效批次，用于解释“某一批次实际应按哪版规则解释”。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # table_id -> {version -> KeyDefinitionSpec}
        self._definitions: dict[str, dict[str, KeyDefinitionSpec]] = {}

    def register(self, spec: KeyDefinitionSpec) -> KeyDefinitionSpec:
        """登记 / 覆盖一个主键定义版本，返回可核对的定义快照。"""
        with self._lock:
            self._definitions.setdefault(spec.table_id, {})[spec.key_version] = spec
            return spec

    def get_definition(self, table_id: str, key_version: str) -> KeyDefinitionSpec | None:
        with self._lock:
            return self._definitions.get(table_id, {}).get(key_version)

    def list_definitions(self, table_id: str) -> list[KeyDefinitionSpec]:
        """列出某湖表的全部主键定义版本（含历史版本），按版本顺序返回。"""
        with self._lock:
            versions = self._definitions.get(table_id, {})
            return [
                versions[name]
                for name in VERSION_ORDER
                if name in versions
            ] + [
                spec for name, spec in versions.items()
                if name not in VERSION_ORDER
            ]

    def resolve_version(self, table_id: str, batch_no: int) -> KeyDefinitionSpec | None:
        """解析某批次写入时应采用的主键版本。

        取 ``effective_batch <= batch_no`` 中版本顺序最高的已启用定义；
        找不到时回退到该表最早登记的定义，保证旧批次始终有规则可依。
        """
        with self._lock:
            definitions = self._definitions.get(table_id, {})
            eligible = [
                spec for spec in definitions.values()
                if spec.status == "已启用" and spec.effective_batch <= batch_no
            ]
            if eligible:
                return max(
                    eligible,
                    key=lambda spec: VERSION_ORDER.index(spec.key_version)
                    if spec.key_version in VERSION_ORDER
                    else len(VERSION_ORDER),
                )
            # 兜底：旧批次早于任何已登记生效批次时，仍返回最早版本
            ordered = self.list_definitions(table_id)
            return ordered[0] if ordered else None

    def active_version(self, table_id: str) -> KeyDefinitionSpec | None:
        """当前最新启用版本（不考虑批次）。"""
        with self._lock:
            definitions = [
                spec for spec in self._definitions.get(table_id, {}).values()
                if spec.status == "已启用"
            ]
            if not definitions:
                return None
            return max(
                definitions,
                key=lambda spec: VERSION_ORDER.index(spec.key_version)
                if spec.key_version in VERSION_ORDER
                else len(VERSION_ORDER),
            )


@dataclass(frozen=True)
class WriteReceipt:
    """一次实际写入的回执：记录批次实际采用的主键版本。"""

    receipt_id: str
    pipeline_id: str
    table_id: str
    batch_no: int
    key_version: str
    key_format: str
    write_count: int
    unresolved_count: int
    checksum: str
    duplicate_recovery: bool = False
    keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class WriteReceiptLedger:
    """写入回执台账：按 (管道, 批次) 幂等，相同批次并发恢复只写一次。"""

    #: claim 返回状态：本调用首次取得写入权
    CLAIM_ACQUIRED = "acquired"
    #: claim 返回状态：批次已有回执，调用方应走恢复路径
    CLAIM_EXISTING = "existing"
    #: claim 返回状态：另一写入方正在写入，调用方不得重复写
    CLAIM_IN_PROGRESS = "in_progress"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # (pipeline_id, batch_no) -> WriteReceipt | _PENDING 占位
        self._receipts: dict[tuple[str, int], WriteReceipt | object] = {}
        self._pending = object()

    def claim(
        self, pipeline_id: str, batch_no: int,
    ) -> tuple[str, WriteReceipt | None]:
        """领取批次，返回 ``(状态, 已有回执或 None)``。

        - ``existing``：批次已实际写入，回执随返回，恢复路径复用同一笔写入；
        - ``acquired``：本调用首次取得写入权，调用方负责随后 commit；
        - ``in_progress``：已有写入方领取但尚未提交，禁止并发第二次写入。
        """
        with self._lock:
            key = (pipeline_id, batch_no)
            current = self._receipts.get(key)
            if isinstance(current, WriteReceipt):
                return self.CLAIM_EXISTING, current
            if current is self._pending:
                return self.CLAIM_IN_PROGRESS, None
            self._receipts[key] = self._pending
            return self.CLAIM_ACQUIRED, None

    def release(self, pipeline_id: str, batch_no: int) -> None:
        """放弃写入权（如前置校验失败），允许后续重新领取。"""
        with self._lock:
            key = (pipeline_id, batch_no)
            if self._receipts.get(key) is self._pending:
                self._receipts.pop(key, None)

    def commit(self, receipt: WriteReceipt) -> WriteReceipt:
        with self._lock:
            self._receipts[(receipt.pipeline_id, receipt.batch_no)] = receipt
            return receipt

    def find(self, pipeline_id: str, batch_no: int) -> WriteReceipt | None:
        with self._lock:
            current = self._receipts.get((pipeline_id, batch_no))
            return current if isinstance(current, WriteReceipt) else None

    def list_receipts(self, pipeline_id: str | None = None) -> list[WriteReceipt]:
        with self._lock:
            receipts = [
                receipt for receipt in self._receipts.values()
                if isinstance(receipt, WriteReceipt)
                and (pipeline_id is None or receipt.pipeline_id == pipeline_id)
            ]
            return sorted(receipts, key=lambda item: (item.pipeline_id, item.batch_no))


@dataclass
class UnresolvedRecord:
    """无法核实商户来源、待回查的订单记录。"""

    table_id: str
    batch_no: int
    order_id: str
    reason: str
    raw_record: dict[str, Any]
    status: str = "待回查"
    resolution: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class UnresolvedMerchantQueue:
    """迁移中商户来源无法核实的数据回查队列。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: list[UnresolvedRecord] = []

    def enqueue(self, item: UnresolvedRecord) -> UnresolvedRecord:
        with self._lock:
            self._items.append(item)
            return item

    def list_pending(self, table_id: str | None = None) -> list[UnresolvedRecord]:
        with self._lock:
            return [
                item for item in self._items
                if item.status == "待回查"
                and (table_id is None or item.table_id == table_id)
            ]

    def resolve(self, order_id: str, merchant_id: str) -> UnresolvedRecord | None:
        """回查补录商户来源后，按 V2 重算主键并标记已回查。"""
        with self._lock:
            for item in self._items:
                if item.order_id == order_id and item.status == "待回查":
                    item.raw_record["merchant_id"] = merchant_id
                    item.status = "已回查"
                    item.resolution = build_primary_key(item.raw_record, KEY_VERSION_V2)
                    return item
            return None

    def all_items(self) -> list[UnresolvedRecord]:
        with self._lock:
            return list(self._items)


def receipt_checksum(keys: list[str], key_version: str, batch_no: int) -> str:
    """同一批订单 + 同一版主键规则产生稳定校验和，供恢复写入核对。"""
    payload = f"{key_version}|{batch_no}|{','.join(sorted(keys))}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def make_receipt_id(pipeline_id: str, batch_no: int) -> str:
    return f"RC-{pipeline_id}-B{batch_no:04d}"


# 全局单例：两个入口（湖表定义、批流合并）共享同一份可核对资产
key_version_registry = KeyVersionRegistry()
write_receipt_ledger = WriteReceiptLedger()
unresolved_merchant_queue = UnresolvedMerchantQueue()
