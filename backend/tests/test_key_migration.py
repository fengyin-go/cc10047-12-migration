"""主键格式迁移测试：覆盖 V1→V2 版本化主键、批流合并回执、
同批次幂等恢复、旧批次按原规则解释、商户来源回查等核心约定。

不依赖 httpx，直接调用领域服务与共享注册表，可在仅安装 fastapi 的环境运行。
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, '.')

from app.key_versioning import (
    KEY_VERSION_V1,
    KEY_VERSION_V2,
    build_primary_key,
    key_version_registry,
    unresolved_merchant_queue,
    write_receipt_ledger,
)
from app.services import service_for
from app.store import store


def _fresh_pipeline(table_id: str) -> int:
    """登记一条指向指定湖表的批流合并管道，返回其 id。"""
    rows = store.rows("batch_merge")
    entry = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": "运行中",
        "pending": False,
        "abnormal": False,
        "管道标识": f"pipe-{table_id}",
        "流来源": "orders-stream",
        "批来源": "orders-batch",
        "目标湖表": table_id,
        "去重字段": "order_id",
        "冲突解决": "batch-wins",
        "合并窗口": "5min",
        "上次合并": "",
    }
    rows.append(entry)
    return entry["id"]


def _fresh_table(table_id: str) -> int:
    rows = store.rows("lake_table")
    entry = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": "草稿",
        "pending": True,
        "abnormal": False,
        "表标识": table_id,
        "表名称": table_id,
        "主键列": "order_id",
        "分区列": "dt",
        "存储格式": "iceberg",
        "压缩算法": "zstd",
        "表版本": "v1",
        "当前快照": "snap-0",
    }
    rows.append(entry)
    return entry["id"]


class TestKeyVersioningPrimitives:
    def test_v1_key_ignores_merchant(self):
        assert build_primary_key({"order_id": "19", "merchant_id": "A"}, KEY_VERSION_V1) == "19"

    def test_v2_key_distinguishes_merchants(self):
        key_a = build_primary_key({"order_id": "19", "merchant_id": "A"}, KEY_VERSION_V2)
        key_b = build_primary_key({"order_id": "19", "merchant_id": "B"}, KEY_VERSION_V2)
        assert key_a == "A||19"
        assert key_b == "B||19"
        assert key_a != key_b

    def test_v2_missing_merchant_raises(self):
        try:
            build_primary_key({"order_id": "19"}, KEY_VERSION_V2)
        except ValueError:
            return
        raise AssertionError("V2 缺少商户来源时应抛出主键构建错误")


class TestPrimaryKeyMigrationScenario:
    def test_full_handover_scenario(self):
        table_id = "order_lake_e2e"
        table_row_id = _fresh_table(table_id)
        pipeline_id = _fresh_pipeline(table_id)
        lake_service = service_for("lake_table")
        merge_service = service_for("batch_merge")
        key_service = service_for("key_definition")
        review_service = service_for("migration_review")

        # ---- 阶段0：V1 准备订单 19，保留商户 A 来源（V1 规则只看 order_id）----
        v1_register = key_service.domain_register_version(table_id, KEY_VERSION_V1, effective_batch=1)
        assert v1_register["ok"] is True
        r0 = merge_service.domain_write_orders(
            pipeline_id, [{"order_id": "19", "merchant_id": "A"}], batch_no=10,
        )
        assert r0["ok"] is True
        assert r0["actual_key_version"] == KEY_VERSION_V1
        assert r0["receipt"]["keys"] == ["19"]  # V1 键格式：仅 order_id
        first_receipt_id = r0["receipt"]["receipt_id"]
        first_checksum = r0["receipt"]["checksum"]

        # ---- 阶段1：启用 V2（湖表定义入口迁移），V1 仍可查原规则 ----
        migration = lake_service.domain_migrate_primary_key(
            table_row_id, target_version=KEY_VERSION_V2, effective_batch=11,
        )
        assert migration["ok"] is True
        versions = {item["key_version"] for item in migration["definitions"]}
        assert versions == {KEY_VERSION_V1, KEY_VERSION_V2}
        v1_lookup = key_service.domain_get_definition(table_id, KEY_VERSION_V1)
        assert v1_lookup["ok"] is True
        assert v1_lookup["definition"]["key_columns"] == ["order_id"]
        assert key_service.domain_get_definition(table_id, KEY_VERSION_V2)[
            "definition"
        ]["key_format"] == "{merchant_id}||{order_id}"

        # ---- 阶段2：V2 下送入 A、B 两个商户的同号订单 ----
        r1 = merge_service.domain_write_orders(
            pipeline_id,
            [
                {"order_id": "19", "merchant_id": "A"},
                {"order_id": "19", "merchant_id": "B"},
            ],
            batch_no=11,
        )
        assert r1["ok"] is True
        assert r1["actual_key_version"] == KEY_VERSION_V2
        assert sorted(r1["written_keys"]) == ["A||19", "B||19"]  # 两种键格式可区分

        # ---- 阶段3：切换后重新处理原批次两次 —— 同一笔写入，版本归属仍为 V1 ----
        r2 = merge_service.domain_reprocess_batch(pipeline_id, 10)
        r3 = merge_service.domain_reprocess_batch(pipeline_id, 10)
        for result in (r2, r3):
            assert result["ok"] is True
            assert result["duplicate_recovery"] is True
            assert result["receipt"]["receipt_id"] == first_receipt_id
            assert result["receipt"]["checksum"] == first_checksum
            assert result["actual_key_version"] == KEY_VERSION_V1

        # 回执台账中该批次始终只有一条
        receipt = write_receipt_ledger.find(f"pipe-{table_id}", 10)
        assert receipt is not None
        assert receipt.key_version == KEY_VERSION_V1
        assert receipt.write_count == 1

        # ---- 阶段4：值班员依据回执追查实际版本 ----
        trace = review_service.domain_trace_batch(f"pipe-{table_id}", 10)
        assert trace["ok"] is True
        assert trace["actual_key_version"] == KEY_VERSION_V1
        assert trace["review_todo"]["关联回执"] == first_receipt_id

        # ---- 复核待办已同步接收实际写入回执 ----
        pending = review_service.domain_pending_reviews(table_id)
        receipt_ids = {row["关联回执"] for row in pending["items"]}
        assert first_receipt_id in receipt_ids
        assert r1["receipt"]["receipt_id"] in receipt_ids

    def test_concurrent_recovery_writes_once(self):
        table_id = "order_lake_concurrent"
        pipeline_id = _fresh_pipeline(table_id)
        key_service = service_for("key_definition")
        merge_service = service_for("batch_merge")
        key_service.domain_register_version(table_id, KEY_VERSION_V1, effective_batch=1)

        first = merge_service.domain_write_orders(
            pipeline_id, [{"order_id": "19", "merchant_id": "A"}], batch_no=20,
        )
        assert first["duplicate_recovery"] is False

        results: list[dict] = []
        barrier = threading.Barrier(4)

        def recover() -> None:
            barrier.wait()
            results.append(merge_service.domain_reprocess_batch(pipeline_id, 20))

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: recover(), range(4)))

        assert len(results) == 4
        assert all(item["duplicate_recovery"] is True for item in results)
        assert len({item["receipt"]["checksum"] for item in results}) == 1
        receipt = write_receipt_ledger.find(f"pipe-{table_id}", 20)
        assert receipt is not None and receipt.write_count == 1

    def test_unresolved_merchant_can_be_rechecked(self):
        table_id = "order_lake_unresolved"
        pipeline_id = _fresh_pipeline(table_id)
        key_service = service_for("key_definition")
        merge_service = service_for("batch_merge")
        key_service.domain_register_version(table_id, KEY_VERSION_V2, effective_batch=1)

        result = merge_service.domain_write_orders(
            pipeline_id,
            [{"order_id": "88"}, {"order_id": "89", "merchant_id": "A"}],
            batch_no=30,
        )
        assert result["ok"] is True
        assert result["receipt"]["write_count"] == 1
        assert result["receipt"]["unresolved_count"] == 1

        pending = merge_service.domain_list_unresolved(pipeline_id)
        assert pending["pending"] == 1
        assert pending["items"][0]["order_id"] == "88"

        resolved = merge_service.domain_resolve_unresolved("88", "B")
        assert resolved["ok"] is True
        assert resolved["item"]["resolution"] == "B||88"
        assert merge_service.domain_list_unresolved(pipeline_id)["pending"] == 0

    def test_concurrent_first_write_writes_once(self):
        table_id = "order_lake_first_write"
        pipeline_id = _fresh_pipeline(table_id)
        key_service = service_for("key_definition")
        merge_service = service_for("batch_merge")
        key_service.domain_register_version(table_id, KEY_VERSION_V2, effective_batch=1)

        barrier = threading.Barrier(5)
        outcomes: list[dict] = []

        def first_write() -> None:
            barrier.wait()
            outcomes.append(
                merge_service.domain_write_orders(
                    pipeline_id, [{"order_id": "19", "merchant_id": "A"}], batch_no=50,
                )
            )

        with ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(lambda _: first_write(), range(5)))

        acquired = [o for o in outcomes if not o.get("duplicate_recovery") and o.get("ok")]
        # 落后线程可能命中“写入中”拦截或已提交回执的恢复路径，二者都不允许二次写入
        deduped = [o for o in outcomes if o is not acquired[0]]
        assert len(acquired) == 1
        assert all(
            o.get("ok") is False or o.get("duplicate_recovery") is True
            for o in deduped
        )
        receipt = write_receipt_ledger.find(f"pipe-{table_id}", 50)
        assert receipt is not None and receipt.write_count == 1
        recovered_checksums = {
            o["receipt"]["checksum"] for o in deduped if o.get("ok")
        }
        assert recovered_checksums <= {receipt.checksum}
        assert acquired[0]["actual_key_version"] == KEY_VERSION_V2

    def test_old_batch_claimed_under_v1_keeps_rule_after_v2(self):
        table_id = "order_lake_legacy"
        pipeline_id = _fresh_pipeline(table_id)
        key_service = service_for("key_definition")
        merge_service = service_for("batch_merge")

        key_service.domain_register_version(table_id, KEY_VERSION_V1, effective_batch=1)
        legacy = merge_service.domain_write_orders(
            pipeline_id, [{"order_id": "19", "merchant_id": "A"}], batch_no=40,
        )
        assert legacy["actual_key_version"] == KEY_VERSION_V1

        key_service.domain_register_version(table_id, KEY_VERSION_V2, effective_batch=41)
        # 新批次按 V2，旧批次重处理仍按 V1
        new_batch = merge_service.domain_write_orders(
            pipeline_id, [{"order_id": "19", "merchant_id": "B"}], batch_no=41,
        )
        assert new_batch["actual_key_version"] == KEY_VERSION_V2
        reprocessed = merge_service.domain_reprocess_batch(pipeline_id, 40)
        assert reprocessed["actual_key_version"] == KEY_VERSION_V1
        assert reprocessed["receipt"]["keys"] == ["19"]
