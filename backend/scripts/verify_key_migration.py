#!/usr/bin/env python3
"""主键 V1→V2 迁移交接场景的可核对验证脚本（仅依赖标准库 + 本应用）。

运行：
    cd backend && .venv/bin/python scripts/verify_key_migration.py

场景严格对应交接要求：
  1. V1 准备订单 19（保留商户 A 来源）；
  2. 启用 V2 后送入 A、B 两个商户的同号订单；
  3. 重新处理原批次两次，检查两种键格式、同一笔写入、实际版本归属；
  4. 校验回执追查、复核待办同步、并发恢复只写一次、商户来源回查。
"""
from __future__ import annotations

import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.key_versioning import (  # noqa: E402
    KEY_VERSION_V1,
    KEY_VERSION_V2,
    write_receipt_ledger,
)
from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"[{mark}] {label}" + (f" —— {detail}" if detail else ""))
    if not condition:
        raise SystemExit(f"断言失败：{label} {detail}")


def create_pipeline(table_id: str) -> int:
    rows = store.rows("batch_merge")
    entry = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": "运行中", "pending": False, "abnormal": False,
        "管道标识": f"pipe-{table_id}", "流来源": "orders-stream",
        "批来源": "orders-batch", "目标湖表": table_id,
        "去重字段": "order_id", "冲突解决": "batch-wins",
        "合并窗口": "5min", "上次合并": "",
    }
    rows.append(entry)
    return entry["id"]


def create_table(table_id: str) -> int:
    rows = store.rows("lake_table")
    entry = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": "草稿", "pending": True, "abnormal": False,
        "表标识": table_id, "表名称": table_id, "主键列": "order_id",
        "分区列": "dt", "存储格式": "iceberg", "压缩算法": "zstd",
        "表版本": "v1", "当前快照": "snap-0",
    }
    rows.append(entry)
    return entry["id"]


def main() -> None:
    table_id = "order_lake_handover"
    table_row_id = create_table(table_id)
    pipeline_id = create_pipeline(table_id)

    lake = service_for("lake_table")
    merge = service_for("batch_merge")
    keys = service_for("key_definition")
    review = service_for("migration_review")

    print("== 阶段0：V1 准备订单 19，保留商户 A 来源 ==")
    reg = keys.domain_register_version(table_id, KEY_VERSION_V1, effective_batch=1)
    check("V1 主键定义已登记", reg["ok"], reg.get("definition", {}).get("key_format", ""))
    r_v1 = merge.domain_write_orders(
        pipeline_id, [{"order_id": "19", "merchant_id": "A"}], batch_no=10,
    )
    check("V1 批次写入成功", r_v1["ok"])
    check("V1 实际版本归属为 V1", r_v1["actual_key_version"] == KEY_VERSION_V1)
    check("V1 键格式仅 order_id", r_v1["receipt"]["keys"] == ["19"], str(r_v1["receipt"]["keys"]))
    v1_receipt_id = r_v1["receipt"]["receipt_id"]
    v1_checksum = r_v1["receipt"]["checksum"]
    print(f"    回执 {v1_receipt_id} 校验和 {v1_checksum}")

    print("== 阶段1：启用 V2（湖表定义入口），V1 原规则仍可查 ==")
    migration = lake.domain_migrate_primary_key(
        table_row_id, target_version=KEY_VERSION_V2, effective_batch=11,
    )
    check("主键迁移成功", migration["ok"], migration.get("message", ""))
    versions = {item["key_version"] for item in migration["definitions"]}
    check("两版定义并存且可核对", versions == {KEY_VERSION_V1, KEY_VERSION_V2}, str(versions))
    v1_still = keys.domain_get_definition(table_id, KEY_VERSION_V1)
    check("旧版本 V1 仍能查到原主键规则", v1_still["ok"] and v1_still["definition"]["key_columns"] == ["order_id"])
    v2 = keys.domain_get_definition(table_id, KEY_VERSION_V2)
    check("V2 为商户+订单复合键", v2["definition"]["key_format"] == "{merchant_id}||{order_id}")

    print("== 阶段2：V2 送入 A、B 两个商户的同号订单 19 ==")
    r_v2 = merge.domain_write_orders(
        pipeline_id,
        [{"order_id": "19", "merchant_id": "A"}, {"order_id": "19", "merchant_id": "B"}],
        batch_no=11,
    )
    check("V2 批次写入成功", r_v2["ok"])
    check("V2 实际版本归属为 V2", r_v2["actual_key_version"] == KEY_VERSION_V2)
    check("同号订单产生两种可区分键格式", sorted(r_v2["written_keys"]) == ["A||19", "B||19"],
          str(r_v2["written_keys"]))

    print("== 阶段3：切换后重新处理原批次两次（同一笔写入，版本仍归 V1） ==")
    rp1 = merge.domain_reprocess_batch(pipeline_id, 10)
    rp2 = merge.domain_reprocess_batch(pipeline_id, 10)
    for idx, result in enumerate((rp1, rp2), start=1):
        check(f"第{idx}次重处理走恢复路径且未二次写入", result["duplicate_recovery"] is True)
        check(f"第{idx}次重处理回执仍是原回执", result["receipt"]["receipt_id"] == v1_receipt_id)
        check(f"第{idx}次重处理校验和一致", result["receipt"]["checksum"] == v1_checksum)
        check(f"第{idx}次重处理实际版本仍为 V1", result["actual_key_version"] == KEY_VERSION_V1)

    print("== 阶段4：值班员依据回执追查实际版本 + 复核待办同步 ==")
    trace = review.domain_trace_batch(f"pipe-{table_id}", 10)
    check("回执追查到实际版本 V1", trace["ok"] and trace["actual_key_version"] == KEY_VERSION_V1)
    check("追溯到关联复核待办", trace["review_todo"]["关联回执"] == v1_receipt_id)
    pending = review.domain_pending_reviews(table_id)
    receipt_ids = {row["关联回执"] for row in pending["items"]}
    check("复核待办已接收两批实际写入回执",
          {v1_receipt_id, r_v2["receipt"]["receipt_id"]}.issubset(receipt_ids), str(receipt_ids))

    print("== 阶段5：相同批次并发恢复只写一次 ==")
    barrier = threading.Barrier(5)
    outcomes: list[dict] = []

    def recover() -> None:
        barrier.wait()
        outcomes.append(merge.domain_reprocess_batch(pipeline_id, 10))

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(pool.map(lambda _: recover(), range(5)))
    check("5 个并发恢复全部命中同一回执",
          len(outcomes) == 5 and all(o["duplicate_recovery"] for o in outcomes))
    check("并发恢复只产生同一笔写入（校验和唯一）",
          len({o["receipt"]["checksum"] for o in outcomes}) == 1)
    receipt = write_receipt_ledger.find(f"pipe-{table_id}", 10)
    check("台账中批次写入条数保持为 1", receipt.write_count == 1)

    print("== 阶段6：迁移中无法核实商户来源的数据可回查 ==")
    r_missing = merge.domain_write_orders(
        pipeline_id, [{"order_id": "77"}, {"order_id": "78", "merchant_id": "A"}], batch_no=12,
    )
    check("缺商户订单未写入且计数", r_missing["receipt"]["unresolved_count"] == 1
          and r_missing["receipt"]["write_count"] == 1)
    unresolved = merge.domain_list_unresolved(pipeline_id)
    check("待回查队列可查到订单 77",
          unresolved["pending"] == 1 and unresolved["items"][0]["order_id"] == "77")
    resolved = merge.domain_resolve_unresolved("77", "C")
    check("补录商户后按 V2 重算主键", resolved["ok"] and resolved["item"]["resolution"] == "C||77")
    check("回查后队列清空", merge.domain_list_unresolved(pipeline_id)["pending"] == 0)

    print("\n全部核对通过：两种键格式可区分、同批次只写一次、旧批次按 V1 原规则解释、回执与复核待办可追溯。")


if __name__ == "__main__":
    main()
