#!/usr/bin/env python3
"""端到端验收：按 actions.json 的真实业务时间线演示完整补算编排。

时间线（对应 docs/input-contract.md）：
1. 基线原件入仓，建立 10-01..10-03 的 daily/balance/turnover 与第一套自洽快照；
2. step1 修订 10-01 为 90/30（补报回收明细）；
3. step2 明确清空 10-02（零行产物，区别于缺分区）；
4. step3 再次修订 10-01 为 95/30；
5. 影响分析 -> 精准补算 -> 领取第二套自洽快照并 DuckDB 验证数值；
6. 打印证据目录：分区修订、尝试状态、产物 current 指针、快照清单。

用法：python3 -I scripts/acceptance_demo.py [warehouse_dir]
"""
import shutil
import sqlite3
import sys
from collections import Counter
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from warehouse.api import open_warehouse  # noqa: E402
from warehouse.snapshot import SnapshotError  # noqa: E402


def show(con, sql, title):
    print(f"\n== {title} ==")
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.execute(sql).description] if False else None
    for r in rows:
        print(" ", dict(r))


def main():
    wh_dir = Path(sys.argv[1] if len(sys.argv) > 1 else ROOT / "var/acceptance").resolve()
    import shutil
    shutil.rmtree(wh_dir, ignore_errors=True)

    wh = open_warehouse(wh_dir, ROOT / "inputs/jobs.json")
    try:
        # 1) 基线
        for table in ("raw_day", "opening", "calendar"):
            wh.register_parquet(table, str(ROOT / f"inputs/{table}.parquet"))
        run = wh.plan("2026-09-25", "2026-10-03")
        baseline_exec = wh.work(run["run_id"], "baseline")
        snap1 = wh.claim("2026-10-01", "2026-10-03", note="基线自洽快照")
        print(f"[1] 基线：执行 {len(baseline_exec)} 节点，快照 {snap1['snapshot_id']}")

        # 2-4) 应用 actions 三步
        results = wh.apply_actions(str(ROOT / "inputs/actions.json"),
                                   str(ROOT / "inputs/raw_day.parquet"))
        for r in results:
            print(f"[action step{r['step']}] {r['table']}@{r['day']} "
                  f"rev{r['revision_no']} state={r['state']} rows={r['rows']}")

        # 5) 影响分析（以最新修订后的全部变更点传播到 10-03）
        impact = wh.impact("2026-10-03", [("raw_day", "2026-10-01"),
                                          ("raw_day", "2026-10-02")])
        print(f"\n[impact] 受影响节点 {len(impact['affected_nodes'])} 个:")
        for node in impact["affected_nodes"]:
            print("   ", node)

        # 混拼必须被拒绝
        try:
            wh.claim("2026-10-01", "2026-10-03")
            raise SystemExit("不应允许混拼快照")
        except SnapshotError as exc:
            print("\n[guard] 补算前领取被拒绝：", str(exc).splitlines()[0])

        # 6) 补算并重新领取
        run2 = wh.plan("2026-10-01", "2026-10-03")
        executed = wh.work(run2["run_id"], "recompute")
        states = Counter(i["status"] for i in wh.run_items(run2["run_id"]))
        print(f"\n[recompute] 执行 {len(executed)} 节点，run 终态 {dict(states)}")

        snap2 = wh.claim("2026-10-01", "2026-10-03", note="补算后自洽快照")
        print(f"[claim] 新快照 {snap2['snapshot_id']} 条目 {snap2['entry_count']} "
              f"consistent={snap2['consistent']}")

        con = duckdb.connect(snap2["duckdb_path"])
        print("\n[duckdb] 第二套快照数值（基于 95/30 与 10-02 清空）：")
        for r in con.execute("SELECT * FROM daily ORDER BY day").fetchall():
            print("  daily   ", r)
        for r in con.execute("SELECT * FROM balance ORDER BY day").fetchall():
            print("  balance ", r)
        for r in con.execute("SELECT * FROM turnover ORDER BY day").fetchall():
            print("  turnover", r)
        con.close()

        # 7) 证据目录
        cat = sqlite3.connect(wh_dir / "meta/catalog.sqlite")
        cat.row_factory = sqlite3.Row
        show(cat, "SELECT table_name,day,revision_no,state,row_count,origin "
                  "FROM source_revisions WHERE table_name='raw_day' ORDER BY day,revision_no",
             "证据：raw_day 修订历史（零行 cleared 与缺分区不同）")
        show(cat, "SELECT job_name,day,status,row_count FROM attempts "
                  "ORDER BY job_name,day,started_at", "证据：尝试（success/stale/failed）")
        show(cat, "SELECT job_name,day,row_count,is_current FROM products "
                  "ORDER BY job_name,day,product_id", "证据：产物 current 指针")
        show(cat, "SELECT snapshot_id,entry_count,note FROM snapshots ORDER BY created_at",
             "证据：自洽快照清单")
        cat.close()
        print(f"\n仓库目录: {wh_dir}")
    finally:
        wh.close()


if __name__ == "__main__":
    main()
