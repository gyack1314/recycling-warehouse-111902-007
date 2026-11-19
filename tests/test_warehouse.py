"""依赖感知补算编排的验收测试。

每个用例都在独立临时仓库中真实执行 SQL，证据（分区、尝试、产物、快照）落盘可查；
不使用预填的状态表模拟结果。
"""
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb

from warehouse.api import open_warehouse
from warehouse.graph import add_days

ROOT = Path(__file__).resolve().parents[1]
JOBS = ROOT / "inputs/jobs.json"
RAW = ROOT / "inputs/raw_day.parquet"
OPENING = ROOT / "inputs/opening.parquet"
CALENDAR = ROOT / "inputs/calendar.parquet"


class WarehouseCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="wh-test-"))
        self.wh_path = self.tmp / "warehouse"
        self.wh = open_warehouse(self.wh_path, JOBS)
        for table, path in (("raw_day", RAW), ("opening", OPENING),
                            ("calendar", CALENDAR)):
            self.wh.register_parquet(table, str(path))

    def tearDown(self):
        self.wh.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def db(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.wh_path / "meta/catalog.sqlite")
        con.row_factory = sqlite3.Row
        return con

    def baseline(self):
        self.wh.plan("2026-09-25", "2026-10-03")
        run = self.wh.plan("2026-09-25", "2026-10-03")
        self.wh.work(run["run_id"], "test-worker")
        return run

    # ---------- 1. 依赖按日期偏移展开，而非表名排序 ----------
    def test_dependency_expansion_uses_offsets(self):
        refs = dict(((t, d), off) for t, d, off in
                    self.wh.graph.refs("turnover", "2026-10-03"))
        window = [add_days("2026-10-03", k) for k in range(-6, 1)]
        for day in window:
            self.assertEqual(refs[("daily", day)],
                             (day and window.index(day) - 6))
        self.assertEqual(refs[("balance", "2026-10-03")], 0)
        bal_refs = self.wh.graph.refs("balance", "2026-10-01")
        self.assertIn(("balance", "2026-09-30", -1), bal_refs)

    def test_topo_order_not_alphabetical(self):
        # 字母序 balance 在 daily 前，拓扑序必须 daily 先
        self.assertEqual(self.wh.graph.topo_order(),
                         ["daily", "balance", "turnover"])

    # ---------- 2. 真实执行 SQL，数值正确 ----------
    def test_baseline_executes_real_sql_with_expected_values(self):
        self.baseline()
        claim = self.wh.claim("2026-10-01", "2026-10-03")
        self.assertTrue(claim["consistent"])
        con = duckdb.connect(claim["duckdb_path"])
        try:
            balance = dict(con.execute(
                "SELECT day, closing FROM balance ORDER BY day").fetchall())
            self.assertEqual(balance["2026-10-01"], Decimal("80.000"))
            self.assertEqual(balance["2026-10-02"], Decimal("90.000"))
            self.assertEqual(balance["2026-10-03"], Decimal("75.000"))
            turnover = dict(con.execute(
                "SELECT day, shipped_7d FROM turnover ORDER BY day").fetchall())
            self.assertEqual(turnover["2026-10-03"], Decimal("55.000"))
        finally:
            con.close()

    # ---------- 3. 锚点之外缺前置：指出原因，不补零 ----------
    def test_missing_prerequisite_is_blocked_not_zero(self):
        run = self.wh.plan("2026-09-25", "2026-09-26")
        self.wh.work(run["run_id"], "w")
        with self.db() as con:
            rows = con.execute(
                "SELECT status, blocker_json FROM run_items "
                "WHERE job_name='balance'").fetchall()
        blocked = [r for r in rows if r["status"] == "blocked"]
        self.assertTrue(blocked)
        reason = json.loads(blocked[0]["blocker_json"])[0]["reason"]
        self.assertIn("不默认补零", reason)

    def test_opening_anchor_stops_recursion(self):
        run = self.wh.plan("2026-10-01", "2026-10-01")
        self.wh.work(run["run_id"], "w")
        items = {i["job_name"]: i["status"] for i in self.wh.run_items(run["run_id"])}
        self.assertEqual(items["balance"], "committed")

    # ---------- 4. 影响范围沿日期边精确传播 ----------
    def test_impact_propagates_through_real_date_edges(self):
        self.baseline()
        impact = self.wh.impact("2026-10-03", [("raw_day", "2026-10-01")])
        affected = set(map(tuple, impact["affected_nodes"]))
        # daily@10-01 -> 余额链 3 天 + 周转 3 天 + daily 自身 = 7
        self.assertEqual(len(affected), 7)
        self.assertNotIn(("daily", "2026-09-30"), affected)
        self.assertIn(("balance", "2026-10-03"), affected)
        self.assertIn(("turnover", "2026-10-02"), affected)

    # ---------- 5. 修订后只重算受影响节点，未受影响跳过 ----------
    def test_recompute_touches_only_impacted_nodes(self):
        self.baseline()
        self.wh.register_rows(
            "raw_day", "2026-10-01",
            [{"day": "2026-10-01", "site": "N", "received": "90.000",
              "shipped": "30.000", "revision": 2}], str(RAW))
        run = self.wh.plan("2026-10-01", "2026-10-03")
        executed = self.wh.work(run["run_id"], "w")
        nodes = {tuple(e["node"]) for e in executed}
        self.assertEqual(len(nodes), 7)
        self.assertNotIn(("daily", "2026-10-02"), nodes)
        items = self.wh.run_items(run["run_id"])
        self.assertTrue(any(i["status"] == "skipped" for i in items))

    # ---------- 6. 执行期间输入变动：stale 不发布，重试读新版本 ----------
    def test_stale_attempt_kept_but_not_published(self):
        self.baseline()
        self.wh.register_rows(
            "raw_day", "2026-10-01",
            [{"day": "2026-10-01", "site": "N", "received": "90.000",
              "shipped": "30.000", "revision": 2}], str(RAW))
        hook = ROOT / "scripts/precommit_hook.py"
        os.environ["WH_PRECOMMIT_HOOK"] = f"{hook}:fire"
        os.environ["WH_TEST_WAREHOUSE"] = str(self.wh_path)
        os.environ["WH_TEST_REVISION_ROWS"] = json.dumps(
            [{"day": "2026-10-01", "site": "N", "received": "95.000",
              "shipped": "30.000", "revision": 3}])
        try:
            run = self.wh.plan("2026-10-01", "2026-10-01")
            results = self.wh.work(run["run_id"], "w")
        finally:
            for key in ("WH_PRECOMMIT_HOOK", "WH_TEST_WAREHOUSE",
                        "WH_TEST_REVISION_ROWS"):
                os.environ.pop(key, None)
        self.assertEqual(results[0]["status"], "stale")
        with self.db() as con:
            stale = con.execute(
                "SELECT * FROM attempts WHERE status='stale'").fetchall()
            self.assertEqual(len(stale), 1)
            currents = con.execute(
                "SELECT count(*) c FROM products WHERE job_name='daily' "
                "AND day='2026-10-01' AND is_current=1").fetchone()["c"]
            self.assertEqual(currents, 1)  # 旧 current 未被覆盖
        # 重试：新指纹、新尝试、发布 rev3 结果，并把受影响下游一并补算
        self.wh.retry(run["run_id"], "daily", "2026-10-01")
        self.wh.work(run["run_id"], "w-retry")
        follow = self.wh.plan("2026-10-01", "2026-10-03")
        self.wh.work(follow["run_id"], "w-follow")
        claim = self.wh.claim("2026-10-01", "2026-10-03")
        con = duckdb.connect(claim["duckdb_path"])
        try:
            received = con.execute(
                "SELECT received FROM daily WHERE day='2026-10-01'").fetchone()[0]
            self.assertEqual(received, Decimal("95.000"))
        finally:
            con.close()

    # ---------- 7. 零行产物与缺分区分别对待 ----------
    def test_cleared_partition_zero_row_vs_missing(self):
        self.baseline()
        self.wh.clear("raw_day", "2026-10-02", str(RAW))
        state = self.wh.catalog.source_state("raw_day", "2026-10-02")
        self.assertEqual(state["state"], "cleared")
        self.assertEqual(state["row_count"], 0)
        self.assertTrue(Path(state["path"]).exists())  # 零行 parquet 证据
        # 从未登记的日期是 missing
        self.assertIsNone(self.wh.catalog.source_state("raw_day", "2026-11-01"))
        run = self.wh.plan("2026-10-02", "2026-10-02")
        self.wh.work(run["run_id"], "w")
        with self.db() as con:
            row = con.execute(
                "SELECT row_count FROM products WHERE job_name='daily' "
                "AND day='2026-10-02' AND is_current=1").fetchone()
            self.assertEqual(row["row_count"], 0)

    # ---------- 8. 快照拒绝混拼，失败时可回退上一套 ----------
    def test_snapshot_rejects_mixed_versions_and_fallback_works(self):
        self.baseline()
        first = self.wh.claim("2026-10-01", "2026-10-03")
        self.wh.register_rows(
            "raw_day", "2026-10-01",
            [{"day": "2026-10-01", "site": "N", "received": "90.000",
              "shipped": "30.000", "revision": 2}], str(RAW))
        partial = self.wh.plan("2026-10-01", "2026-10-01")
        self.wh.work(partial["run_id"], "w")  # 只重算 daily
        from warehouse.snapshot import SnapshotError
        with self.assertRaises(SnapshotError):
            self.wh.claim("2026-10-01", "2026-10-03")
        fallback = self.wh.fallback_snapshot()
        self.assertEqual(fallback["snapshot_id"], first["snapshot_id"])
        verified = self.wh.verify_snapshot(first["snapshot_id"])
        self.assertTrue(verified["all_verified"])

    # ---------- 9. 尝试记录实际读取的输入版本 ----------
    def test_attempt_manifest_pins_exact_input_versions(self):
        run = self.wh.plan("2026-10-01", "2026-10-01")
        self.wh.work(run["run_id"], "w")
        with self.db() as con:
            row = con.execute(
                "SELECT input_manifest_json FROM attempts "
                "WHERE job_name='daily' AND day='2026-10-01'").fetchone()
        manifest = json.loads(row["input_manifest_json"])
        self.assertEqual([(m["table"], m["day"]) for m in manifest],
                         [("raw_day", "2026-10-01")])
        self.assertTrue(all(m["sha256"] for m in manifest))
        # turnover 的七日窗口需要完整窗口区，单独验证
        run2 = self.wh.plan("2026-09-25", "2026-10-01")
        self.wh.work(run2["run_id"], "w2")
        with self.db() as con:
            row = con.execute(
                "SELECT input_manifest_json FROM attempts "
                "WHERE job_name='turnover' AND day='2026-10-01' "
                "AND status='success'").fetchone()
        manifest = json.loads(row["input_manifest_json"])
        self.assertEqual(len([m for m in manifest if m["table"] == "daily"]), 7)
        self.assertIn(("balance", "2026-10-01"),
                      {(m["table"], m["day"]) for m in manifest})

    # ---------- 10. 独立重试不借用旧成功标志 ----------
    def test_retry_creates_new_attempt_with_new_fingerprint(self):
        run = self.wh.plan("2026-10-01", "2026-10-01")
        self.wh.work(run["run_id"], "w")
        self.wh.register_rows(
            "raw_day", "2026-10-01",
            [{"day": "2026-10-01", "site": "N", "received": "77.000",
              "shipped": "30.000", "revision": 9}], str(RAW))
        run2 = self.wh.plan("2026-10-01", "2026-10-01")
        self.wh.work(run2["run_id"], "w")
        with self.db() as con:
            fps = [r["input_fingerprint"] for r in con.execute(
                "SELECT DISTINCT input_fingerprint FROM attempts "
                "WHERE job_name='daily' AND day='2026-10-01'")]
            self.assertEqual(len(fps), 2)  # 修订前后两个不同指纹


if __name__ == "__main__":
    unittest.main(verbosity=2)
