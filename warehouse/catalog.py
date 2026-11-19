"""证据目录：修订、尝试、产物、运行项与快照的事务化访问。

并发约定：
- 两个调度进程通过 SQLite WAL + 原子 UPDATE 领取 run_items，同一作业/日不会被两个进程同时执行；
- 规划（register/run 创建）用文件锁 fcntl 串行化，执行期只占用短事务；
- 提交前重新核对输入指纹，输入已变动则记 stale，绝不发布。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from .graph import ANCHOR_DAY, ANCHOR_SOURCE_TABLE, DependencyGraph, JobSpec
from .models import ITEM_PENDING, initialize

LEASE_SECONDS = 300


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%fZ")


def new_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}_{uuid.uuid4().hex[:8]}"


@contextmanager
def transaction(con: sqlite3.Connection):
    con.execute("BEGIN IMMEDIATE")
    try:
        yield
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


class Catalog:
    def __init__(self, con: sqlite3.Connection):
        self.con = con
        initialize(con)

    # ---------- 作业定义 ----------
    def upsert_job_specs(self, specs: dict[str, JobSpec]) -> dict[str, bool]:
        changed: dict[str, bool] = {}
        with transaction(self.con):
            for name, spec in specs.items():
                row = self.con.execute(
                    "SELECT spec_sha256 FROM job_specs WHERE job_name=?", (name,)
                ).fetchone()
                if row is None:
                    self.con.execute(
                        "INSERT INTO job_specs(job_name,spec_json,spec_sha256) VALUES(?,?,?)",
                        (name, json.dumps(asdict(spec), ensure_ascii=False), spec.spec_sha256),
                    )
                    changed[name] = True
                else:
                    changed[name] = row["spec_sha256"] != spec.spec_sha256
                    if changed[name]:
                        self.con.execute(
                            "UPDATE job_specs SET spec_json=?, spec_sha256=? WHERE job_name=?",
                            (json.dumps(asdict(spec), ensure_ascii=False), spec.spec_sha256, name),
                        )
        return changed

    def current_spec_sha(self, job_name: str) -> str | None:
        row = self.con.execute(
            "SELECT spec_sha256 FROM job_specs WHERE job_name=?", (job_name,)
        ).fetchone()
        return row["spec_sha256"] if row else None

    # ---------- 原始分区修订 ----------
    def register_revision(
        self,
        table: str,
        day: str,
        state: str,
        path: str,
        file_sha256: str | None,
        row_count: int | None,
        origin: str,
        note: str | None = None,
    ) -> dict:
        """登记一次分区修订，返回修订记录与是否实际变化。

        state=cleared 表示“明确声明完整为空”（保留零行证据），
        与从未登记的 missing 严格区分。内容未变的重复登记是 no-op。
        """
        with transaction(self.con):
            current = self.con.execute(
                "SELECT c.state, r.file_sha256 FROM source_current c "
                "JOIN source_revisions r ON r.revision_id=c.revision_id "
                "WHERE c.table_name=? AND c.day=?",
                (table, day),
            ).fetchone()
            if (
                current is not None
                and current["state"] == state
                and current["file_sha256"] == file_sha256
            ):
                rev = self.con.execute(
                    "SELECT * FROM source_revisions WHERE revision_id=?",
                    (current["revision_id"],),
                ).fetchone()
                return {"changed": False, "revision": dict(rev)}

            next_no = self.con.execute(
                "SELECT COALESCE(MAX(revision_no),0)+1 AS n FROM source_revisions "
                "WHERE table_name=? AND day=?",
                (table, day),
            ).fetchone()["n"]
            revision_id = self.con.execute(
                "INSERT INTO source_revisions"
                "(table_name,day,state,revision_no,path,file_sha256,row_count,origin,note) "
                "VALUES(?,?,?,?,?,?,?,?,?) RETURNING revision_id",
                (table, day, state, next_no, path, file_sha256, row_count, origin, note),
            ).fetchone()["revision_id"]
            self.con.execute(
                "INSERT INTO source_current(table_name,day,revision_id,state) "
                "VALUES(?,?,?,?) ON CONFLICT(table_name,day) DO UPDATE SET "
                "revision_id=excluded.revision_id, state=excluded.state",
                (table, day, revision_id, state),
            )
            rev = self.con.execute(
                "SELECT * FROM source_revisions WHERE revision_id=?", (revision_id,)
            ).fetchone()
            return {"changed": True, "revision": dict(rev)}

    def source_state(self, table: str, day: str) -> dict | None:
        row = self.con.execute(
            "SELECT c.state, c.revision_id, r.revision_no, r.path, r.file_sha256, "
            "r.row_count, r.table_name, r.day "
            "FROM source_current c JOIN source_revisions r ON r.revision_id=c.revision_id "
            "WHERE c.table_name=? AND c.day=?",
            (table, day),
        ).fetchone()
        return dict(row) if row else None

    def list_source_partitions(self, table: str | None = None) -> list[dict]:
        sql = (
            "SELECT r.table_name, r.day, c.state, r.revision_no, r.revision_id, "
            "r.path, r.file_sha256, r.row_count, r.registered_at, r.origin "
            "FROM source_current c JOIN source_revisions r ON r.revision_id=c.revision_id"
        )
        args: tuple = ()
        if table:
            sql += " WHERE r.table_name=?"
            args = (table,)
        sql += " ORDER BY r.table_name, r.day"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    def list_revisions(self, table: str | None = None, day: str | None = None) -> list[dict]:
        sql = "SELECT * FROM source_revisions WHERE 1=1"
        args: list = []
        if table:
            sql += " AND table_name=?"
            args.append(table)
        if day:
            sql += " AND day=?"
            args.append(day)
        sql += " ORDER BY table_name, day, revision_no"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    # ---------- 产物 ----------
    def current_product(self, job_name: str, day: str) -> dict | None:
        row = self.con.execute(
            "SELECT * FROM products WHERE job_name=? AND day=? AND is_current=1",
            (job_name, day),
        ).fetchone()
        return dict(row) if row else None

    def list_products(self, job_name: str | None = None) -> list[dict]:
        sql = (
            "SELECT p.*, a.status AS attempt_status FROM products p "
            "JOIN attempts a ON a.attempt_id=p.attempt_id WHERE 1=1"
        )
        args: tuple = ()
        if job_name:
            sql += " AND p.job_name=?"
            args = (job_name,)
        sql += " ORDER BY p.job_name, p.day, p.product_id"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    # ---------- 输入解析与指纹 ----------
    def resolve_input(self, graph: DependencyGraph, table: str, day: str) -> dict:
        """把一个依赖引用解析成精确的可读版本。

        返回 {status, ...}：
        - ok:      指向当前产物或原件分区（含零行）
        - anchor:  由 opening 锚点满足
        - missing: 缺必要前置（调用方负责阻断，不默认补零）
        """
        # opening 锚点优先：balance 在锚点日的 -1 依赖由明确给定的 opening 满足，
        # 不应再去找不存在的 balance@2026-09-30 产物
        anchor = graph.anchor_ref(table, day)
        if anchor is not None:
            src = self.source_state(ANCHOR_SOURCE_TABLE, ANCHOR_DAY)
            if src is None:
                return {"status": "missing", "table": ANCHOR_SOURCE_TABLE, "day": ANCHOR_DAY}
            return {
                "status": "anchor",
                "kind": "source",
                "table": ANCHOR_SOURCE_TABLE,
                "day": ANCHOR_DAY,
                "path": src["path"],
                "sha256": src["file_sha256"],
                "row_count": src["row_count"],
                "revision_id": src["revision_id"],
                "fingerprint": f"source:{ANCHOR_SOURCE_TABLE}:{ANCHOR_DAY}:rev{src['revision_id']}:{src['file_sha256']}",
            }
        if graph.is_job(table):
            product = self.current_product(table, day)
            if product is None:
                return {"status": "missing", "table": table, "day": day}
            return {
                "status": "ok",
                "kind": "product",
                "table": table,
                "day": day,
                "path": product["output_path"],
                "sha256": product["output_sha256"],
                "row_count": product["row_count"],
                "revision_id": None,
                "fingerprint": f"product:{table}:{day}:{product['output_sha256']}",
            }
        src = self.source_state(table, day)
        if src is None:
            return {"status": "missing", "table": table, "day": day}
        return {
            "status": "ok",
            "kind": "source",
            "table": table,
            "day": day,
            "state": src["state"],
            "path": src["path"],
            "sha256": src["file_sha256"],
            "row_count": src["row_count"],
            "revision_id": src["revision_id"],
            "fingerprint": (
                f"source:{table}:{day}:rev{src['revision_id']}:{src['file_sha256']}:{src['state']}"
            ),
        }

    def resolve_attempt_inputs(
        self, graph: DependencyGraph, job_name: str, day: str
    ) -> tuple[list[dict], list[dict]]:
        """展开作业在某日的全部输入。返回 (resolved, blockers)。"""
        resolved: list[dict] = []
        blockers: list[dict] = []
        for table, dep_day, offset in graph.refs(job_name, day):
            ref = self.resolve_input(graph, table, dep_day)
            ref["offset"] = offset
            if ref["status"] == "missing":
                blockers.append(
                    {
                        "job": job_name,
                        "day": day,
                        "missing_table": table,
                        "missing_day": dep_day,
                        "offset": offset,
                        "reason": (
                            f"作业 {job_name}@{day} 需要 {table}@{dep_day}（偏移 {offset}），"
                            "该前置分区不存在；按契约不默认补零"
                        ),
                    }
                )
            else:
                resolved.append(ref)
        return resolved, blockers

    @staticmethod
    def fingerprint_of(resolved: Iterable[dict], spec_sha256: str) -> str:
        import hashlib

        parts = [f"spec:{spec_sha256}"]
        for ref in sorted(resolved, key=lambda r: (r["table"], r["day"])):
            parts.append(ref["fingerprint"])
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    # ---------- 运行项 ----------
    def create_run(self, run_id: str, nodes: list[tuple[str, str]]) -> None:
        with transaction(self.con):
            for job_name, day in nodes:
                self.con.execute(
                    "INSERT INTO run_items(run_id,job_name,day,status) VALUES(?,?,?,?) "
                    "ON CONFLICT(run_id,job_name,day) DO UPDATE SET "
                    "status=CASE WHEN run_items.status IN ('committed','skipped') "
                    "THEN run_items.status ELSE excluded.status END, "
                    "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
                    (run_id, job_name, day, ITEM_PENDING),
                )

    def set_item_status(
        self,
        run_id: str,
        job_name: str,
        day: str,
        status: str,
        **fields,
    ) -> None:
        sets = ["status=?", "updated_at=?"]
        args: list = [status, utc_now()]
        for key, value in fields.items():
            sets.append(f"{key}=?")
            args.append(
                json.dumps(value, ensure_ascii=False) if key == "blocker_json" and value is not None else value
            )
        args += [run_id, job_name, day]
        self.con.execute(
            f"UPDATE run_items SET {', '.join(sets)} "
            "WHERE run_id=? AND job_name=? AND day=?",
            args,
        )

    def get_item(self, run_id: str, job_name: str, day: str) -> dict | None:
        row = self.con.execute(
            "SELECT * FROM run_items WHERE run_id=? AND job_name=? AND day=?",
            (run_id, job_name, day),
        ).fetchone()
        return dict(row) if row else None

    def list_items(self, run_id: str) -> list[dict]:
        return [
            dict(r)
            for r in self.con.execute(
                "SELECT * FROM run_items WHERE run_id=? ORDER BY day, job_name", (run_id,)
            ).fetchall()
        ]

    def requeue_dead_leases(self, worker_id: str) -> int:
        """崩溃/超时的 running 项重新可领取（已落库的尝试证据保留）。"""
        with transaction(self.con):
            return self._requeue_dead_leases_locked()

    def _requeue_dead_leases_locked(self) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=LEASE_SECONDS)).strftime(
            "%Y-%m-%dT%H:%M:%fZ"
        )
        cur = self.con.execute(
            "UPDATE run_items SET status='pending', claimed_by=NULL, lease_until=NULL, "
            "updated_at=? WHERE status='running' AND lease_until IS NOT NULL "
            "AND lease_until < ?",
            (utc_now(), cutoff),
        )
        return cur.rowcount

    def _node_exists(self, graph: DependencyGraph, table: str, day: str) -> bool:
        """前置节点是否在仓库中存在任何形态（原件分区/作业产物/锚点）。

        不存在 = 硬缺失（blocked，要求运营补前置）；
        存在但当前 run 尚未提交 = 软等待（保持 pending）。
        """
        anchor = graph.anchor_ref(table, day)
        if anchor is not None:
            return self.source_state(ANCHOR_SOURCE_TABLE, ANCHOR_DAY) is not None
        if graph.is_job(table):
            row = self.con.execute(
                "SELECT 1 FROM products WHERE job_name=? AND day=? LIMIT 1",
                (table, day),
            ).fetchone()
            return row is not None
        return self.source_state(table, day) is not None

    def claim_next(
        self, run_id: str, graph: DependencyGraph, worker_id: str
    ) -> dict | None:
        """原子领取一个前置已就绪的运行项。

        就绪判定：所有输入引用当前都可解析（产物 current 或原件分区存在）。
        领取是条件 UPDATE，两个调度进程只有一个能成功。
        """
        lease = (datetime.now(timezone.utc) + timedelta(seconds=LEASE_SECONDS)).strftime(
            "%Y-%m-%dT%H:%M:%fZ"
        )
        # 同日按作业拓扑层级排序（daily -> balance -> turnover），
        # 不能按表名字母序，否则下游会在上游重算前被错误跳过
        order = {name: i for i, name in enumerate(graph.topo_order())}
        with transaction(self.con):
            self._requeue_dead_leases_locked()
            # blocked 不是终态：前置可能在本轮其他节点提交后变为可满足，每轮重新评估
            rows = self.con.execute(
                "SELECT * FROM run_items WHERE run_id=? AND status IN "
                "('pending','ready','blocked') ORDER BY day, job_name",
                (run_id,),
            ).fetchall()
            rows = sorted(rows, key=lambda r: (r["day"], order.get(r["job_name"], 99)))
            for row in rows:
                job_name, day = row["job_name"], row["day"]
                resolved, blockers = self.resolve_attempt_inputs(graph, job_name, day)
                if blockers:
                    # 区分“前置尚未产出”与“前置确实缺失”：
                    # 前者保持 pending，等同 run 的其他 worker 提交后下一轮可领取；
                    # 后者才落 blocked，避免并发 worker 互相饿死
                    hard_missing = any(
                        not self._node_exists(graph, b["missing_table"], b["missing_day"])
                        for b in blockers
                    )
                    if hard_missing:
                        self.con.execute(
                            "UPDATE run_items SET status='blocked', blocker_json=?, "
                            "updated_at=? WHERE run_id=? AND job_name=? AND day=? "
                            "AND status IN ('pending','ready','blocked')",
                            (json.dumps(blockers, ensure_ascii=False), utc_now(),
                             run_id, job_name, day),
                        )
                    else:
                        self.con.execute(
                            "UPDATE run_items SET blocker_json=?, updated_at=? "
                            "WHERE run_id=? AND job_name=? AND day=?",
                            (json.dumps(blockers, ensure_ascii=False), utc_now(),
                             run_id, job_name, day),
                        )
                    continue
                # 执行期裁决：前置已在本 run 中重算后，指纹通常不再匹配，
                # 只有当前目录下产物仍精确匹配才跳过，避免无谓重算
                fp = self.fingerprint_of(resolved, self.current_spec_sha(job_name))
                product = self.current_product(job_name, day)
                if product is not None and product["input_fingerprint"] == fp:
                    self.con.execute(
                        "UPDATE run_items SET status='skipped', blocker_json=NULL, "
                        "updated_at=? WHERE run_id=? AND job_name=? AND day=? "
                        "AND status IN ('pending','ready','blocked')",
                        (utc_now(), run_id, job_name, day),
                    )
                    continue
                cur = self.con.execute(
                    "UPDATE run_items SET status='running', claimed_by=?, lease_until=?, "
                    "attempts_count=attempts_count+1, blocker_json=NULL, updated_at=? "
                    "WHERE run_id=? AND job_name=? AND day=? AND status IN "
                    "('pending','ready','blocked')",
                    (worker_id, lease, utc_now(), run_id, job_name, day),
                )
                if cur.rowcount == 1:
                    return {"job_name": job_name, "day": day}
        return None

    # ---------- 尝试与发布 ----------
    def record_attempt(self, attempt: dict) -> None:
        with transaction(self.con):
            self.con.execute(
                "INSERT INTO attempts(attempt_id,run_id,job_name,day,status,"
                "input_fingerprint,input_manifest_json,output_sha256,output_path,"
                "row_count,error,spec_sha256,finished_at,worker_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    attempt["attempt_id"],
                    attempt.get("run_id"),
                    attempt["job_name"],
                    attempt["day"],
                    attempt["status"],
                    attempt["input_fingerprint"],
                    json.dumps(attempt["input_manifest"], ensure_ascii=False, indent=2),
                    attempt.get("output_sha256"),
                    attempt.get("output_path"),
                    attempt.get("row_count"),
                    attempt.get("error"),
                    attempt["spec_sha256"],
                    utc_now(),
                    attempt.get("worker_id"),
                ),
            )

    def check_freshness(
        self, graph: DependencyGraph, job_name: str, day: str, fingerprint: str
    ) -> tuple[bool, list[dict], list[dict]]:
        """提交前复核：重新解析输入并比对指纹。

        旧尝试的成功标志不能授权新输入——指纹不一致即 stale。
        """
        resolved, blockers = self.resolve_attempt_inputs(graph, job_name, day)
        current_fp = self.fingerprint_of(resolved, self.current_spec_sha(job_name))
        return current_fp == fingerprint, resolved, blockers

    def publish_product(self, attempt: dict) -> dict:
        """原子发布：旧 current 指针下线，新产物成为唯一 current。"""
        with transaction(self.con):
            existing = self.con.execute(
                "SELECT product_id FROM products WHERE job_name=? AND day=? AND output_sha256=?",
                (attempt["job_name"], attempt["day"], attempt["output_sha256"]),
            ).fetchone()
            if existing is None:
                product_id = self.con.execute(
                    "INSERT INTO products(job_name,day,attempt_id,output_sha256,"
                    "output_path,row_count,input_fingerprint) VALUES(?,?,?,?,?,?,?) "
                    "RETURNING product_id",
                    (
                        attempt["job_name"],
                        attempt["day"],
                        attempt["attempt_id"],
                        attempt["output_sha256"],
                        attempt["output_path"],
                        attempt["row_count"],
                        attempt["input_fingerprint"],
                    ),
                ).fetchone()["product_id"]
            else:
                product_id = existing["product_id"]
            self.con.execute(
                "UPDATE products SET is_current=0 WHERE job_name=? AND day=? AND product_id<>?",
                (attempt["job_name"], attempt["day"], product_id),
            )
            self.con.execute(
                "UPDATE products SET is_current=1, attempt_id=?, input_fingerprint=? "
                "WHERE product_id=?",
                (attempt["attempt_id"], attempt["input_fingerprint"], product_id),
            )
            return dict(self.con.execute(
                "SELECT * FROM products WHERE product_id=?", (product_id,)
            ).fetchone())

    def list_attempts(self, run_id: str | None = None) -> list[dict]:
        sql = "SELECT * FROM attempts WHERE 1=1"
        args: tuple = ()
        if run_id:
            sql += " AND run_id=?"
            args = (run_id,)
        sql += " ORDER BY started_at, attempt_id"
        return [dict(r) for r in self.con.execute(sql, args).fetchall()]

    # ---------- 快照 ----------
    def record_snapshot(
        self,
        snapshot_id: str,
        run_id: str | None,
        day_from: str | None,
        day_to: str | None,
        manifest_path: str,
        duckdb_path: str | None,
        entries: list[dict],
        note: str | None,
    ) -> None:
        with transaction(self.con):
            self.con.execute(
                "INSERT INTO snapshots(snapshot_id,run_id,day_from,day_to,manifest_path,"
                "duckdb_path,entry_count,note) VALUES(?,?,?,?,?,?,?,?)",
                (
                    snapshot_id,
                    run_id,
                    day_from,
                    day_to,
                    manifest_path,
                    duckdb_path,
                    len(entries),
                    note,
                ),
            )
            self.con.executemany(
                "INSERT INTO snapshot_entries(snapshot_id,job_name,day,product_id,"
                "output_sha256,input_fingerprint) VALUES(?,?,?,?,?,?)",
                [
                    (
                        snapshot_id,
                        e["job_name"],
                        e["day"],
                        e["product_id"],
                        e["output_sha256"],
                        e["input_fingerprint"],
                    )
                    for e in entries
                ],
            )

    def latest_snapshot(self) -> dict | None:
        row = self.con.execute(
            "SELECT * FROM snapshots ORDER BY created_at DESC, snapshot_id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def list_snapshots(self) -> list[dict]:
        return [dict(r) for r in self.con.execute(
            "SELECT * FROM snapshots ORDER BY created_at"
        ).fetchall()]

    def snapshot_entries(self, snapshot_id: str) -> list[dict]:
        return [dict(r) for r in self.con.execute(
            "SELECT * FROM snapshot_entries WHERE snapshot_id=? ORDER BY job_name,day",
            (snapshot_id,),
        ).fetchall()]

    # ---------- 影响范围 ----------
    def changed_source_nodes(self, since_revision_id: int | None) -> set[tuple[str, str]]:
        sql = (
            "SELECT DISTINCT table_name, day FROM source_revisions "
            "WHERE table_name NOT IN (SELECT job_name FROM job_specs)"
        )
        args: tuple = ()
        if since_revision_id is not None:
            sql += " AND revision_id > ?"
            args = (since_revision_id,)
        return {(r["table_name"], r["day"]) for r in self.con.execute(sql, args).fetchall()}

    def product_days(self, job_name: str) -> list[str]:
        return [
            r["day"]
            for r in self.con.execute(
                "SELECT DISTINCT day FROM products WHERE job_name=? AND is_current=1 ORDER BY day",
                (job_name,),
            ).fetchall()
        ]
