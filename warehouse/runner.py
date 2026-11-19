"""执行引擎：在 DuckDB 中实际运行项目 SQL，固定输入版本并留存证据。

关键纪律：
1. 每次尝试先解析全部输入的精确版本（产物 sha256 或原件 revision+sha256），
   形成 input_manifest 与指纹；DuckDB 视图直接绑定不可变 parquet 路径；
2. SQL 无外部副作用，输出写入内容寻址 artifacts（零行也产生 parquet）；
3. 发布前重新解析输入并比对指纹，期间输入变动 -> stale，产物保留但不发布；
   重试是新的尝试、新的指纹，旧成功标志不会授权新输入；
4. 失败记录错误与证据，run_item 回到 failed，可独立重试。
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import duckdb

from .catalog import Catalog, new_id
from .config import Config
from .graph import DependencyGraph
from .models import (
    ATTEMPT_FAILED,
    ATTEMPT_STALE,
    ATTEMPT_SUCCESS,
    ITEM_COMMITTED,
    ITEM_FAILED,
)

# 每个作业输出的业务列类型（day/site 为字符串，金额统一 decimal(18,3)）
OUTPUT_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "daily": [("day", "VARCHAR"), ("site", "VARCHAR"),
              ("received", "DECIMAL(18,3)"), ("shipped", "DECIMAL(18,3)")],
    "balance": [("day", "VARCHAR"), ("site", "VARCHAR"),
                ("closing", "DECIMAL(18,3)")],
    "turnover": [("day", "VARCHAR"), ("site", "VARCHAR"),
                 ("shipped_7d", "DECIMAL(18,3)"), ("closing", "DECIMAL(18,3)")],
}

# 原件 parquet 的金额列是字符串，SQL 直接 coalesce 会与整数 0 类型冲突；
# 在源视图层显式规范化为与产物一致的类型（项目测试即通过 cast 建表解决）。
SOURCE_CAST_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "raw_day": [("day", "VARCHAR"), ("site", "VARCHAR"),
                ("received", "DECIMAL(18,3)"), ("shipped", "DECIMAL(18,3)"),
                ("revision", "BIGINT")],
    "opening": [("day", "VARCHAR"), ("site", "VARCHAR"),
                ("closing", "DECIMAL(18,3)")],
    "calendar": [("day", "VARCHAR"), ("closed", "BOOLEAN")],
}


def _view_name(table: str, day: str) -> str:
    return f"in_{table}_{day.replace('-', '_')}"


def _sql_literal(text: str) -> str:
    """把路径内联为 SQL 字符串字面量。路径由仓库生成，仍做标准转义。"""
    return "'" + text.replace("'", "''") + "'"


def _precommit_hook():
    """测试钩子：提交前执行外部回调，用于制造执行期间的输入修订竞争。"""
    hook = os.environ.get("WH_PRECOMMIT_HOOK")
    if not hook:
        return
    module_path, func_name = hook.split(":")
    import importlib.util

    spec = importlib.util.spec_from_file_location("wh_hook", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    getattr(module, func_name)()


class Runner:
    def __init__(self, config: Config, catalog: Catalog, graph: DependencyGraph,
                 worker_id: str):
        self.config = config
        self.catalog = catalog
        self.graph = graph
        self.worker_id = worker_id

    def execute_node(self, run_id: str, job_name: str, day: str) -> dict:
        spec = self.graph.specs[job_name]
        attempt_id = new_id("att")
        attempt_dir = self.config.attempt_dir(attempt_id)
        attempt_dir.mkdir(parents=True, exist_ok=True)

        # 1) 解析并固定输入版本（缺前置在 claim 阶段已阻断，这里再防一次）
        resolved, blockers = self.catalog.resolve_attempt_inputs(self.graph, job_name, day)
        if blockers:
            raise RuntimeError("存在未满足的前置: " + json.dumps(blockers, ensure_ascii=False))

        fingerprint = self.catalog.fingerprint_of(resolved, spec.spec_sha256)
        manifest = {
            "attempt_id": attempt_id,
            "run_id": run_id,
            "job": job_name,
            "day": day,
            "spec_sha256": spec.spec_sha256,
            "input_fingerprint": fingerprint,
            "inputs": [
                {k: ref[k] for k in (
                    "table", "day", "offset", "kind", "path", "sha256",
                    "row_count", "revision_id", "state",
                ) if k in ref}
                for ref in sorted(resolved, key=lambda r: (r["table"], r["day"]))
            ],
            "worker_id": self.worker_id,
        }
        (attempt_dir / "input_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        try:
            result = self._run_sql(spec.sql, day, resolved, job_name)
            output_path, output_sha, row_count = self._persist_output(
                job_name, result, attempt_dir
            )
        except Exception as exc:  # 失败留证据，状态回到 failed
            attempt = {
                "attempt_id": attempt_id,
                "run_id": run_id,
                "job_name": job_name,
                "day": day,
                "status": ATTEMPT_FAILED,
                "input_fingerprint": fingerprint,
                "input_manifest": manifest["inputs"],
                "spec_sha256": spec.spec_sha256,
                "error": f"{type(exc).__name__}: {exc}",
                "worker_id": self.worker_id,
            }
            (attempt_dir / "error.txt").write_text(attempt["error"], encoding="utf-8")
            self.catalog.record_attempt(attempt)
            self.catalog.set_item_status(
                run_id, job_name, day, ITEM_FAILED, attempt_id=attempt_id
            )
            return {"node": (job_name, day), "status": ITEM_FAILED, "attempt_id": attempt_id,
                    "error": attempt["error"]}

        # 2) 测试/竞争窗口：真实系统里这里可能耗时，期间原件可被修订
        _precommit_hook()

        # 3) 提交前新鲜度复核：旧成功不能授权新输入
        fresh, current_resolved, current_blockers = self.catalog.check_freshness(
            self.graph, job_name, day, fingerprint
        )
        if not fresh:
            attempt = {
                "attempt_id": attempt_id,
                "run_id": run_id,
                "job_name": job_name,
                "day": day,
                "status": ATTEMPT_STALE,
                "input_fingerprint": fingerprint,
                "input_manifest": manifest["inputs"],
                "output_sha256": output_sha,
                "output_path": str(output_path),
                "row_count": row_count,
                "spec_sha256": spec.spec_sha256,
                "error": "执行期间输入发生变化，旧结果保留核查但不发布；需按新输入重试",
                "worker_id": self.worker_id,
            }
            (attempt_dir / "stale.txt").write_text(
                json.dumps(
                    {"old_fingerprint": fingerprint,
                     "current_inputs": [
                         {k: r.get(k) for k in ("table", "day", "sha256", "revision_id", "state")}
                         for r in current_resolved],
                     "current_blockers": current_blockers},
                    ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self.catalog.record_attempt(attempt)
            self.catalog.set_item_status(
                run_id, job_name, day, ITEM_FAILED, attempt_id=attempt_id
            )
            return {"node": (job_name, day), "status": ATTEMPT_STALE, "attempt_id": attempt_id}

        # 4) 发布（原子切换 current 指针），旧产物不删除
        attempt = {
            "attempt_id": attempt_id,
            "run_id": run_id,
            "job_name": job_name,
            "day": day,
            "status": ATTEMPT_SUCCESS,
            "input_fingerprint": fingerprint,
            "input_manifest": manifest["inputs"],
            "output_sha256": output_sha,
            "output_path": str(output_path),
            "row_count": row_count,
            "spec_sha256": spec.spec_sha256,
            "worker_id": self.worker_id,
        }
        self.catalog.record_attempt(attempt)
        product = self.catalog.publish_product(attempt)
        self.catalog.set_item_status(
            run_id, job_name, day, ITEM_COMMITTED, attempt_id=attempt_id
        )
        (attempt_dir / "result.json").write_text(
            json.dumps({"attempt": attempt, "product_id": product["product_id"]},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {"node": (job_name, day), "status": ITEM_COMMITTED,
                "attempt_id": attempt_id, "product_id": product["product_id"],
                "row_count": row_count}

    # ---------- DuckDB 执行 ----------
    def _run_sql(self, sql: str, day: str, resolved: list[dict], job_name: str):
        con = duckdb.connect()
        try:
            grouped: dict[str, list[dict]] = {}
            for ref in resolved:
                grouped.setdefault(ref["table"], []).append(ref)
            for table, refs in grouped.items():
                refs = sorted(refs, key=lambda r: r["day"])
                union_parts = []
                for ref in refs:
                    view = _view_name(table, ref["day"])
                    # CREATE VIEW 不能使用预编译参数，路径内联（仓库自控路径，已转义）
                    cast_cols = SOURCE_CAST_COLUMNS.get(table)
                    if cast_cols and ref["kind"] == "source":
                        # 显式投影业务列：排除分区目录带来的 dt/rev Hive 列，
                        # 同时把字符串金额规范化为 decimal，避免与 SQL 中的 0 类型冲突
                        select = ", ".join(
                            f"cast({c} as {t}) AS {c}" for c, t in cast_cols
                        )
                    else:
                        select = "*"
                    con.execute(
                        f"CREATE VIEW {view} AS "
                        f"SELECT {select} FROM read_parquet("
                        f"{_sql_literal(ref['path'])}, union_by_name=true, hive_partitioning=0)"
                    )
                    union_parts.append(f"SELECT * FROM {view}")
                con.execute(
                    f"CREATE VIEW {table} AS " + " UNION ALL ".join(union_parts)
                )
            # SQL 文本中的 previous_balance 别名：绑定到 balance 输入的不可变路径
            if any(r["table"] == "balance" for r in resolved):
                balance_refs = sorted(
                    (r for r in resolved if r["table"] == "balance"),
                    key=lambda r: r["day"],
                )
                con.execute(
                    "CREATE VIEW previous_balance AS "
                    + " UNION ALL ".join(
                        f"SELECT * FROM {_view_name('balance', r['day'])}"
                        for r in balance_refs
                    )
                )
            elif job_name == "balance":
                # 锚点日：previous_balance 绑定 opening 原件（金额列同样需要 cast）
                opening = next(r for r in resolved if r["table"] == "opening")
                con.execute(
                    "CREATE VIEW previous_balance AS "
                    "SELECT cast(day as VARCHAR) AS day, cast(site as VARCHAR) AS site, "
                    "cast(closing as DECIMAL(18,3)) AS closing FROM read_parquet("
                    f"{_sql_literal(opening['path'])}, union_by_name=true, "
                    "hive_partitioning=0)"
                )
            columns = OUTPUT_COLUMNS[job_name]
            cast_sql = ", ".join(f"cast({c} as {t}) AS {c}" for c, t in columns)
            full_sql = f"SELECT {cast_sql} FROM ({sql})"
            return con.execute(full_sql, {"d": day}).fetchall()
        finally:
            con.close()

    def _persist_output(self, job_name: str, rows, attempt_dir: Path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        columns = OUTPUT_COLUMNS[job_name]
        type_map = {
            "VARCHAR": pa.string(),
            "DECIMAL(18,3)": pa.decimal128(18, 3),
        }
        schema = pa.schema([pa.field(c, type_map[t]) for c, t in columns])
        cols = {c: [row[i] for row in rows] for i, (c, _) in enumerate(columns)}
        table = pa.table({c: cols[c] for c, _ in columns}, schema=schema)

        # 先落到尝试目录作为执行证据，再按内容哈希入仓（去重、不可变）
        local_out = attempt_dir / "output.parquet"
        pq.write_table(table, local_out, compression="zstd")
        data = local_out.read_bytes()
        output_sha = hashlib.sha256(data).hexdigest()
        target = self.config.artifact_path(output_sha)
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        return target, output_sha, table.num_rows


def run_loop(config: Config, catalog: Catalog, graph: DependencyGraph,
             run_id: str, worker_id: str, max_tasks: int | None = None) -> list[dict]:
    """一个调度进程的领取-执行循环；两个进程可同时对同一 run 调用。"""
    import time

    runner = Runner(config, catalog, graph, worker_id)
    results: list[dict] = []
    processed = 0
    idle_rounds = 0
    while True:
        claim = catalog.claim_next(run_id, graph, worker_id)
        if claim is not None:
            idle_rounds = 0
            result = runner.execute_node(run_id, claim["job_name"], claim["day"])
            results.append(result)
            processed += 1
            if max_tasks is not None and processed >= max_tasks:
                break
            continue
        # 本轮无可领取：若还有其他 worker 在执行，等待其释放后再试，
        # 避免把“等前置”的 pending 误判为 run 结束
        states = {
            r["status"]
            for r in catalog.con.execute(
                "SELECT status FROM run_items WHERE run_id=?", (run_id,)
            ).fetchall()
        }
        if "running" in states:
            idle_rounds += 1
            if idle_rounds > 600:  # 约 5 分钟兜底，防止崩溃 worker 永久挂起
                break
            time.sleep(0.5)
            continue
        break
    return results
