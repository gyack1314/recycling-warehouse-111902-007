"""对外门面：把目录、锁、图与服务组装成四类入口 + 状态查询。

入口对应验收要求：
- register：登记修订（原件分区 / 明确清空 / actions 批量应用）
- impact：查看真正受影响的下游日期
- run/retry：运行补算、独立失败重试、双调度进程竞争领取
- claim：领取一套固定版本的自洽仓库结果用于 DuckDB 查询
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pyarrow.parquet as pq

from .catalog import Catalog
from .config import Config, resolve_config
from .graph import DependencyGraph, load_job_specs
from .ingest import apply_actions, clear_partition, register_parquet, register_rows
from .planner import Planner, planning_lock
from .runner import run_loop
from .snapshot import SnapshotService


class Warehouse:
    def __init__(self, config: Config, jobs_path: str | Path):
        self.config = config.ensure()
        self.jobs_path = Path(jobs_path)
        specs = load_job_specs(self.jobs_path)
        self.graph = DependencyGraph(specs)
        con = self.config.connect()
        self.catalog = Catalog(con)
        with planning_lock(self.config):
            self.catalog.upsert_job_specs(specs)

    def close(self) -> None:
        self.catalog.con.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---------- 登记修订 ----------
    def register_parquet(self, table: str, parquet_path: str,
                         note: str | None = None) -> list[dict]:
        with planning_lock(self.config):
            return register_parquet(self.config, self.catalog, table, parquet_path, note=note)

    def register_rows(self, table: str, day: str, rows: list[dict],
                      schema_hint_path: str, note: str | None = None) -> dict:
        hint = pq.read_table(schema_hint_path)
        with planning_lock(self.config):
            return register_rows(self.config, self.catalog, table, day, rows,
                                 note=note, empty_schema_hint=hint)

    def clear(self, table: str, day: str, schema_hint_path: str,
              note: str | None = None) -> dict:
        hint = pq.read_table(schema_hint_path)
        with planning_lock(self.config):
            return clear_partition(self.config, self.catalog, table, day, hint, note=note)

    def apply_actions(self, actions_path: str, schema_hint_path: str) -> list[dict]:
        hint = pq.read_table(schema_hint_path)
        with planning_lock(self.config):
            return apply_actions(self.config, self.catalog, actions_path, hint)

    def revisions(self, table: str | None = None, day: str | None = None) -> list[dict]:
        return self.catalog.list_revisions(table, day)

    def sources(self, table: str | None = None) -> list[dict]:
        return self.catalog.list_source_partitions(table)

    # ---------- 影响范围 ----------
    def impact(self, through: str,
               tables_days: list[tuple[str, str]] | None = None) -> dict:
        with planning_lock(self.config):
            if tables_days is None:
                # 默认取所有当前原件分区（首次基线影响分析）
                changed = {(r["table_name"], r["day"]) for r in self.catalog.list_source_partitions()}
            else:
                changed = set(tables_days)
            return Planner(self.config, self.catalog, self.graph).impact(changed, through)

    # ---------- 补算 ----------
    def plan(self, day_from: str, day_to: str,
             impacted_only: list[tuple[str, str]] | None = None) -> dict:
        with planning_lock(self.config):
            planner = Planner(self.config, self.catalog, self.graph)
            only = set(impacted_only) if impacted_only is not None else None
            return planner.plan_run(day_from, day_to, only)

    def work(self, run_id: str, worker_id: str,
             max_tasks: int | None = None) -> list[dict]:
        """调度进程入口；两个进程用不同 worker_id 同时调用即竞争领取。"""
        return run_loop(self.config, self.catalog, self.graph,
                        run_id, worker_id, max_tasks)

    def retry(self, run_id: str, job_name: str, day: str) -> dict:
        with planning_lock(self.config):
            return Planner(self.config, self.catalog, self.graph).retry_item(
                run_id, job_name, day
            )

    def run_items(self, run_id: str) -> list[dict]:
        return self.catalog.list_items(run_id)

    def attempts(self, run_id: str | None = None) -> list[dict]:
        return self.catalog.list_attempts(run_id)

    def products(self, job_name: str | None = None) -> list[dict]:
        return self.catalog.list_products(job_name)

    # ---------- 领取 ----------
    def claim(self, day_from: str, day_to: str,
              run_id: str | None = None, note: str | None = None,
              allow_partial: bool = False) -> dict:
        with planning_lock(self.config):
            return SnapshotService(self.config, self.catalog, self.graph).build_snapshot(
                day_from, day_to, run_id=run_id, note=note, allow_partial=allow_partial
            )

    def snapshots(self) -> list[dict]:
        return self.catalog.list_snapshots()

    def verify_snapshot(self, snapshot_id: str) -> dict:
        return SnapshotService(self.config, self.catalog, self.graph).verify_snapshot(
            snapshot_id
        )

    def fallback_snapshot(self) -> dict:
        return SnapshotService(self.config, self.catalog, self.graph).fallback_snapshot()


def open_warehouse(warehouse_path: str | None, jobs_path: str | Path) -> Warehouse:
    return Warehouse(resolve_config(warehouse_path), jobs_path)


@contextmanager
def warehouse_session(warehouse_path: str | None, jobs_path: str | Path):
    wh = open_warehouse(warehouse_path, jobs_path)
    try:
        yield wh
    finally:
        wh.close()
