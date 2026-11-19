"""仓库领取：把一套自洽的 current 产物固定成可查询快照。

一致性规则：
- 快照内每个 (作业,日) 条目都必须是 is_current 产物，且其输入指纹仍能被当前目录复算；
- 任一条目已过期（输入被修订而未重算）则拒绝“当前快照”，避免把新日明细、旧库存、
  半截周转表拼进同一次读取；
- 历史快照目录与 manifest 不可变，失败的补算不会删除上一套自洽结果，
  使用者可显式回退领取（claim --snapshot 或 fallback）；
- DuckDB 查询库中的视图绑定不可变 parquet 路径，查询结果可复现。
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .catalog import Catalog, new_id
from .config import Config
from .graph import DependencyGraph
from .planner import daterange


class SnapshotError(RuntimeError):
    pass


class SnapshotService:
    def __init__(self, config: Config, catalog: Catalog, graph: DependencyGraph):
        self.config = config
        self.catalog = catalog
        self.graph = graph

    def _collect_entries(self, day_from: str, day_to: str,
                         jobs: list[str]) -> tuple[list[dict], list[dict]]:
        entries: list[dict] = []
        problems: list[dict] = []
        for day in daterange(day_from, day_to):
            for job_name in jobs:
                product = self.catalog.current_product(job_name, day)
                if product is None:
                    problems.append({
                        "job": job_name, "day": day,
                        "problem": "缺少当前产物（可能未算完或被阻断）",
                    })
                    continue
                # 新鲜度：用当前目录重新解析输入并比对发布时指纹
                resolved, blockers = self.catalog.resolve_attempt_inputs(
                    self.graph, job_name, day
                )
                if blockers:
                    problems.append({"job": job_name, "day": day,
                                     "problem": "前置缺失", "blockers": blockers})
                    continue
                fp = self.catalog.fingerprint_of(
                    resolved, self.catalog.current_spec_sha(job_name)
                )
                if fp != product["input_fingerprint"]:
                    problems.append({
                        "job": job_name, "day": day,
                        "problem": "产物已因输入变动而过期，需补算后才能一起查询",
                        "published_fingerprint": product["input_fingerprint"],
                        "current_fingerprint": fp,
                    })
                    continue
                entries.append({
                    "job_name": job_name,
                    "day": day,
                    "product_id": product["product_id"],
                    "output_sha256": product["output_sha256"],
                    "output_path": product["output_path"],
                    "row_count": product["row_count"],
                    "input_fingerprint": product["input_fingerprint"],
                    "attempt_id": product["attempt_id"],
                })
        return entries, problems

    def build_snapshot(self, day_from: str, day_to: str,
                       run_id: str | None = None,
                       note: str | None = None,
                       allow_partial: bool = False) -> dict:
        jobs = [j for level in self.graph.job_levels() for j in level]
        entries, problems = self._collect_entries(day_from, day_to, jobs)
        if problems and not allow_partial:
            raise SnapshotError(
                "无法形成自洽快照：\n"
                + "\n".join(f"- {p['job']}@{p['day']}: {p['problem']}" for p in problems)
            )

        snapshot_id = new_id("snap")
        snap_dir = self.config.snapshot_dir(snapshot_id)
        snap_dir.mkdir(parents=True)
        manifest = {
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "day_from": day_from,
            "day_to": day_to,
            "jobs": jobs,
            "consistent": not problems,
            "entries": entries,
            "problems": problems,
            "note": note,
        }
        manifest_path = snap_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        duckdb_path = snap_dir / "warehouse.duckdb"
        self._build_duckdb(duckdb_path, jobs, entries)

        self.catalog.record_snapshot(
            snapshot_id, run_id, day_from, day_to,
            str(manifest_path), str(duckdb_path), entries, note,
        )
        return {"snapshot_id": snapshot_id,
                "manifest_path": str(manifest_path),
                "duckdb_path": str(duckdb_path),
                "entry_count": len(entries),
                "problems": problems,
                "consistent": not problems}

    def _build_duckdb(self, duckdb_path: Path, jobs: list[str],
                      entries: list[dict]) -> None:
        if duckdb_path.exists():
            duckdb_path.unlink()
        con = duckdb.connect(str(duckdb_path))
        try:
            for job_name in jobs:
                job_entries = sorted(
                    (e for e in entries if e["job_name"] == job_name),
                    key=lambda e: e["day"],
                )
                if not job_entries:
                    continue
                # 固定到快照时的不可变路径（artifacts 内容寻址，永不覆写）
                parts = [
                    f"SELECT * FROM read_parquet('{e['output_path']}', union_by_name=true)"
                    for e in job_entries
                ]
                con.execute(f"CREATE VIEW {job_name} AS " + " UNION ALL ".join(parts))
                con.execute(
                    "CREATE TABLE IF NOT EXISTS snapshot_evidence "
                    "(job_name VARCHAR, day VARCHAR, output_sha256 VARCHAR, "
                    "input_fingerprint VARCHAR, attempt_id VARCHAR)"
                )
                con.executemany(
                    "INSERT INTO snapshot_evidence VALUES (?,?,?,?,?)",
                    [(e["job_name"], e["day"], e["output_sha256"],
                      e["input_fingerprint"], e["attempt_id"]) for e in job_entries],
                )
        finally:
            con.close()

    def fallback_snapshot(self) -> dict:
        """返回上一套已发布快照的元数据（其文件不可变，仍可查询）。"""
        snap = self.catalog.latest_snapshot()
        if snap is None:
            raise SnapshotError("没有任何历史快照可回退")
        return snap

    def verify_snapshot(self, snapshot_id: str) -> dict:
        """复核历史快照：逐条检查 manifest 中的 parquet 与 sha256 仍在且未变。"""
        snap_dir = self.config.snapshot_dir(snapshot_id)
        manifest = json.loads((snap_dir / "manifest.json").read_text(encoding="utf-8"))
        checks = []
        for entry in manifest["entries"]:
            p = Path(entry["output_path"])
            import hashlib

            ok = p.exists() and hashlib.sha256(p.read_bytes()).hexdigest() == entry["output_sha256"]
            checks.append({"job": entry["job_name"], "day": entry["day"], "verified": ok})
        return {"snapshot_id": snapshot_id,
                "all_verified": all(c["verified"] for c in checks),
                "checks": checks}
