"""仓库配置与目录布局。

目录职责：
- raw/      不可变原件分区（按 表/day 分区，每次登记产生一个新版本目录）
- artifacts/ 内容寻址的不可变作业产物（含零行 parquet，按 sha256 去重）
- attempts/ 每次尝试的输入清单、输出与证据
- snapshots/ 已发布的固定版本快照（manifest + duckdb）
- meta/catalog.sqlite 证据目录（修订、尝试、产物、快照、运行项）
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

CATALOG_NAME = "catalog.sqlite"


@dataclass(frozen=True)
class Config:
    root: Path

    @property
    def raw_dir(self) -> Path:
        return self.root / "raw"

    @property
    def artifacts_dir(self) -> Path:
        return self.root / "artifacts"

    @property
    def attempts_dir(self) -> Path:
        return self.root / "attempts"

    @property
    def snapshots_dir(self) -> Path:
        return self.root / "snapshots"

    @property
    def meta_dir(self) -> Path:
        return self.root / "meta"

    @property
    def db_path(self) -> Path:
        return self.meta_dir / CATALOG_NAME

    @property
    def lock_path(self) -> Path:
        return self.meta_dir / "catalog.lock"

    def ensure(self) -> "Config":
        for p in (
            self.raw_dir,
            self.artifacts_dir,
            self.attempts_dir,
            self.snapshots_dir,
            self.meta_dir,
        ):
            p.mkdir(parents=True, exist_ok=True)
        return self

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA busy_timeout=30000")
        con.execute("PRAGMA synchronous=NORMAL")
        return con

    def partition_dir(self, table: str, day: str, revision: int) -> Path:
        return self.raw_dir / table / f"dt={day}" / f"rev={revision}"

    def attempt_dir(self, attempt_id: str) -> Path:
        return self.attempts_dir / attempt_id

    def artifact_path(self, sha: str) -> Path:
        return self.artifacts_dir / sha[:2] / f"{sha}.parquet"

    def snapshot_dir(self, snapshot_id: str) -> Path:
        return self.snapshots_dir / snapshot_id


def resolve_config(path: str | os.PathLike[str] | None) -> Config:
    if path is None:
        path = os.environ.get("WH_WAREHOUSE", "var/warehouse")
    return Config(Path(path).resolve()).ensure()
