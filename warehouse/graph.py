"""作业依赖图：按业务日期偏移展开依赖，而不是按表名排序。

支持的关系：
- 同日依赖 offset=0（daily 读 raw_day；balance 读同日 daily；turnover 读同日 balance）
- 前一日余额 offset=-1（balance 自依赖，递归到 opening 锚点为止）
- 前向七日窗口 offsets=-6..0（turnover 读七张 daily 日分区）
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# opening 是明确给定的 2026-09-30 初始余额，作为 balance 链条的起算锚点；
# 不继续向更早日期递归，也不默认其他日期为 0。
ANCHOR_SOURCE_TABLE = "opening"
ANCHOR_DAY = "2026-09-30"
ANCHOR_TARGET_JOB = "balance"
ANCHOR_TARGET_DAY = "2026-10-01"


def add_days(day: str, offset: int) -> str:
    return (date.fromisoformat(day) + timedelta(days=offset)).isoformat()


@dataclass(frozen=True)
class Dependency:
    table: str
    offsets: tuple[int, ...]


@dataclass(frozen=True)
class JobSpec:
    name: str
    depends: tuple[Dependency, ...]
    sql: str
    spec_sha256: str

    def dependency_refs(self, day: str) -> list[tuple[str, str, int]]:
        """返回 (输入表, 输入业务日期, 偏移) 的完整展开。"""
        refs: list[tuple[str, str, int]] = []
        for dep in self.depends:
            for offset in dep.offsets:
                refs.append((dep.table, add_days(day, offset), offset))
        return refs


def load_job_specs(path: str | Path) -> dict[str, JobSpec]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    specs: dict[str, JobSpec] = {}
    for name, body in raw.items():
        payload = json.dumps(body, ensure_ascii=False, sort_keys=True).encode("utf-8")
        depends = tuple(
            Dependency(d["table"], tuple(d["offsets"])) for d in body["depends"]
        )
        specs[name] = JobSpec(
            name=name,
            depends=depends,
            sql=body["sql"],
            spec_sha256=hashlib.sha256(payload).hexdigest(),
        )
    return specs


class DependencyGraph:
    def __init__(self, specs: dict[str, JobSpec]):
        self.specs = specs
        self.job_names = set(specs)

    def is_job(self, table: str) -> bool:
        return table in self.job_names

    def refs(self, job_name: str, day: str) -> list[tuple[str, str, int]]:
        return self.specs[job_name].dependency_refs(day)

    def job_levels(self) -> list[list[str]]:
        """同日作业的拓扑层级（Kahn），跨日关系由规划器按日期保证。"""
        indegree = {name: 0 for name in self.specs}
        edges: dict[str, list[str]] = {name: [] for name in self.specs}
        for name, spec in self.specs.items():
            for dep in spec.depends:
                if dep.table in self.specs and 0 in dep.offsets:
                    edges[dep.table].append(name)
                    indegree[name] += 1
        levels: list[list[str]] = []
        remaining = dict(indegree)
        while remaining:
            level = sorted(n for n, d in remaining.items() if d == 0)
            if not level:
                raise RuntimeError("作业依赖存在同日环: " + ",".join(remaining))
            levels.append(level)
            for n in level:
                remaining.pop(n)
                for nxt in edges[n]:
                    remaining[nxt] -= 1
        return levels

    def topo_order(self) -> list[str]:
        """同日作业的扁平拓扑序，用于领取排序。"""
        return [name for level in self.job_levels() for name in level]

    def reverse_impact(
        self, changed: set[tuple[str, str]], through: str
    ) -> set[tuple[str, str]]:
        """从变更分区反向传播，找出真正受影响的下游 (作业, 日期)。

        边 (input_table@x, offset=k) -> job@d，其中 x=d+k，故 d=x-k。
        本项目偏移均 <=0，影响只向同日或未来日期传播。
        """
        affected: set[tuple[str, str]] = set()
        queue = list(changed)
        seen = set(changed)
        while queue:
            table, day = queue.pop(0)
            candidates: list[tuple[str, str]] = []
            # 常规偏移边
            for job_name, spec in self.specs.items():
                for dep in spec.depends:
                    if dep.table != table:
                        continue
                    for offset in dep.offsets:
                        target_day = add_days(day, -offset)
                        candidates.append((job_name, target_day))
            # opening 锚点边：opening@09-30 -> balance@10-01
            if table == ANCHOR_SOURCE_TABLE and day == ANCHOR_DAY:
                candidates.append((ANCHOR_TARGET_JOB, ANCHOR_TARGET_DAY))
            for node in candidates:
                job_name, target_day = node
                if target_day > through:
                    continue
                if job_name not in self.specs:
                    continue
                if node in seen:
                    continue
                seen.add(node)
                affected.add(node)
                queue.append(node)
        return affected

    def anchor_ref(self, table: str, dep_day: str) -> tuple[str, str] | None:
        """balance 的 -1 依赖落在锚点日时，改由 opening 锚点满足。"""
        if (
            table == ANCHOR_TARGET_JOB
            and dep_day == ANCHOR_DAY
        ):
            return ANCHOR_SOURCE_TABLE, ANCHOR_DAY
        return None
