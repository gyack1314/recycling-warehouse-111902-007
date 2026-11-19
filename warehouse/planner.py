"""影响分析与补算规划：把日期偏移依赖展开成真正要重算的节点集合。

规划纪律：
- 影响只沿依赖边按业务日期传播，不能因为“同名表”就整表重算；
- 区间内未受影响、且输入指纹仍匹配的 current 产物继续可用（skipped）；
- 缺少必要前置分区的节点标 blocked 并给出原因，绝不默认补零；
- 运营只申请一段日期时，窗口/余额链需要的前置缺失会被明确指出。
"""
from __future__ import annotations

import fcntl
from contextlib import contextmanager
from datetime import date, timedelta

from .catalog import Catalog, new_id
from .config import Config
from .graph import DependencyGraph


def daterange(day_from: str, day_to: str) -> list[str]:
    a = date.fromisoformat(day_from)
    b = date.fromisoformat(day_to)
    if a > b:
        raise ValueError(f"区间反转: {day_from} > {day_to}")
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


@contextmanager
def planning_lock(config: Config):
    """规划类动作（登记修订、建 run、发布快照）的跨进程互斥。"""
    config.ensure()
    handle = open(config.lock_path, "w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(handle, fcntl.LOCK_UN)
        handle.close()


class Planner:
    def __init__(self, config: Config, catalog: Catalog, graph: DependencyGraph):
        self.config = config
        self.catalog = catalog
        self.graph = graph

    def impact(self, changed: set[tuple[str, str]], through: str) -> dict:
        """计算受影响节点，并区分：待重算 / 已是最新 / 被阻断。"""
        affected = self.graph.reverse_impact(changed, through)
        pending: list[dict] = []
        up_to_date: list[dict] = []
        blocked: list[dict] = []
        for job_name, day in sorted(affected, key=lambda x: (x[1], x[0])):
            resolved, blockers = self.catalog.resolve_attempt_inputs(self.graph, job_name, day)
            if blockers:
                blocked.extend(blockers)
                continue
            spec = self.graph.specs[job_name]
            fp = self.catalog.fingerprint_of(resolved, spec.spec_sha256)
            product = self.catalog.current_product(job_name, day)
            if product is not None and product["input_fingerprint"] == fp:
                up_to_date.append({"job": job_name, "day": day,
                                   "product_id": product["product_id"]})
            else:
                pending.append({"job": job_name, "day": day,
                                "reason": "输入变更或尚无产物"})
        return {
            "changed_sources": sorted(changed),
            "through": through,
            "affected_nodes": sorted(affected),
            "pending_recompute": pending,
            "up_to_date": up_to_date,
            "blocked": blocked,
        }

    def plan_run(self, day_from: str, day_to: str,
                 only_impacted: set[tuple[str, str]] | None = None) -> dict:
        """在 [day_from, day_to] 上规划全部作业的节点集合。"""
        run_id = new_id("run")
        days = daterange(day_from, day_to)
        levels = self.graph.job_levels()
        ordered_jobs = [j for level in levels for j in level]

        nodes: list[tuple[str, str]] = []
        blocked: list[dict] = []
        for day in days:
            for job_name in ordered_jobs:
                if only_impacted is not None and (job_name, day) not in only_impacted:
                    continue
                nodes.append((job_name, day))
                # 注意：不在规划期判定 skipped——同一次 run 中前置节点稍后会被重算，
                # 此刻匹配的指纹会在前置提交后失效。是否跳过推迟到领取时按当前目录裁决。
                _, blockers = self.catalog.resolve_attempt_inputs(
                    self.graph, job_name, day
                )
                if blockers:
                    blocked.extend(blockers)

        self.catalog.create_run(run_id, nodes)
        for blocker in blocked:
            self.catalog.set_item_status(
                run_id, blocker["job"], blocker["day"], "blocked",
                blocker_json=[blocker],
            )
        return {
            "run_id": run_id,
            "day_from": day_from,
            "day_to": day_to,
            "nodes": nodes,
            "blocked_at_planning": blocked,
        }

    def retry_item(self, run_id: str, job_name: str, day: str) -> dict:
        """独立重试：旧尝试证据保留，运行项回到 pending，由下一次领取重新计算。

        不允许用旧尝试的 success 标志跳过——重试必然重新解析输入、生成新指纹。
        """
        item = self.catalog.get_item(run_id, job_name, day)
        if item is None:
            raise KeyError(f"运行项不存在: {run_id}/{job_name}@{day}")
        if item["status"] not in ("failed", "blocked", "running"):
            raise RuntimeError(
                f"节点状态为 {item['status']}，只有 failed/blocked/超时 running 可重试"
            )
        self.catalog.set_item_status(run_id, job_name, day, "pending",
                                     claimed_by=None, lease_until=None,
                                     blocker_json=None)
        return {"run_id": run_id, "job": job_name, "day": day,
                "previous_attempt": item["attempt_id"], "status": "pending"}
