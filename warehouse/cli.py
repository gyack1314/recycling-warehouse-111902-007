"""命令行入口：登记修订、查看影响、运行补算、领取结果。

示例：
  python -m warehouse.cli register-parquet --table raw_day --file inputs/raw_day.parquet
  python -m warehouse.cli apply-actions --actions inputs/actions.json --schema inputs/raw_day.parquet
  python -m warehouse.cli impact --through 2026-10-03
  python -m warehouse.cli run --from 2026-10-01 --to 2026-10-03
  python -m warehouse.cli claim --from 2026-10-01 --to 2026-10-03
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .api import open_warehouse
from .snapshot import SnapshotError

DEFAULT_JOBS = "inputs/jobs.json"


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="warehouse",
        description="回收日仓：依赖感知的补算编排与固定版本查询",
    )
    parser.add_argument("--warehouse", "-w", default=None,
                        help="仓库根目录（默认 WH_WAREHOUSE 或 var/warehouse）")
    parser.add_argument("--jobs", default=None, help="SQL 作业定义 JSON")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("register-parquet", help="登记一个真实 Parquet 原件（按 day 分区修订）")
    p.add_argument("--table", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--note")

    p = sub.add_parser("register-rows", help="登记某业务日的行（零行需配合 --schema 明确为空）")
    p.add_argument("--table", required=True)
    p.add_argument("--day", required=True)
    p.add_argument("--rows-json", required=True, help="JSON 数组字符串或 @文件路径")
    p.add_argument("--schema", required=True, help="零行时推断 schema 的 parquet")
    p.add_argument("--note")

    p = sub.add_parser("clear", help="明确清空某分区（零行证据，区别于缺分区）")
    p.add_argument("--table", required=True)
    p.add_argument("--day", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--note")

    p = sub.add_parser("apply-actions", help="按顺序应用 actions.json 的 replace_partition")
    p.add_argument("--actions", required=True)
    p.add_argument("--schema", required=True)

    p = sub.add_parser("revisions", help="查看修订历史")
    p.add_argument("--table")
    p.add_argument("--day")

    p = sub.add_parser("sources", help="查看当前原件分区（present/cleared，未列出即 missing）")
    p.add_argument("--table")

    p = sub.add_parser("impact", help="查看受影响的下游作业/日期，不执行计算")
    p.add_argument("--through", required=True, help="影响传播截止日期（含端点）")
    p.add_argument("--table-day", action="append", default=[],
                   help="指定变更点 表:日期，可重复；省略则取全部当前原件分区")

    p = sub.add_parser("plan", help="只规划补算运行项，不执行")
    p.add_argument("--from", dest="day_from", required=True)
    p.add_argument("--to", dest="day_to", required=True)

    p = sub.add_parser("run", help="规划并执行补算（本进程内领取直到无可领取项）")
    p.add_argument("--from", dest="day_from", required=True)
    p.add_argument("--to", dest="day_to", required=True)
    p.add_argument("--worker-id", default="worker-solo")
    p.add_argument("--max-tasks", type=int, default=None)
    p.add_argument("--plan-only", action="store_true")

    p = sub.add_parser("worker", help="作为调度进程对已有 run 竞争领取执行")
    p.add_argument("--run-id", required=True)
    p.add_argument("--worker-id", required=True)
    p.add_argument("--max-tasks", type=int, default=None)

    p = sub.add_parser("retry", help="独立重试某个失败/阻断节点（旧尝试保留，重新读输入）")
    p.add_argument("--run-id", required=True)
    p.add_argument("--job", required=True)
    p.add_argument("--day", required=True)
    p.add_argument("--worker-id", default="worker-retry")

    p = sub.add_parser("status", help="查看运行项、尝试、产物与快照总览")
    p.add_argument("--run-id")
    p.add_argument("--job")

    p = sub.add_parser("claim", help="领取固定版本的自洽仓库结果（manifest + DuckDB）")
    p.add_argument("--from", dest="day_from", required=True)
    p.add_argument("--to", dest="day_to", required=True)
    p.add_argument("--run-id")
    p.add_argument("--note")
    p.add_argument("--allow-partial", action="store_true",
                   help="允许把缺失/过期条目显式记录为问题（默认拒绝不自洽快照）")

    p = sub.add_parser("snapshots", help="列出历史快照")

    p = sub.add_parser("verify-snapshot", help="复核快照 parquet 与 sha256")
    p.add_argument("--snapshot-id", required=True)

    p = sub.add_parser("fallback", help="取上一套已发布快照（失败补算期间仍可查询）")

    return parser


def _load_rows(arg: str) -> list[dict]:
    if arg.startswith("@"):
        return json.loads(Path(arg[1:]).read_text(encoding="utf-8"))
    return json.loads(arg)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    jobs_path = args.jobs or DEFAULT_JOBS
    with open_warehouse(args.warehouse, jobs_path) as wh:
        try:
            if args.cmd == "register-parquet":
                _print(wh.register_parquet(args.table, args.file, args.note))
            elif args.cmd == "register-rows":
                _print(wh.register_rows(args.table, args.day,
                                        _load_rows(args.rows_json), args.schema, args.note))
            elif args.cmd == "clear":
                _print(wh.clear(args.table, args.day, args.schema, args.note))
            elif args.cmd == "apply-actions":
                _print(wh.apply_actions(args.actions, args.schema))
            elif args.cmd == "revisions":
                _print(wh.revisions(args.table, args.day))
            elif args.cmd == "sources":
                _print(wh.sources(args.table))
            elif args.cmd == "impact":
                changed = None
                if args.table_day:
                    changed = [tuple(x.split(":", 1)) for x in args.table_day]
                _print(wh.impact(args.through, changed))
            elif args.cmd == "plan":
                _print(wh.plan(args.day_from, args.day_to))
            elif args.cmd == "run":
                plan = wh.plan(args.day_from, args.day_to)
                if args.plan_only:
                    _print(plan)
                    return 0
                results = wh.work(plan["run_id"], args.worker_id, args.max_tasks)
                _print({"plan": plan, "executed": results,
                        "items": wh.run_items(plan["run_id"])})
            elif args.cmd == "worker":
                _print(wh.work(args.run_id, args.worker_id, args.max_tasks))
            elif args.cmd == "retry":
                outcome = wh.retry(args.run_id, args.job, args.day)
                results = wh.work(args.run_id, args.worker_id)
                _print({"retry": outcome, "executed": results,
                        "items": wh.run_items(args.run_id)})
            elif args.cmd == "status":
                payload = {
                    "sources": wh.sources(args.job),
                    "products": wh.products(args.job),
                    "attempts": wh.attempts(args.run_id),
                    "run_items": wh.run_items(args.run_id) if args.run_id else [],
                    "snapshots": wh.snapshots(),
                }
                _print(payload)
            elif args.cmd == "claim":
                _print(wh.claim(args.day_from, args.day_to, args.run_id,
                                args.note, args.allow_partial))
            elif args.cmd == "snapshots":
                _print(wh.snapshots())
            elif args.cmd == "verify-snapshot":
                _print(wh.verify_snapshot(args.snapshot_id))
            elif args.cmd == "fallback":
                _print(wh.fallback_snapshot())
            else:  # pragma: no cover
                return 2
        except SnapshotError as exc:
            print(f"快照被拒绝: {exc}", file=sys.stderr)
            return 3
        except (RuntimeError, KeyError, FileNotFoundError, ValueError) as exc:
            print(f"操作失败: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
