#!/usr/bin/env python3
"""双调度进程竞争验收：两个进程对同一 run 并发领取，验证无重复执行。

用法：python3 -I scripts/race_demo.py <warehouse_dir>
流程：登记原件 -> 规划 run -> 两个子进程并发 worker -> 汇总领取归属与终态。
"""
import json
import sqlite3
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WH = Path(sys.argv[1]).resolve()


def cli(*args):
    return subprocess.run(
        [sys.executable, "-I", str(ROOT / "scripts/wh.py"), "-w", str(WH), *args],
        capture_output=True, text=True, check=True,
    )


def main():
    for table in ("raw_day", "opening", "calendar"):
        cli("register-parquet", "--table", table,
            "--file", str(ROOT / f"inputs/{table}.parquet"))
    plan = json.loads(cli("plan", "--from", "2026-09-25", "--to", "2026-10-03").stdout)
    run_id = plan["run_id"]
    print("run:", run_id, "规划节点:", len(plan["nodes"]))

    procs = [
        subprocess.Popen(
            [sys.executable, "-I", str(ROOT / "scripts/wh.py"), "-w", str(WH),
             "worker", "--run-id", run_id, "--worker-id", f"worker-{name}",
             "--max-tasks", "8"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for name in ("A", "B")
    ]
    outputs = [p.communicate() for p in procs]
    for name, (out, err), p in zip(("A", "B"), outputs, procs):
        if p.returncode != 0:
            raise SystemExit(f"worker {name} 失败: {err}")

    nodes_a = [tuple(x["node"]) for x in json.loads(outputs[0][0])]
    nodes_b = [tuple(x["node"]) for x in json.loads(outputs[1][0])]
    overlap = set(nodes_a) & set(nodes_b)
    print("worker-A 执行:", len(nodes_a), "worker-B 执行:", len(nodes_b))
    print("重叠执行:", overlap if overlap else "无（条件 UPDATE 原子领取生效）")

    con = sqlite3.connect(WH / "meta/catalog.sqlite")
    con.row_factory = sqlite3.Row
    states = Counter(r[0] for r in con.execute(
        "SELECT status FROM run_items"))
    owners = Counter(r[0] for r in con.execute(
        "SELECT claimed_by FROM run_items WHERE claimed_by IS NOT NULL"))
    print("run 终态:", dict(states))
    print("领取归属:", dict(owners))
    assert not overlap, "同一节点被两个进程执行"
    assert states.get("committed") == 15, states
    print("OK: 15 个可计算节点恰好各执行一次")


if __name__ == "__main__":
    main()
