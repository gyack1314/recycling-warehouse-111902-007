"""预提交竞争钩子：作业执行期间修订 raw_day@2026-10-01（revision 2）。

由 WH_PRECOMMIT_HOOK 环境变量指定，在 SQL 已执行、发布新鲜度复核之前触发，
用于验证 stale 处置：旧尝试产物保留但不发布，current 不被覆盖。
仓库与作业路径通过环境变量传入，钩子自身不持有业务状态。
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from warehouse.api import open_warehouse  # noqa: E402

FIRED = False


def fire():
    global FIRED
    if FIRED:
        return
    FIRED = True
    warehouse = os.environ["WH_TEST_WAREHOUSE"]
    jobs = os.environ.get("WH_TEST_JOBS", str(ROOT / "inputs/jobs.json"))
    schema = os.environ.get("WH_TEST_SCHEMA", str(ROOT / "inputs/raw_day.parquet"))
    rows_json = os.environ.get(
        "WH_TEST_REVISION_ROWS",
        '[{"day":"2026-10-01","site":"N","received":"90.000","shipped":"30.000","revision":2}]',
    )
    import json

    rows = json.loads(rows_json)
    with open_warehouse(warehouse, jobs) as wh:
        rec = wh.register_rows("raw_day", "2026-10-01", rows, schema,
                               note="precommit 竞争修订")
        print(f"[hook] 执行期间登记修订: {rec}", file=sys.stderr)
