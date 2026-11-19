"""证据目录的数据模型与建表语句。

状态语义（验收要求能区分三类）：
- 已算完且当前可用：run_items.status='committed' 且 products.is_current=1
- 因输入变动而过期：attempts.status='stale'（证据保留，不发布），
  或 products.is_current=0（被更新版本替换的旧产物）
- 可一起用于查询：snapshots 中的 manifest，所有条目指向同一套自洽 current 产物
"""
from __future__ import annotations

SCHEMA_VERSION = 1

# 尝试终态
ATTEMPT_SUCCESS = "success"
ATTEMPT_STALE = "stale"
ATTEMPT_FAILED = "failed"

# 运行项状态
ITEM_PENDING = "pending"      # 已规划，等待前置
ITEM_READY = "ready"          # 前置已提交，可领取
ITEM_RUNNING = "running"      # 已被某调度进程领取
ITEM_COMMITTED = "committed"  # 成功并发布
ITEM_FAILED = "failed"        # 尝试失败，可独立重试
ITEM_BLOCKED = "blocked"      # 缺少必要前置，默认不补零
ITEM_SKIPPED = "skipped"      # 命中当前自洽产物，无需重算

# 原始分区状态
PRESENT = "present"   # 有行或零行的明确分区（零行 != 缺分区）
CLEARED = "cleared"   # 明确声明该日完整为空（actions rows=[] 等）
MISSING = "missing"   # 从未登记，不是零

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 作业定义的登记指纹，定义变化会让旧产物失效
CREATE TABLE IF NOT EXISTS job_specs (
    job_name TEXT PRIMARY KEY,
    spec_json TEXT NOT NULL,
    spec_sha256 TEXT NOT NULL,
    registered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- 原始分区修订：raw_day 等源表的每次 replace/clear
CREATE TABLE IF NOT EXISTS source_revisions (
    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    day TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('present','cleared')),
    revision_no INTEGER NOT NULL,
    path TEXT NOT NULL,
    file_sha256 TEXT,
    row_count INTEGER,
    registered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    origin TEXT NOT NULL DEFAULT 'register',
    note TEXT,
    UNIQUE(table_name, day, revision_no)
);

-- 每个分区当前指针
CREATE TABLE IF NOT EXISTS source_current (
    table_name TEXT NOT NULL,
    day TEXT NOT NULL,
    revision_id INTEGER NOT NULL REFERENCES source_revisions(revision_id),
    state TEXT NOT NULL,
    PRIMARY KEY (table_name, day)
);

-- 一次确定的尝试：同 作业/日/输入指纹 复用同一定性
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT,
    job_name TEXT NOT NULL,
    day TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('success','stale','failed')),
    input_fingerprint TEXT NOT NULL,
    input_manifest_json TEXT NOT NULL,
    output_sha256 TEXT,
    output_path TEXT,
    row_count INTEGER,
    error TEXT,
    spec_sha256 TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    finished_at TEXT,
    worker_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_attempts_node ON attempts(job_name, day);

-- 产物指针：同一 (作业,日) 保留全部历史版本，current 唯一
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    day TEXT NOT NULL,
    attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id),
    output_sha256 TEXT NOT NULL,
    output_path TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    input_fingerprint TEXT NOT NULL,
    is_current INTEGER NOT NULL DEFAULT 1,
    published_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE(job_name, day, output_sha256)
);
CREATE INDEX IF NOT EXISTS idx_products_node ON products(job_name, day);

-- 规划出的运行项，支撑领取、竞争与独立重试
CREATE TABLE IF NOT EXISTS run_items (
    run_id TEXT NOT NULL,
    job_name TEXT NOT NULL,
    day TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_id TEXT REFERENCES attempts(attempt_id),
    blocker_json TEXT,
    claimed_by TEXT,
    lease_until TEXT,
    attempts_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (run_id, job_name, day)
);
CREATE INDEX IF NOT EXISTS idx_run_items_claim ON run_items(status, run_id);

-- 自洽快照：整套 current 产物的固定版本视图
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id TEXT PRIMARY KEY,
    run_id TEXT,
    day_from TEXT,
    day_to TEXT,
    manifest_path TEXT NOT NULL,
    duckdb_path TEXT,
    entry_count INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    note TEXT
);

-- 快照内每个条目的精确证据（与 manifest 文件互为印证）
CREATE TABLE IF NOT EXISTS snapshot_entries (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    job_name TEXT NOT NULL,
    day TEXT NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    output_sha256 TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, job_name, day)
);
"""


def initialize(con) -> None:
    con.executescript(SCHEMA_SQL)
    row = con.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone()
    if row is None:
        con.execute(
            "INSERT INTO schema_meta(key,value) VALUES('schema_version',?)",
            (str(SCHEMA_VERSION),),
        )
    elif int(row["value"]) != SCHEMA_VERSION:
        raise RuntimeError(f"目录版本不兼容: {row['value']} != {SCHEMA_VERSION}")
