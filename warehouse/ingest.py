"""原件登记：把真实 Parquet 分区组织成不可变、带版本的仓库分区。

- register：读取一个 parquet 文件（或按 day 列拆分），按内容哈希落 raw/，登记修订；
- clear：明确清空某分区，写零行 parquet 证据并登记 state=cleared；
  零行产物与缺分区分别对待——clear 后是“明确为空”，未登记才是 missing。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import Catalog
from .config import Config


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _write_partition_file(config: Config, table: str, day: str, revision_no: int,
                          table_obj: pa.Table, origin: str) -> Path:
    target_dir = config.partition_dir(table, day, revision_no)
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "data.parquet"
    # 先写临时文件再移动，保证读者不会看到半截文件
    tmp = target_dir / ".data.parquet.tmp"
    pq.write_table(table_obj, tmp, compression="zstd")
    tmp.replace(target)
    return target


def register_parquet(
    config: Config,
    catalog: Catalog,
    table: str,
    parquet_path: str | Path,
    origin: str = "register",
    note: str | None = None,
) -> list[dict]:
    """登记一个 parquet 文件。含 day 列时按天拆成多个日分区，分别修订。"""
    parquet_path = Path(parquet_path).resolve()
    if not parquet_path.exists():
        raise FileNotFoundError(f"原件不存在: {parquet_path}")
    # 原件可来自仓库外（如项目 inputs/）；读取后只把不可变副本写入仓库 raw/
    table_obj = pq.read_table(parquet_path)
    if "day" not in table_obj.column_names:
        raise ValueError(f"{table} 的 parquet 缺少 day 列，无法按业务日期分区")

    results: list[dict] = []
    days = sorted(set(table_obj.column("day").to_pylist()))
    for day in days:
        part = table_obj.filter(pa.compute.field("day") == day)
        current = catalog.source_state(table, day)
        revision_no = (current["revision_no"] + 1) if current else 1
        target_dir = config.partition_dir(table, day, revision_no)
        target_dir.mkdir(parents=True, exist_ok=True)
        tmp = target_dir / ".probe.parquet"
        pq.write_table(part, tmp, compression="zstd")
        probe_bytes = tmp.read_bytes()
        probe_sha = _sha256_bytes(probe_bytes)
        if (
            current is not None
            and current["file_sha256"] == probe_sha
            and current["state"] == "present"
        ):
            tmp.unlink()
            results.append({"table": table, "day": day, "changed": False,
                            "revision_no": current["revision_no"],
                            "rows": part.num_rows})
            continue
        target = target_dir / "data.parquet"
        tmp.replace(target)
        rec = catalog.register_revision(
            table=table,
            day=day,
            state="present",
            path=str(target),
            file_sha256=probe_sha,
            row_count=part.num_rows,
            origin=origin,
            note=note,
        )
        results.append({"table": table, "day": day, "changed": rec["changed"],
                        "revision_no": rec["revision"]["revision_no"],
                        "rows": part.num_rows,
                        "revision_id": rec["revision"]["revision_id"]})
    return results


def register_rows(
    config: Config,
    catalog: Catalog,
    table: str,
    day: str,
    rows: list[dict],
    origin: str = "register",
    note: str | None = None,
    empty_schema_hint: pa.Table | None = None,
) -> dict:
    """登记一组行（actions.json 的 replace_partition 入口）。rows=[] 即明确清空。"""
    if rows:
        table_obj = pa.Table.from_pylist(rows)
    elif empty_schema_hint is not None:
        table_obj = empty_schema_hint.schema.empty_table()
    else:
        raise ValueError("零行登记需要 schema 提示，无法凭空推断列类型")

    current = catalog.source_state(table, day)
    revision_no = (current["revision_no"] + 1) if current else 1
    tmp_dir = config.partition_dir(table, day, revision_no)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / ".probe.parquet"
    pq.write_table(table_obj, tmp, compression="zstd")
    probe_bytes = tmp.read_bytes()
    probe_sha = _sha256_bytes(probe_bytes)
    state = "present" if rows else "cleared"

    if (
        current is not None
        and current["file_sha256"] == probe_sha
        and current["state"] == state
    ):
        tmp.unlink()
        return {"table": table, "day": day, "changed": False,
                "revision_no": current["revision_no"], "rows": 0, "state": state}

    target_dir = config.partition_dir(table, day, revision_no)
    target = target_dir / "data.parquet"
    tmp.replace(target)
    rec = catalog.register_revision(
        table=table,
        day=day,
        state=state,
        path=str(target),
        file_sha256=probe_sha,
        row_count=table_obj.num_rows,
        origin=origin,
        note=note,
    )
    return {"table": table, "day": day, "changed": rec["changed"],
            "revision_no": rec["revision"]["revision_no"],
            "rows": table_obj.num_rows, "state": state,
            "revision_id": rec["revision"]["revision_id"]}


def clear_partition(
    config: Config,
    catalog: Catalog,
    table: str,
    day: str,
    schema_hint: pa.Table,
    origin: str = "clear",
    note: str | None = None,
) -> dict:
    """明确清空：写零行 parquet 证据，登记 state=cleared（区别于 missing）。"""
    return register_rows(
        config, catalog, table, day, [], origin=origin, note=note,
        empty_schema_hint=schema_hint,
    )


def apply_actions(config: Config, catalog: Catalog, actions_path: str | Path,
                  schema_hint_table: pa.Table) -> list[dict]:
    """按顺序应用 actions.json；每个 replace_partition 都是一次独立修订。"""
    actions = json.loads(Path(actions_path).read_text(encoding="utf-8"))
    out: list[dict] = []
    for action in actions:
        if action["op"] != "replace_partition":
            raise ValueError(f"不支持的动作: {action['op']}")
        rec = register_rows(
            config,
            catalog,
            table=action["table"],
            day=action["day"],
            rows=action["rows"],
            origin=f"actions.json#step{action['step']}",
            note=json.dumps(action, ensure_ascii=False),
            empty_schema_hint=schema_hint_table,
        )
        rec["step"] = action["step"]
        out.append(rec)
    return out
