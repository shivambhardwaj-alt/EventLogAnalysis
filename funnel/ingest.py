from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .db import transaction

log = logging.getLogger(__name__)

FORMATS = {".jsonl": "jsonl", ".ndjson": "jsonl", ".csv": "csv"}
CSV_COLUMNS = ["event_id", "user_id", "event_name", "ts", "device", "country", "channel", "app_version", "product_id", "price"]
CSV_PROPERTY_COLUMNS = {"product_id", "price"}


@dataclass
class IngestResult:
    files_seen: int = 0
    files_ingested: int = 0
    rows_ingested: int = 0
    skipped_already_loaded: list[str] = field(default_factory=list)
    empty_files: list[str] = field(default_factory=list)
    unsupported_files: list[str] = field(default_factory=list)
    loaded_files: list[str] = field(default_factory=list)
    erased_rows_dropped: int = 0


def discover(incoming_dir: Path) -> list[Path]:
    if not incoming_dir.exists():
        return []
    return sorted(p for p in incoming_dir.iterdir() if p.is_file() and not p.name.startswith("."))


def parse_jsonl(text: str) -> tuple[list[tuple[int, str]], int]:
    rows, blanks = [], 0
    for line_no, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            blanks += 1
            continue
        rows.append((line_no, line.strip()))
    return rows, blanks


def parse_csv(text: str) -> tuple[list[tuple[int, str]], int]:
    rows, blanks = [], 0
    reader = csv.reader(io.StringIO(text))
    header: list[str] | None = None
    for record in reader:
        line_no = reader.line_num
        if not record or all(not c.strip() for c in record):
            blanks += 1
            continue
        if header is None:
            header = [h.strip().lower() for h in record]
            missing = [c for c in ("event_id", "user_id", "event_name", "ts") if c not in header]
            if missing:
                raise ValueError(f"CSV header missing required columns {missing}")
            continue
        if len(record) != len(header):
            payload = {"__parse_error__": "csv_column_count_mismatch", "_raw": record}
        else:
            values = {h: (v if v.strip() != "" else None) for h, v in zip(header, record)}
            payload = {k: v for k, v in values.items() if k not in CSV_PROPERTY_COLUMNS}
            payload["properties"] = {k: values.get(k) for k in CSV_PROPERTY_COLUMNS if k in values}
        rows.append((line_no, json.dumps(payload, ensure_ascii=False)))
    return rows, blanks


def ingest_new_files(con: sqlite3.Connection, incoming_dir: Path, run_id: str) -> IngestResult:
    result = IngestResult()
    for path in discover(incoming_dir):
        result.files_seen += 1
        fmt = FORMATS.get(path.suffix.lower())
        if fmt is None:
            result.unsupported_files.append(path.name)
            log.warning("ignoring unsupported file %s", path.name)
            continue

        data = path.read_bytes()
        file_hash = hashlib.sha256(data).hexdigest()
        known = con.execute("SELECT file_name FROM ingest_files WHERE file_hash = ?", (file_hash,)).fetchone()
        if known:
            result.skipped_already_loaded.append(path.name)
            log.info("skip %s (content already ingested as %s)", path.name, known["file_name"])
            continue

        text = data.decode("utf-8-sig", errors="replace")
        try:
            rows, blanks = parse_csv(text) if fmt == "csv" else parse_jsonl(text)
        except ValueError as exc:
            log.error("cannot parse %s: %s", path.name, exc)
            result.unsupported_files.append(f"{path.name} ({exc})")
            continue

        now = time.time()
        with transaction(con):
            con.execute(
                "INSERT INTO ingest_files (file_hash, file_name, file_format, line_count, blank_lines, run_id, ingested_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (file_hash, path.name, fmt, len(rows), blanks, run_id,
                 datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="seconds")),
            )
            con.executemany(
                "INSERT INTO raw_events (run_id, file_hash, line_no, raw_payload, ingested_at_epoch) VALUES (?, ?, ?, ?, ?)",
                [(run_id, file_hash, line_no, payload, now) for line_no, payload in rows],
            )
            erased = con.execute("""
                DELETE FROM raw_events
                WHERE file_hash = ?
                  AND sha256(TRIM(CAST(CASE WHEN json_valid(raw_payload)
                                            THEN json_extract(raw_payload, '$.user_id') END AS TEXT)))
                      IN (SELECT user_hash FROM erased_users)""", (file_hash,)).rowcount
            if erased:
                result.erased_rows_dropped += erased
                log.info("dropped %d rows of erased users from %s", erased, path.name)
        result.files_ingested += 1
        result.rows_ingested += len(rows)
        result.loaded_files.append(path.name)
        if not rows:
            result.empty_files.append(path.name)
            log.warning("file %s is empty - recorded so it is not re-read", path.name)
        else:
            log.info("ingested %s rows=%d blank_lines=%d format=%s", path.name, len(rows), blanks, fmt)
    return result