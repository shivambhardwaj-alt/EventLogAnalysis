from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent / "sql"
MIN_SQLITE = (3, 35, 0)

_BLOCK_RE = re.compile(r"^--\s*name:\s*(\w+)\s*$", re.MULTILINE)
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class DatabaseError(RuntimeError):
    pass


def connect(path: str | Path) -> sqlite3.Connection:
    if sqlite3.sqlite_version_info < MIN_SQLITE:
        raise DatabaseError(
            f"SQLite {sqlite3.sqlite_version} is too old; need >= {'.'.join(map(str, MIN_SQLITE))}. "
            "Upgrade Python (3.11+ from python.org bundles a recent SQLite)."
        )
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path), isolation_level=None)
    con.row_factory = sqlite3.Row
    con.create_function("sha256", 1, _sha256, deterministic=True)
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA busy_timeout = 5000")
    if str(path) != ":memory:":
        con.execute("PRAGMA journal_mode = WAL")
        con.execute("PRAGMA synchronous = NORMAL")
    try:
        con.execute("SELECT json_valid('{}')").fetchone()
    except sqlite3.OperationalError as exc:
        raise DatabaseError("This SQLite build has no JSON support") from exc
    return con


def _sha256(value: Any) -> str | None:
    return None if value is None else hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def user_hash(user_id: str) -> str:
    return _sha256(user_id.strip())


@contextmanager
def transaction(con: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    con.execute("BEGIN IMMEDIATE")
    try:
        yield con
    except BaseException:
        con.execute("ROLLBACK")
        raise
    else:
        con.execute("COMMIT")


def split_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buf = ""
    for line in sql_text.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            stmt = buf.strip()
            if _has_code(stmt):
                statements.append(stmt)
            buf = ""
    if _has_code(buf):
        raise ValueError(f"Incomplete SQL statement at end of script: {buf[:80]!r}")
    return statements


def _has_code(text: str) -> bool:
    return any(line.strip() and not line.strip().startswith("--") for line in text.splitlines())


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def sql_blocks(name: str) -> dict[str, str]:
    text = read_sql(name)
    parts = _BLOCK_RE.split(text)
    return {parts[i]: parts[i + 1].strip() for i in range(1, len(parts), 2)}


def run_script(
    con: sqlite3.Connection,
    sql_text: str,
    params: Mapping[str, Any] | None = None,
) -> int:
    changed = 0
    for stmt in split_statements(sql_text):
        cur = con.execute(stmt, _params_for(stmt, params or {}))
        if cur.rowcount and cur.rowcount > 0:
            changed += cur.rowcount
    return changed


def run_file(con: sqlite3.Connection, name: str, params: Mapping[str, Any] | None = None) -> int:
    log.debug("sql file=%s", name)
    return run_script(con, read_sql(name), params)


def scalar(con: sqlite3.Connection, sql: str, params: Mapping[str, Any] | tuple = ()) -> Any:
    row = con.execute(sql, params).fetchone()
    return None if row is None else row[0]


def safe_identifier(name: str, allowed: set[str]) -> str:
    if name not in allowed or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Identifier {name!r} is not allowed (allowed: {sorted(allowed)})")
    return name


def _params_for(stmt: str, params: Mapping[str, Any]) -> dict[str, Any]:
    names = set(re.findall(r":([A-Za-z_]\w*)", stmt))
    missing = names - set(params)
    missing = {m for m in missing if not re.search(rf"'[^']*:{m}[^']*'", stmt)}
    if missing:
        raise KeyError(f"SQL parameter(s) not provided: {sorted(missing)}")
    return {k: v for k, v in params.items() if k in names}


SCHEMA_VERSION = "2"

RAW_TABLES = {"ingest_files", "raw_events", "pipeline_runs", "pipeline_state", "erased_users"}


def apply_schema(con: sqlite3.Connection) -> bool:
    migrated = False
    has_state = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pipeline_state'").fetchone()
    if has_state:
        row = con.execute("SELECT value FROM pipeline_state WHERE key='schema_version'").fetchone()
        current = row[0] if row else "1"
        if current != SCHEMA_VERSION:
            log.warning("schema v%s -> v%s: dropping derived tables, raw data kept", current, SCHEMA_VERSION)
            tables = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
            con.execute("PRAGMA foreign_keys = OFF")
            for t in tables:
                if t not in RAW_TABLES:
                    con.execute(f'DROP TABLE IF EXISTS "{t}"')
            con.execute("PRAGMA foreign_keys = ON")
            migrated = True
    con.executescript(read_sql("schema.sql"))
    con.execute("INSERT OR REPLACE INTO pipeline_state (key, value) VALUES ('schema_version', ?)", (SCHEMA_VERSION,))
    return migrated