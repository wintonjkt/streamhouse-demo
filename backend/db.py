"""Thin Db2 access layer on top of ibm_db (one shared connection guarded by a lock)."""
import re
import threading
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence

import ibm_db

from .config import SQL_DIR, settings

_lock = threading.RLock()
_conn = None

# Drop order is the reverse of creation order because of foreign keys.
TABLES_IN_CREATE_ORDER = ["CUSTOMER", "ACCOUNT", "SCENARIO_RUN", "TXN_EVENT", "DIGITAL_EVENT", "AI_RECOMMENDATION"]
EVENT_TABLES_DELETE_ORDER = ["AI_RECOMMENDATION", "DIGITAL_EVENT", "TXN_EVENT", "SCENARIO_RUN"]


class Db2Error(RuntimeError):
    pass


def _connect():
    global _conn
    try:
        _conn = ibm_db.connect(settings.db2_dsn, "", "")
    except Exception as exc:
        _conn = None
        raise Db2Error("Db2 connection failed: %s" % (ibm_db.conn_errormsg() or exc))
    return _conn


def _get_conn():
    global _conn
    if _conn is None or not ibm_db.active(_conn):
        _connect()
    return _conn


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def ts(value: datetime) -> str:
    """Format a datetime as a Db2 timestamp literal usable as a parameter."""
    return value.strftime("%Y-%m-%d %H:%M:%S.%f")


def _run(sql: str, params: Sequence[Any] = ()):
    conn = _get_conn()
    try:
        stmt = ibm_db.prepare(conn, sql)
        ibm_db.execute(stmt, tuple(params))
        return stmt
    except Exception as exc:
        detail = ibm_db.stmt_errormsg() or str(exc)
        raise Db2Error(detail)


def execute(sql: str, params: Sequence[Any] = ()) -> int:
    with _lock:
        stmt = _run(sql, params)
        return ibm_db.num_rows(stmt)


def query(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    with _lock:
        stmt = _run(sql, params)
        rows = []
        row = ibm_db.fetch_assoc(stmt)
        while row:
            rows.append({k.lower(): to_jsonable(v) for k, v in row.items()})
            row = ibm_db.fetch_assoc(stmt)
        return rows


def query_one(sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
    rows = query(sql, params)
    return rows[0] if rows else None


def _statements(path) -> List[str]:
    text = path.read_text(encoding="utf-8")
    text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def existing_tables() -> List[str]:
    rows = query("SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT SCHEMA AND TYPE = 'T'")
    return sorted(r["tabname"] for r in rows)


def create_schema(drop_existing: bool = False) -> Dict[str, Any]:
    with _lock:
        present = set(existing_tables())
        dropped = []
        if drop_existing:
            for table in reversed(TABLES_IN_CREATE_ORDER):
                if table in present:
                    execute("DROP TABLE %s" % table)
                    dropped.append(table)
            present = set(existing_tables())
        created = []
        for stmt in _statements(SQL_DIR / "schema.sql"):
            match = re.match(r"CREATE TABLE\s+(\w+)", stmt, re.I)
            name = match.group(1).upper() if match else None
            if name in present:
                continue
            execute(stmt)
            created.append(name)
        return {"dropped": dropped, "created": created, "tables": existing_tables()}


def seed() -> Dict[str, Any]:
    """Reset to the initial state: remove all event data and reload the seed rows."""
    with _lock:
        for table in EVENT_TABLES_DELETE_ORDER + ["ACCOUNT", "CUSTOMER"]:
            execute("DELETE FROM %s" % table)
        for stmt in _statements(SQL_DIR / "seed.sql"):
            execute(stmt)
        return {
            "customers": query_one("SELECT COUNT(*) AS n FROM CUSTOMER")["n"],
            "accounts": query_one("SELECT COUNT(*) AS n FROM ACCOUNT")["n"],
        }


def clear_events() -> Dict[str, int]:
    """Remove generated events and runs but keep customer and account reference data."""
    with _lock:
        return {t.lower(): execute("DELETE FROM %s" % t) for t in EVENT_TABLES_DELETE_ORDER}


def ping() -> Dict[str, Any]:
    row = query_one("SELECT CURRENT SERVER AS db, CURRENT SCHEMA AS schema_name FROM SYSIBM.SYSDUMMY1")
    return {"database": row["db"], "schema": row["schema_name"]}
