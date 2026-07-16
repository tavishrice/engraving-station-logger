"""Shared Postgres helper. One place to get a connection from DATABASE_URL.

Uses psycopg 3 (psycopg[binary]). On Render, set DATABASE_URL to the contribution
database's connection string. If the logger and the DB are NOT in the same Render
region, use the EXTERNAL connection string (the internal *.render.com host only
resolves inside the same region's private network) — otherwise a connect() will
stall until it times out.

IMPORTANT: connect_timeout is set so a momentary DB/network problem FAILS FAST
(<= connect_timeout seconds) instead of hanging. The engraving logger only ever
touches the DB from a background thread (see PgSink), never in the request path,
so a slow/failed connect can never take the scan station down.
"""
import os
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get("DATABASE_URL", "")

# seconds to wait for a TCP/SSL connect before giving up (fail fast, never hang)
CONNECT_TIMEOUT = int(os.environ.get("PG_CONNECT_TIMEOUT", "8"))


def connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    # autocommit off by default; callers commit. row_factory gives dict rows.
    # keepalives so a long-idle background connection notices a dropped socket.
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row,
        connect_timeout=CONNECT_TIMEOUT,
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
    )


def execmany_ignore_conflict(cur, sql, rows):
    """Insert many rows; duplicates (ON CONFLICT DO NOTHING) are silently skipped."""
    if not rows:
        return 0
    cur.executemany(sql, rows)
    return cur.rowcount
