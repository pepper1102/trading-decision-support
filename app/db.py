from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "local.db"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"


def get_conn() -> sqlite3.Connection:
    """SQLite接続を返す（row_factory=sqlite3.Row）。"""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    """schema.sqlを読み込んでテーブルを初期化し、WALモードを有効化する。"""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(schema_sql)
        conn.commit()


def get_last_run() -> Optional[sqlite3.Row]:
    """最新のsuccessなbatch_runsレコードを返す。なければNone。"""
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM batch_runs
            WHERE status = 'success'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()


# ──────────────────────────────────────────────
# EDINET コードキャッシュ（DB永続化）
# ──────────────────────────────────────────────

def get_db_edinet_code(security_code: str, max_age_days: int = 30) -> str | None:
    """DBキャッシュからEDINETコードを返す。期限切れ/未登録はNone。"""
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT edinet_code, cached_at FROM edinet_code_cache WHERE security_code = ?",
            (security_code,),
        ).fetchone()
    if row:
        cached_at = datetime.fromisoformat(row[1])
        if datetime.now() - cached_at < timedelta(days=max_age_days):
            return row[0]
    return None


def save_edinet_code_cache(security_code: str, edinet_code: str) -> None:
    """EDINETコードをDBキャッシュに保存する（UPSERT）。"""
    now = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """INSERT INTO edinet_code_cache (security_code, edinet_code, cached_at)
               VALUES (?, ?, ?)
               ON CONFLICT(security_code) DO UPDATE SET
                 edinet_code=excluded.edinet_code, cached_at=excluded.cached_at""",
            (security_code, edinet_code, now),
        )
        conn.commit()


# ──────────────────────────────────────────────
# 財務諸表の鮮度チェック・DB読み込み
# ──────────────────────────────────────────────

def statements_need_refresh(code: str, max_age_days: int = 30) -> bool:
    """DBのstatements最終更新がmax_age_days日より古い（または未取得）かを返す。"""
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT MAX(updated_at) FROM statements WHERE code = ?", (code,)
        ).fetchone()
    if row and row[0]:
        last_updated = datetime.fromisoformat(row[0])
        return datetime.now() - last_updated >= timedelta(days=max_age_days)
    return True


def read_statements_from_db(code: str) -> list[dict[str, Any]]:
    """DBからstatements（raw_json）を読み込んでdictリストで返す。"""
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        rows = conn.execute(
            "SELECT raw_json FROM statements WHERE code = ? ORDER BY disclosed_date DESC",
            (code,),
        ).fetchall()
    return [json.loads(r[0]) for r in rows if r[0]]
