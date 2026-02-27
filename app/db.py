from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from .config import settings

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"
DB_PATH = Path(settings.db_path).expanduser().resolve()


def get_conn() -> sqlite3.Connection:
    """SQLite接続を返す（row_factory=sqlite3.Row）。"""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _migrate_db(conn: sqlite3.Connection) -> None:
    """既存DBへの後方互換カラム追加（エラー無視）。
    schema.sql は CREATE TABLE IF NOT EXISTS 形式のため、
    既存テーブルへのカラム追加はここで行う。
    """
    migrations = [
        # Fix4: source/source_version/ingested_at
        "ALTER TABLE daily_quotes ADD COLUMN source TEXT",
        "ALTER TABLE daily_quotes ADD COLUMN source_version TEXT",
        "ALTER TABLE daily_quotes ADD COLUMN ingested_at TEXT",
        "ALTER TABLE statements ADD COLUMN source TEXT",
        "ALTER TABLE statements ADD COLUMN source_version TEXT",
        "ALTER TABLE statements ADD COLUMN ingested_at TEXT",
        "ALTER TABLE dividends ADD COLUMN source TEXT",
        "ALTER TABLE dividends ADD COLUMN source_version TEXT",
        "ALTER TABLE dividends ADD COLUMN ingested_at TEXT",
        "ALTER TABLE announcements ADD COLUMN source TEXT",
        "ALTER TABLE announcements ADD COLUMN source_version TEXT",
        "ALTER TABLE announcements ADD COLUMN ingested_at TEXT",
        # Fix5: sentiment metadata
        "ALTER TABLE news ADD COLUMN sentiment_method TEXT DEFAULT 'rule'",
        "ALTER TABLE news ADD COLUMN sentiment_model TEXT",
        "ALTER TABLE news ADD COLUMN sentiment_confidence REAL",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # カラムが既に存在する場合は無視
    conn.commit()


def init_db() -> None:
    """schema.sqlを読み込んでテーブルを初期化し、WALモードを有効化する。"""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(schema_sql)
        _migrate_db(conn)
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
    """DBからstatementsを読み込んでdictリストで返す（明示列優先、raw_jsonフォールバック）。"""
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        rows = conn.execute(
            """SELECT disclosed_date, net_sales, operating_profit, equity,
                      total_assets, net_income, eps, raw_json
               FROM statements WHERE code = ? ORDER BY disclosed_date DESC""",
            (code,),
        ).fetchall()
    result: list[dict[str, Any]] = []
    for r in rows:
        row_dict: dict[str, Any] = {
            "DisclosedDate": r[0],
            "NetSales": r[1],
            "OperatingProfit": r[2],
            "Equity": r[3],
            "TotalAssets": r[4],
            "NetIncome": r[5],
            "EarningsPerShare": r[6],
        }
        # 明示列が全てNoneの場合はraw_jsonから復元を試みる
        if all(v is None for k, v in row_dict.items() if k != "DisclosedDate") and r[7]:
            try:
                row_dict = json.loads(r[7])
            except json.JSONDecodeError:
                pass
        result.append(row_dict)
    return result


# ──────────────────────────────────────────────
# Watermark（銘柄・フィード別の最終取得済み公開日時）
# ──────────────────────────────────────────────

def get_watermark(code: str, feed: str) -> str | None:
    """銘柄・フィードの最終取得済み公開日時を返す（なければNone）。"""
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(
            "SELECT last_published_at FROM ingest_watermarks WHERE code = ? AND feed = ?",
            (code, feed),
        ).fetchone()
    return row[0] if row else None


def upsert_watermark(code: str, feed: str, last_published_at: str) -> None:
    """銘柄・フィードのwatermarkをUPSERTする。"""
    now = datetime.now().isoformat(timespec="seconds")
    with sqlite3.connect(str(DB_PATH)) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """INSERT INTO ingest_watermarks (code, feed, last_published_at, last_ingested_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(code, feed) DO UPDATE SET
                 last_published_at=excluded.last_published_at,
                 last_ingested_at=excluded.last_ingested_at""",
            (code, feed, last_published_at, now),
        )
        conn.commit()
