from __future__ import annotations

import json
from typing import Any

from .db import get_conn


def _parse_rules_json(value: str | None) -> list[dict]:
    if not value:
        return []
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return []


def get_summary(batch_run_id: int) -> dict[str, dict[str, int]]:
    """strategy × signal の件数を集計して返す。"""
    strategies = ("swing", "fundamental", "dividend")
    signals = ("buy", "sell", "hold")
    # 先に0埋めしておく（キー欠損によるテンプレート崩れを防ぐ）
    result: dict[str, dict[str, int]] = {
        st: {sg: 0 for sg in signals} for st in strategies
    }
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT strategy, signal, COUNT(*) AS cnt
            FROM judgments
            WHERE batch_run_id = ?
            GROUP BY strategy, signal
            """,
            (batch_run_id,),
        ).fetchall()
    for row in rows:
        st = row["strategy"]
        sg = row["signal"]
        if st in result and sg in result[st]:
            result[st][sg] = int(row["cnt"])
    return result


def get_candidates(
    batch_run_id: int,
    strategy: str,
    signal: str,
    price_min: float | None = None,
    price_max: float | None = None,
) -> list[dict[str, Any]]:
    """条件に合う銘柄をスコア降順で返す。"""
    sql = """
        SELECT j.code, s.name, j.price, j.signal, j.score, j.as_of, j.top_reason
        FROM judgments j
        LEFT JOIN stocks s ON s.code = j.code
        WHERE j.batch_run_id = ? AND j.strategy = ? AND j.signal = ?
    """
    params: list[Any] = [batch_run_id, strategy, signal]

    if price_min is not None:
        sql += " AND j.price >= ?"
        params.append(price_min)
    if price_max is not None:
        sql += " AND j.price <= ?"
        params.append(price_max)

    sql += " ORDER BY j.score DESC"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [
        {
            "code": row["code"],
            "name": row["name"] or row["code"],
            "price": row["price"],
            "signal": row["signal"],
            "score": row["score"],
            "as_of": row["as_of"],
            "top_reason": row["top_reason"],
        }
        for row in rows
    ]


def get_stock_judgments(batch_run_id: int, code: str) -> dict[str, dict[str, Any]]:
    """銘柄の全戦略の判定結果を返す（rules_jsonをパース済み）。"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT strategy, signal, score, price, as_of, top_reason, rules_json
            FROM judgments
            WHERE batch_run_id = ? AND code = ?
            ORDER BY CASE strategy
                WHEN 'swing'       THEN 1
                WHEN 'fundamental' THEN 2
                WHEN 'dividend'    THEN 3
                ELSE 4
            END
            """,
            (batch_run_id, code),
        ).fetchall()

    return {
        row["strategy"]: {
            "strategy": row["strategy"],
            "signal": row["signal"],
            "score": row["score"],
            "price": row["price"],
            "as_of": row["as_of"],
            "top_reason": row["top_reason"],
            "rule_results": _parse_rules_json(row["rules_json"]),
        }
        for row in rows
    }


def get_daily_quotes(code: str, limit: int = 30) -> list[dict[str, Any]]:
    """日付昇順でlimit件の日足データを返す（チャート用）。最新limit件を取得して古い順に並べ直す。"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume, turnover_value
            FROM (
                SELECT date, open, high, low, close, volume, turnover_value
                FROM daily_quotes
                WHERE code = ?
                ORDER BY date DESC
                LIMIT ?
            )
            ORDER BY date ASC
            """,
            (code, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _sentiment_tone(score: float) -> str:
    if score >= 0.15:
        return "positive"
    if score <= -0.15:
        return "negative"
    return "neutral"


def get_recent_news(code: str, limit: int = 10, days: int = 30) -> list[dict[str, Any]]:
    """直近days日・最新limit件のニュースをセンチメント情報付きで返す。"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT published_at, title, url, summary, sentiment_score, source
            FROM news
            WHERE code = ?
              AND datetime(published_at) >= datetime('now', ?)
            ORDER BY datetime(published_at) DESC
            LIMIT ?
            """,
            (code, f"-{int(days)} days", int(limit)),
        ).fetchall()

    out: list[dict[str, Any]] = []
    for row in rows:
        score = float(row["sentiment_score"] or 0.0)
        out.append({
            "published_at": row["published_at"],
            "title": row["title"],
            "url": row["url"],
            "summary": row["summary"] or "",
            "sentiment_score": score,
            "sentiment_tone": _sentiment_tone(score),
            "source": row["source"],
        })
    return out


def get_stock_info(code: str) -> dict[str, Any] | None:
    """stocksテーブルから銘柄情報を返す。"""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT code, name, market, updated_at FROM stocks WHERE code = ? LIMIT 1",
            (code,),
        ).fetchone()
    return dict(row) if row else None
