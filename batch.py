from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from queue import Queue
from threading import Lock, Thread
from typing import Any

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

from app.clients.edinet import EdinetDbClient, to_statements
from app.news import fetch_company_news
from app.config import load_watchlist, settings
from app.db import (
    DB_PATH,
    get_conn,
    get_db_edinet_code,
    get_watermark,
    init_db,
    read_statements_from_db,
    save_edinet_code_cache,
    statements_need_refresh,
    upsert_watermark,
)
from app.rules.engine import RulesOrchestrator

# 並列ワーカー数（yfinance のレート制限を考慮して控えめに設定）
MAX_WORKERS = 5
# EDINET DB財務データの再取得間隔（日）
EDINET_CACHE_DAYS = 30


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        s = value.strip().replace("/", "-")
        return s[:10] if s else None
    return None


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_list(payload: dict[str, Any], preferred_key: str) -> list[dict[str, Any]]:
    v = payload.get(preferred_key)
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]
    for candidate in payload.values():
        if isinstance(candidate, list):
            return [x for x in candidate if isinstance(x, dict)]
    return []


# ──────────────────────────────────────────────
# yfinance 同期クライアント
# ──────────────────────────────────────────────

class YFinanceSyncClient:
    """yfinance同期クライアント（J-Quants互換の戻り値形式に整形）"""

    def __init__(self, history_period: str = "6mo") -> None:
        self._history_period = history_period
        self._ticker_cache: dict[str, yf.Ticker] = {}

    def close(self) -> None:
        return

    def _to_symbol(self, code: str) -> str:
        c = str(code).strip()
        return c if c.upper().endswith(".T") else f"{c}.T"

    def _ticker(self, code: str) -> yf.Ticker:
        symbol = self._to_symbol(code)
        if symbol not in self._ticker_cache:
            self._ticker_cache[symbol] = yf.Ticker(symbol)
        return self._ticker_cache[symbol]

    def _num(self, v: Any) -> float | None:
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
            return float(v)
        except Exception:
            return None

    def _pick(self, df: pd.DataFrame, rows: list[str], col: Any) -> float | None:
        if df is None or df.empty:
            return None
        for r in rows:
            if r in df.index and col in df.columns:
                return self._num(df.at[r, col])
        return None

    def get_listed_info(self, code: str) -> list[dict[str, Any]]:
        t = self._ticker(code)
        info = t.info or {}
        return [{
            "CompanyName": info.get("longName") or info.get("shortName") or str(code),
            "CompanyNameEnglish": info.get("longName") or info.get("shortName") or str(code),
            "MarketCodeName": info.get("exchange") or "TSE",
            "Code": self._to_symbol(code),
        }]

    def get_daily_quotes(self, code: str, date_from: str, date_to: str) -> list[dict[str, Any]]:
        t = self._ticker(code)
        hist = t.history(period=self._history_period, auto_adjust=False)
        if hist is None or hist.empty:
            return []

        start = pd.Timestamp(date_from).date()
        end = pd.Timestamp(date_to).date()

        out: list[dict[str, Any]] = []
        for idx, row in hist.iterrows():
            d = pd.Timestamp(idx).date()
            if d < start or d > end:
                continue

            close = self._num(row.get("Close"))
            volume = self._num(row.get("Volume"))
            turnover = (close * volume) if (close is not None and volume is not None) else None

            out.append({
                "Date": d.isoformat(),
                "Open": self._num(row.get("Open")),
                "High": self._num(row.get("High")),
                "Low": self._num(row.get("Low")),
                "Close": close,
                "Volume": volume,
                "TurnoverValue": turnover,
            })
        return out

    def get_statements(self, code: str) -> list[dict[str, Any]]:
        t = self._ticker(code)
        fin = t.financials
        bs = t.balance_sheet

        cols: list[Any] = []
        if fin is not None and not fin.empty:
            cols.extend(list(fin.columns))
        if bs is not None and not bs.empty:
            cols.extend(list(bs.columns))

        uniq_cols = sorted({pd.Timestamp(c) for c in cols}, reverse=True)
        out: list[dict[str, Any]] = []

        for c in uniq_cols:
            d = pd.Timestamp(c).date().isoformat()
            out.append({
                "DisclosedDate": d,
                "NetSales": self._pick(fin, ["Total Revenue", "Operating Revenue", "Revenue"], c),
                "OperatingProfit": self._pick(fin, ["Operating Income", "Operating Income Loss"], c),
                "Equity": self._pick(bs, ["Stockholders Equity", "Total Stockholder Equity", "Common Stock Equity"], c),
                "TotalAssets": self._pick(bs, ["Total Assets"], c),
                "NetIncome": self._pick(fin, ["Net Income", "Net Income Common Stockholders"], c),
                "EarningsPerShare": self._pick(fin, ["Diluted EPS", "Basic EPS"], c),
            })
        return out

    def get_dividend(self, code: str) -> list[dict[str, Any]]:
        t = self._ticker(code)
        div = t.dividends
        if div is None or len(div) == 0:
            return []

        out: list[dict[str, Any]] = []
        for idx, v in div.items():
            out.append({
                "RecordDate": pd.Timestamp(idx).date().isoformat(),
                "DividendPerShare": self._num(v),
            })
        return out

    def get_announcements(self) -> list[dict[str, Any]]:
        return []


# ──────────────────────────────────────────────
# DB書き込みヘルパー
# ──────────────────────────────────────────────

def upsert_batch_run_start(target_count: int) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO batch_runs (started_at, status, target_count, success_count, error_count, message) VALUES (?, 'running', ?, 0, 0, ?)",
            (now_iso(), target_count, "batch started"),
        )
        conn.commit()
        return int(cur.lastrowid)


def upsert_batch_run_finish(
    batch_run_id: int, status: str, success_count: int, error_count: int, message: str
) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE batch_runs SET finished_at=?, status=?, success_count=?, error_count=?, message=? WHERE id=?",
            (now_iso(), status, success_count, error_count, message, batch_run_id),
        )
        conn.commit()


def upsert_stock(conn: Any, code: str, info: dict[str, Any]) -> None:
    name = str(info.get("CompanyName") or info.get("CompanyNameEnglish") or code)
    market = str(info.get("MarketCodeName") or info.get("MarketCode") or "")
    conn.execute(
        """INSERT INTO stocks (code, name, market, updated_at) VALUES (?, ?, ?, ?)
           ON CONFLICT(code) DO UPDATE SET name=excluded.name, market=excluded.market, updated_at=excluded.updated_at""",
        (code, name, market, now_iso()),
    )


def upsert_daily_quotes(
    conn: Any, code: str, quotes: list[dict[str, Any]], source: str = "yfinance"
) -> None:
    ts = now_iso()
    for q in quotes:
        d = normalize_date(q.get("Date"))
        if not d:
            continue
        conn.execute(
            """INSERT INTO daily_quotes
                 (code, date, open, high, low, close, volume, turnover_value,
                  raw_json, updated_at, source, source_version, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(code, date) DO UPDATE SET
                 open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close,
                 volume=excluded.volume, turnover_value=excluded.turnover_value,
                 raw_json=excluded.raw_json, updated_at=excluded.updated_at,
                 source=excluded.source, source_version=excluded.source_version,
                 ingested_at=excluded.ingested_at""",
            (code, d, to_float(q.get("Open")), to_float(q.get("High")), to_float(q.get("Low")),
             to_float(q.get("Close")), to_float(q.get("Volume")), to_float(q.get("TurnoverValue")),
             json.dumps(q, ensure_ascii=False), ts, source, "v1", ts),
        )


def upsert_statements(
    conn: Any, code: str, statements: list[dict[str, Any]], source: str = "yfinance"
) -> None:
    ts = now_iso()
    for st in statements:
        disclosed_date = normalize_date(st.get("DisclosedDate"))
        if not disclosed_date:
            continue
        net_sales = to_float(st.get("NetSales") or st.get("NetSalesAmount") or st.get("Revenue"))
        conn.execute(
            """INSERT INTO statements
                 (code, disclosed_date, net_sales, operating_profit, equity, total_assets,
                  net_income, eps, raw_json, updated_at, source, source_version, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(code, disclosed_date) DO UPDATE SET
                 net_sales=excluded.net_sales, operating_profit=excluded.operating_profit,
                 equity=excluded.equity, total_assets=excluded.total_assets,
                 net_income=excluded.net_income, eps=excluded.eps,
                 raw_json=excluded.raw_json, updated_at=excluded.updated_at,
                 source=excluded.source, source_version=excluded.source_version,
                 ingested_at=excluded.ingested_at""",
            (code, disclosed_date, net_sales, to_float(st.get("OperatingProfit")),
             to_float(st.get("Equity")), to_float(st.get("TotalAssets")),
             to_float(st.get("NetIncome")),
             to_float(st.get("EarningsPerShare") or st.get("BasicEarningsPerShare") or st.get("EPS")),
             json.dumps(st, ensure_ascii=False), ts, source, "v1", ts),
        )


def upsert_dividends(
    conn: Any, code: str, dividends: list[dict[str, Any]], source: str = "yfinance"
) -> None:
    ts = now_iso()
    for d in dividends:
        record_date = normalize_date(d.get("RecordDate") or d.get("Date"))
        if not record_date:
            continue
        amount = to_float(
            d.get("DividendPerShare") or d.get("ForecastDividendPerShare")
            or d.get("AnnualDividendPerShare") or d.get("Dividend")
        )
        conn.execute(
            """INSERT INTO dividends
                 (code, record_date, dividend_per_share, raw_json, updated_at,
                  source, source_version, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(code, record_date) DO UPDATE SET
                 dividend_per_share=excluded.dividend_per_share,
                 raw_json=excluded.raw_json, updated_at=excluded.updated_at,
                 source=excluded.source, source_version=excluded.source_version,
                 ingested_at=excluded.ingested_at""",
            (code, record_date, amount, json.dumps(d, ensure_ascii=False), ts, source, "v1", ts),
        )


def upsert_announcements(
    conn: Any, announcements: list[dict[str, Any]], source: str = "yfinance"
) -> int:
    count = 0
    ts = now_iso()
    for a in announcements:
        code = str(a.get("Code") or a.get("LocalCode") or "").strip()
        d = normalize_date(a.get("Date") or a.get("AnnouncementDate") or a.get("DisclosedDate"))
        if not code or not d:
            continue
        conn.execute(
            """INSERT INTO announcements
                 (code, date, raw_json, updated_at, source, source_version, ingested_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(code, date) DO UPDATE SET
                 raw_json=excluded.raw_json, updated_at=excluded.updated_at,
                 source=excluded.source, source_version=excluded.source_version,
                 ingested_at=excluded.ingested_at""",
            (code, d, json.dumps(a, ensure_ascii=False), ts, source, "v1", ts),
        )
        count += 1
    return count


def upsert_news(conn: Any, code: str, news_rows: list[dict[str, Any]]) -> str | None:
    """ニュースをDBにUPSERT。挿入したニュースの最大 published_at を返す（watermark用）。"""
    max_pub: str | None = None
    for n in news_rows:
        published_at = str(n.get("published_at") or "").strip()
        title = str(n.get("title") or "").strip()
        url = str(n.get("url") or "").strip()
        summary = str(n.get("summary") or "").strip()
        source = str(n.get("source") or "unknown").strip()
        score = to_float(n.get("sentiment_score")) or 0.0
        method = str(n.get("sentiment_method") or "rule").strip()
        model = n.get("sentiment_model")
        confidence = to_float(n.get("sentiment_confidence"))
        if not (published_at and title and url):
            continue
        conn.execute(
            """INSERT INTO news
                 (code, published_at, title, url, summary, sentiment_score, source,
                  sentiment_method, sentiment_model, sentiment_confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(code, url) DO UPDATE SET
                 published_at=excluded.published_at,
                 title=excluded.title,
                 summary=excluded.summary,
                 sentiment_score=excluded.sentiment_score,
                 source=excluded.source,
                 sentiment_method=excluded.sentiment_method,
                 sentiment_model=excluded.sentiment_model,
                 sentiment_confidence=excluded.sentiment_confidence""",
            (code, published_at, title, url, summary, score, source, method, model, confidence),
        )
        if max_pub is None or published_at > max_pub:
            max_pub = published_at
    return max_pub


def upsert_judgments(conn: Any, batch_run_id: int, code: str, judgments: dict[str, Any]) -> None:
    for strategy, j in judgments.items():
        rules = [asdict(r) for r in j.rule_results]
        rules_json = json.dumps(rules, ensure_ascii=False)
        top_reason = j.rule_results[0].reason if j.rule_results else ""
        conn.execute(
            """INSERT INTO judgments (batch_run_id, code, strategy, signal, score, price, as_of, top_reason, rules_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(batch_run_id, code, strategy) DO UPDATE SET
                 signal=excluded.signal, score=excluded.score, price=excluded.price,
                 as_of=excluded.as_of, top_reason=excluded.top_reason, rules_json=excluded.rules_json""",
            (batch_run_id, code, strategy, j.signal, float(j.score),
             to_float(j.price), str(j.as_of), top_reason, rules_json),
        )


# ──────────────────────────────────────────────
# 並列処理用クラス・関数
# ──────────────────────────────────────────────

@dataclass
class StockPayload:
    """ワーカーがAPIから取得したデータをライタースレッドに渡すためのDTO。"""
    code: str
    listed_info: dict[str, Any]
    quotes: list[dict[str, Any]]
    statements: list[dict[str, Any]]
    dividends: list[dict[str, Any]]
    judgments: dict[str, Any]
    news: list[dict[str, Any]] = field(default_factory=list)
    edinet_fetched: bool = False  # TrueのときのみDB側のupdated_atを更新する


class DailyRateLimiter:
    """EDINET DB APIの1日あたりリクエスト上限を管理する。"""

    def __init__(self, daily_limit: int) -> None:
        self._limit = daily_limit
        self._used = 0
        self._lock = Lock()

    def try_consume(self, n: int = 1) -> bool:
        with self._lock:
            if self._used + n > self._limit:
                return False
            self._used += n
            return True

    @property
    def used(self) -> int:
        with self._lock:
            return self._used


def fetch_stock(
    code: str,
    announcements: list[dict[str, Any]],
    edinet_api_key: str,
    edinet_limiter: DailyRateLimiter,
    newsapi_key: str = "",
) -> StockPayload:
    """ワーカースレッド: 1銘柄のデータをAPIから取得してルール評価を行う。DB書き込みはしない。"""
    yf_client = YFinanceSyncClient(history_period="6mo")
    orchestrator = RulesOrchestrator()

    date_to = date.today()
    date_from = date_to - timedelta(days=120)

    listed_infos = yf_client.get_listed_info(code)
    listed_info = listed_infos[0] if listed_infos else {}
    name = str(listed_info.get("CompanyName") or listed_info.get("CompanyNameEnglish") or code)

    quotes = yf_client.get_daily_quotes(code, date_from.isoformat(), date_to.isoformat())
    dividends = yf_client.get_dividend(code)

    # ── 財務諸表: EDINET DB優先（30日キャッシュ）→ yfinanceフォールバック ──
    statements: list[dict[str, Any]] = []
    edinet_fetched = False

    if edinet_api_key:
        if not statements_need_refresh(code, max_age_days=EDINET_CACHE_DAYS):
            # DBキャッシュが新鮮 → APIコールなしで読む
            statements = read_statements_from_db(code)
            if statements:
                logging.info("  %s: statements from DB cache (%d periods)", code, len(statements))
        elif edinet_limiter.try_consume(1):
            # キャッシュ期限切れ or 未取得 → EDINET APIを叩く
            try:
                edinet_client = EdinetDbClient(edinet_api_key)
                # DBに保存済みのEDINETコードをメモリキャッシュに注入（API呼び出し削減）
                cached_edinet_code = get_db_edinet_code(code)
                if cached_edinet_code:
                    clean = str(code).replace(".T", "").strip()
                    edinet_client._edinet_code_cache[clean] = cached_edinet_code
                try:
                    financials = edinet_client.get_financials(code)
                    statements = to_statements(financials)
                    if statements:
                        edinet_fetched = True
                        logging.info("  %s: EdinetDB statements fetched (%d periods)", code, len(statements))
                    # 新たに解決したEDINETコードをDBキャッシュに保存
                    for sec, edc in edinet_client._edinet_code_cache.items():
                        if not get_db_edinet_code(sec):
                            save_edinet_code_cache(sec, edc)
                finally:
                    edinet_client.close()
            except Exception:
                logging.exception("  %s: EdinetDB failed, falling back to yfinance", code)
        else:
            logging.warning("  %s: EDINET rate limit reached, falling back to yfinance", code)

    if not statements:
        statements = yf_client.get_statements(code)

    # ── ニュース取得（Google News RSS / Yahoo Finance RSS / NewsAPI） ──
    news: list[dict[str, Any]] = []
    try:
        news_since = get_watermark(code, "news")
        news = fetch_company_news(
            code=code,
            company_name=name,
            newsapi_key=newsapi_key,
            since=news_since,
            lookback_days=30,
            limit=10,
        )
        if news:
            logging.info("  %s: %d news items fetched (since=%s)", code, len(news), news_since)
    except Exception:
        logging.exception("  %s: news fetch failed (continuing)", code)

    judgments = asyncio.run(
        orchestrator.evaluate_all(
            code=code,
            name=name,
            quotes=quotes,
            statements=statements,
            dividend_data=dividends,
            announcements=announcements,
        )
    )

    return StockPayload(
        code=code,
        listed_info=listed_info,
        quotes=quotes,
        statements=statements,
        dividends=dividends,
        judgments=judgments,
        news=news,
        edinet_fetched=edinet_fetched,
    )


def writer_loop(q: Queue, batch_run_id: int) -> None:
    """単一ライタースレッド: キューからStockPayloadを受け取ってSQLiteに書き込む。"""
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(str(DB_PATH))
    conn.row_factory = _sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        while True:
            item: StockPayload | None = q.get()
            if item is None:
                break
            try:
                upsert_stock(conn, item.code, item.listed_info)
                upsert_daily_quotes(conn, item.code, item.quotes, source="yfinance")
                # EDINETから新規取得した場合のみupdated_atを更新（30日キャッシュのため）
                if item.edinet_fetched or not item.statements:
                    stmt_source = "edinetdb" if item.edinet_fetched else "yfinance"
                    upsert_statements(conn, item.code, item.statements, source=stmt_source)
                upsert_dividends(conn, item.code, item.dividends, source="yfinance")
                max_pub = upsert_news(conn, item.code, item.news)
                upsert_judgments(conn, batch_run_id, item.code, item.judgments)
                conn.commit()
                # ニュースwatermarkを更新（commitの後）
                if max_pub:
                    upsert_watermark(item.code, "news", max_pub)
                logging.info("  %s: saved to DB", item.code)
            except Exception:
                conn.rollback()
                logging.exception("  %s: DB write failed", item.code)
            finally:
                q.task_done()
    finally:
        conn.close()


# ──────────────────────────────────────────────
# メイン
# ──────────────────────────────────────────────

def main() -> int:
    setup_logging()
    load_dotenv(override=True)

    init_db()
    watchlist = load_watchlist()
    logging.info("watchlist loaded: %d symbols", len(watchlist))

    batch_run_id = upsert_batch_run_start(target_count=len(watchlist))
    logging.info("batch run started: id=%d", batch_run_id)

    edinet_api_key = settings.edinet_api_key or os.getenv("EDINET_API_KEY", "")
    if edinet_api_key:
        logging.info("EdinetDB enabled (cache=%d days, rate_limit=1000/day)", EDINET_CACHE_DAYS)
    else:
        logging.info("EdinetDB not configured (EDINET_API_KEY unset); using yfinance only")

    # 決算発表予定（yfinanceでは空）
    announcements: list[dict[str, Any]] = []
    with get_conn() as conn:
        try:
            tmp_client = YFinanceSyncClient()
            ann = tmp_client.get_announcements()
            saved = upsert_announcements(conn, ann)
            conn.commit()
            logging.info("announcements saved: %d rows", saved)
        except Exception:
            logging.exception("failed to fetch announcements; continuing without")

    newsapi_key = os.getenv("NEWSAPI_KEY", "")
    if newsapi_key:
        logging.info("NewsAPI enabled")
    else:
        logging.info("NewsAPI disabled; Google News RSS + Yahoo Finance RSS のみ使用")

    edinet_limiter = DailyRateLimiter(daily_limit=1000)

    # ライタースレッド起動
    write_queue: Queue = Queue(maxsize=MAX_WORKERS * 2)
    writer = Thread(
        target=writer_loop,
        args=(write_queue, batch_run_id),
        daemon=True,
        name="db-writer",
    )
    writer.start()

    success_count = 0
    error_count = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="stock-worker") as executor:
            futures = {
                executor.submit(
                    fetch_stock, code, announcements, edinet_api_key, edinet_limiter, newsapi_key
                ): code
                for code in watchlist
            }
            for future in as_completed(futures):
                code = futures[future]
                logging.info("processing %s ...", code)
                try:
                    payload = future.result()
                    write_queue.put(payload)
                    success_count += 1
                except Exception:
                    error_count += 1
                    logging.exception("  %s: fetch failed (continuing)", code)

        # ワーカー全終了後、ライタースレッドに終了シグナル
        write_queue.put(None)
        writer.join()

        logging.info(
            "batch finished: success=%d error=%d (EDINET API used=%d)",
            success_count, error_count, edinet_limiter.used,
        )

        upsert_batch_run_finish(
            batch_run_id=batch_run_id,
            status="success" if error_count == 0 else "error",
            success_count=success_count,
            error_count=error_count,
            message=f"completed: success={success_count}, error={error_count}",
        )
        return 0

    except Exception as exc:
        write_queue.put(None)
        writer.join()
        upsert_batch_run_finish(
            batch_run_id=batch_run_id,
            status="error",
            success_count=success_count,
            error_count=error_count,
            message=f"fatal error: {exc}",
        )
        logging.exception("batch failed fatally")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
