from __future__ import annotations

import html
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus

import requests

from .config import settings
from .sentiment import score_hybrid

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 10
HTTP_TIMEOUT = 12


def _strip_html(text: str | None) -> str:
    if not text:
        return ""
    raw = re.sub(r"<[^>]+>", " ", text)
    return " ".join(html.unescape(raw).split())


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    try:
        dt = parsedate_to_datetime(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")



def _fetch_rss(url: str, source_name: str) -> list[dict[str, Any]]:
    try:
        resp = requests.get(
            url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "stock-watch-app/1.0"}
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
    except Exception:
        logging.warning("RSS fetch failed: %s", url)
        return []

    rows: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        desc = _strip_html(item.findtext("description") or "")
        src = (item.findtext("source") or "").strip() or source_name
        dt = _parse_dt(pub)
        if not title or not link or not dt:
            continue
        rows.append({
            "published_at": _to_iso(dt),
            "title": title,
            "url": link,
            "summary": desc[:500],
            "source": src,
        })
    return rows


def _fetch_google_news(code: str, company_name: str) -> list[dict[str, Any]]:
    queries = [company_name, f"{company_name} 株", f"{code} 株価"]
    out: list[dict[str, Any]] = []
    for q in queries:
        url = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=ja&gl=JP&ceid=JP:ja"
        out.extend(_fetch_rss(url, "Google News"))
    return out


def _fetch_yahoo_finance(code: str) -> list[dict[str, Any]]:
    symbol = f"{code}.T"
    url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=JP&lang=ja-JP"
    return _fetch_rss(url, "Yahoo Finance")


def _fetch_newsapi(company_name: str, api_key: str, lookback_days: int) -> list[dict[str, Any]]:
    if not api_key:
        return []
    endpoint = "https://newsapi.org/v2/everything"
    params = {
        "q": company_name,
        "language": "jp",
        "sortBy": "publishedAt",
        "pageSize": 30,
        "from": (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date().isoformat(),
    }
    try:
        resp = requests.get(
            endpoint, params=params, headers={"X-Api-Key": api_key}, timeout=HTTP_TIMEOUT
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        logging.warning("NewsAPI fetch failed: %s", company_name)
        return []

    rows: list[dict[str, Any]] = []
    for a in payload.get("articles", []):
        title = (a.get("title") or "").strip()
        link = (a.get("url") or "").strip()
        dt = _parse_dt(a.get("publishedAt"))
        if not title or not link or not dt:
            continue
        rows.append({
            "published_at": _to_iso(dt),
            "title": title,
            "url": link,
            "summary": _strip_html(a.get("description") or ""),
            "source": (a.get("source") or {}).get("name") or "NewsAPI",
        })
    return rows


def fetch_company_news(
    code: str,
    company_name: str,
    newsapi_key: str = "",
    since: str | None = None,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """企業のニュースをRSS/NewsAPIから取得してセンチメントスコアを付与して返す。

    since: ISO日時文字列。指定時はその日時以降の記事のみ返す（watermark増分取得用）。
           未指定時は lookback_days 日分を返す。
    """
    now = datetime.now(timezone.utc)
    if since:
        cutoff = _parse_dt(since) or (now - timedelta(days=lookback_days))
    else:
        cutoff = now - timedelta(days=lookback_days)

    raw: list[dict[str, Any]] = []
    raw.extend(_fetch_google_news(code, company_name))
    raw.extend(_fetch_yahoo_finance(code))
    raw.extend(_fetch_newsapi(company_name, newsapi_key, lookback_days))

    # URL重複排除
    dedup: dict[str, dict[str, Any]] = {}
    for r in raw:
        if r["url"] not in dedup:
            dedup[r["url"]] = r

    filtered: list[dict[str, Any]] = []
    for r in dedup.values():
        dt = _parse_dt(r["published_at"])
        if not dt or dt < cutoff:
            continue
        s = score_hybrid(r["title"], r.get("summary", ""), mode=settings.sentiment_mode)
        filtered.append({
            "published_at": r["published_at"],
            "title": r["title"],
            "url": r["url"],
            "summary": r.get("summary", ""),
            "sentiment_score": s["score"],
            "sentiment_method": s["method"],
            "sentiment_model": s["model_version"],
            "sentiment_confidence": s["confidence"],
            "source": r.get("source", "unknown"),
        })

    filtered.sort(key=lambda x: x["published_at"], reverse=True)
    return filtered[:limit]
