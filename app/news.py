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

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 10
HTTP_TIMEOUT = 12

POSITIVE_KEYWORDS: dict[str, float] = {
    "増益": 0.9, "上方修正": 0.9, "最高益": 1.0, "好決算": 0.8, "増配": 0.8, "受注増": 0.6,
    "成長": 0.5, "提携": 0.4, "買収": 0.3, "upgrade": 0.5, "beat": 0.6, "outperform": 0.6,
    "record profit": 1.0, "dividend increase": 0.8,
}
NEGATIVE_KEYWORDS: dict[str, float] = {
    "減益": 0.9, "下方修正": 0.9, "赤字": 1.0, "減配": 0.8, "業績悪化": 0.8, "不祥事": 0.8,
    "訴訟": 0.7, "リコール": 0.7, "downgrade": 0.5, "miss": 0.6, "underperform": 0.6,
    "loss": 0.8, "dividend cut": 0.9,
}


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


def _sentiment_score(title: str, summary: str) -> float:
    """キーワードベースのセンチメントスコア（-1.0〜+1.0）を返す。"""
    text = f"{title} {summary}".lower()
    raw = 0.0
    for k, w in POSITIVE_KEYWORDS.items():
        if k in text:
            raw += w
    for k, w in NEGATIVE_KEYWORDS.items():
        if k in text:
            raw -= w
    score = max(-1.0, min(1.0, raw / 3.0))
    if abs(score) < 0.08:
        return 0.0
    return round(score, 3)


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
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, Any]]:
    """企業のニュースをRSS/NewsAPIから取得してセンチメントスコアを付与して返す。"""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

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
        score = _sentiment_score(r["title"], r.get("summary", ""))
        filtered.append({
            "published_at": r["published_at"],
            "title": r["title"],
            "url": r["url"],
            "summary": r.get("summary", ""),
            "sentiment_score": score,
            "source": r.get("source", "unknown"),
        })

    filtered.sort(key=lambda x: x["published_at"], reverse=True)
    return filtered[:limit]
