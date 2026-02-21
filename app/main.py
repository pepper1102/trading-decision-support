from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .db import get_last_run, init_db
from .repository import (
    get_candidates,
    get_daily_quotes,
    get_recent_news,
    get_stock_info,
    get_stock_judgments,
    get_summary,
)

BASE_DIR = Path(__file__).parent


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="株売買支援システム", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _get_latest_run_id() -> int | None:
    """最新のsuccessバッチのIDを返す。なければNone。"""
    row = get_last_run()
    return int(row["id"]) if row else None


# ──────────────────────────────────────────────
# ページルート
# ──────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """ダッシュボード。"""
    run_id = _get_latest_run_id()
    if run_id is None:
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "summary": None, "total": 0,
             "error": "データがありません。まず batch.py を実行してください。"},
        )

    summary = get_summary(run_id)
    # 全戦略の銘柄数（重複なし）
    with __import__("app.db", fromlist=["get_conn"]).get_conn() as conn:
        total = conn.execute(
            "SELECT COUNT(DISTINCT code) AS cnt FROM judgments WHERE batch_run_id=?", (run_id,)
        ).fetchone()["cnt"]

    last_run = get_last_run()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "summary": summary,
            "total": total,
            "error": None,
            "last_updated": last_run["finished_at"] if last_run else None,
        },
    )


@app.get("/candidates", response_class=HTMLResponse)
async def candidates(
    request: Request,
    strategy: str = Query("swing"),
    signal: str = Query("buy"),
    price_min: float | None = Query(None),
    price_max: float | None = Query(None),
):
    """買い/売り候補一覧ページ。"""
    run_id = _get_latest_run_id()
    if run_id is None:
        return templates.TemplateResponse(
            "candidates.html",
            {
                "request": request, "results": [], "strategy": strategy, "signal": signal,
                "price_min": price_min, "price_max": price_max,
                "strategy_label": _strategy_label(strategy),
                "signal_label": _signal_label(signal),
                "error": "データがありません。batch.py を実行してください。",
            },
        )

    results = get_candidates(run_id, strategy, signal, price_min, price_max)
    return templates.TemplateResponse(
        "candidates.html",
        {
            "request": request,
            "results": results,
            "strategy": strategy,
            "signal": signal,
            "price_min": price_min,
            "price_max": price_max,
            "strategy_label": _strategy_label(strategy),
            "signal_label": _signal_label(signal),
            "error": None,
        },
    )


@app.get("/stock/{code}", response_class=HTMLResponse)
async def stock_detail(request: Request, code: str):
    """銘柄詳細ページ。"""
    run_id = _get_latest_run_id()
    if run_id is None:
        raise HTTPException(status_code=503, detail="データがありません。batch.py を実行してください。")

    info = get_stock_info(code)
    name = info["name"] if info else code
    judgments = get_stock_judgments(run_id, code)
    quotes = get_daily_quotes(code, limit=30)

    chart_labels = [q["date"] for q in quotes]
    chart_closes = [q["close"] for q in quotes]
    chart_volumes = [q["volume"] for q in quotes]
    news_items = get_recent_news(code, limit=10, days=30)

    return templates.TemplateResponse(
        "detail.html",
        {
            "request": request,
            "code": code,
            "name": name,
            "judgments": judgments,
            "chart_labels": chart_labels,
            "chart_closes": chart_closes,
            "chart_volumes": chart_volumes,
            "news_items": news_items,
        },
    )


# ──────────────────────────────────────────────
# JSONエンドポイント
# ──────────────────────────────────────────────

@app.get("/api/stock/{code}")
async def api_stock(code: str):
    """銘柄の判定結果をJSON形式で返す。"""
    run_id = _get_latest_run_id()
    if run_id is None:
        raise HTTPException(status_code=503, detail="データがありません。batch.py を実行してください。")
    return get_stock_judgments(run_id, code)


# ──────────────────────────────────────────────
# ヘルパー
# ──────────────────────────────────────────────

def _strategy_label(s: str) -> str:
    return {"swing": "短期(スイング)", "fundamental": "中長期(ファンダ)", "dividend": "配当重視"}.get(s, s)


def _signal_label(s: str) -> str:
    return {"buy": "買い", "sell": "売り", "hold": "様子見"}.get(s, s)
