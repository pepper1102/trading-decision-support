"""APScheduler でクイックスタートジョブを平日のJSTスケジュールで実行する。"""
from __future__ import annotations

import asyncio
import logging
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .quickstart_jobs import (
    run_candidate_scan,
    run_entry_signal,
    run_exit_signal,
    run_survival_test,
)

JST = ZoneInfo("Asia/Tokyo")
LOG = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


async def _run_in_thread(fn) -> None:
    await asyncio.to_thread(fn)


def start_scheduler() -> AsyncIOScheduler:
    """スケジューラを起動する。既に起動中なら何もしない。"""
    global _scheduler
    if _scheduler and _scheduler.running:
        return _scheduler

    s = AsyncIOScheduler(
        timezone=JST,
        job_defaults={
            "coalesce": True,        # 遅延実行が重なったら1回だけ実行
            "max_instances": 1,      # 同じジョブの多重実行を防ぐ
            "misfire_grace_time": 120,
        },
    )

    # 14:50 — 候補抽出
    s.add_job(
        lambda: asyncio.create_task(_run_in_thread(run_candidate_scan)),
        CronTrigger(day_of_week="mon-fri", hour=14, minute=50, timezone=JST),
        id="qs_candidate_1450",
        replace_existing=True,
    )

    # 15:00〜15:15 毎分 — 生き残りテスト
    s.add_job(
        lambda: asyncio.create_task(_run_in_thread(run_survival_test)),
        CronTrigger(day_of_week="mon-fri", hour=15, minute="0-15", timezone=JST),
        id="qs_survival_1500_1515",
        replace_existing=True,
    )

    # 15:05〜15:14 毎分 — エントリーシグナル
    s.add_job(
        lambda: asyncio.create_task(_run_in_thread(run_entry_signal)),
        CronTrigger(day_of_week="mon-fri", hour=15, minute="5-14", timezone=JST),
        id="qs_entry_1505_1514",
        replace_existing=True,
    )

    # 9:00〜9:30 毎分 — 決済シグナル
    s.add_job(
        lambda: asyncio.create_task(_run_in_thread(run_exit_signal)),
        CronTrigger(day_of_week="mon-fri", hour=9, minute="0-30", timezone=JST),
        id="qs_exit_0900_0930",
        replace_existing=True,
    )

    s.start()
    LOG.info("quickstart scheduler started")
    _scheduler = s
    return s


def stop_scheduler() -> None:
    """スケジューラを停止する。"""
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        LOG.info("quickstart scheduler stopped")
    _scheduler = None
