"""ギャップアップ引け前仕込み戦略 ― スケジュールジョブ群

タイムライン（JST, 平日のみ）
  14:50        run_candidate_scan   ギャップアップ率ランキング抽出
  15:00〜15:15 run_survival_test    生き残りテスト（1分ごと）
  15:05〜15:14 run_entry_signal     エントリーシグナルDB書き込み
   9:00〜 9:30 run_exit_signal      決済シグナルDB書き込み

注意: 実際の注文は行わない。シグナルをDBに記録するのみ。
"""
from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf

from .config import load_watchlist
from .db import get_conn

JST = ZoneInfo("Asia/Tokyo")
LOG = logging.getLogger(__name__)

# ── パラメータ（deep-research-report.md に基づく） ──────────────────
CANDIDATE_LIMIT = 10          # 候補上限銘柄数（仮定）
GAP_UP_RATE_MIN = 0.10        # ギャップアップ率 +10% 以上
VOLUME_RATIO_MIN = 2.0        # 出来高前日比 2倍 以上
HIGH_DISTANCE_MAX = 0.05      # 高値からの押し 5% 以内
SURVIVAL_DROP_LIMIT = -0.02   # 15:00 基準から -2% で生存除外（仮定）
MAX_ENTRIES_PER_DAY = 2       # 同日最大エントリー銘柄数（仮定）
ENTRY_ALLOCATION_PCT = 0.02   # 1銘柄あたり口座の 2%（仮定）
TAKE_PROFIT = 0.05            # +5% で利確（出典あり）
STOP_LOSS = -0.02             # -2% で損切り（仮定）


# ── ユーティリティ ───────────────────────────────────────────────────

def _now_jst() -> datetime:
    return datetime.now(tz=JST)


def _today_jst() -> str:
    return _now_jst().date().isoformat()


def _ts_jst() -> str:
    return _now_jst().isoformat(timespec="seconds")


def _symbol(code: str) -> str:
    return code if code.endswith(".T") else f"{code}.T"


def _latest_price_and_volume(code: str) -> tuple[float | None, float | None]:
    """最新の価格と累計出来高を返す。取得失敗時は (None, None)。"""
    t = yf.Ticker(_symbol(code))
    d1m = t.history(period="1d", interval="1m", auto_adjust=False)
    if d1m is not None and not d1m.empty:
        last = d1m.iloc[-1]
        return float(last["Close"]), float(last.get("Volume", 0.0))
    d1d = t.history(period="5d", interval="1d", auto_adjust=False)
    if d1d is None or d1d.empty:
        return None, None
    last = d1d.iloc[-1]
    return float(last["Close"]), float(last.get("Volume", 0.0))


# ── ジョブ ───────────────────────────────────────────────────────────

def run_candidate_scan() -> None:
    """14:50 — ギャップアップ率ランキングを抽出し qs_candidates に書き込む。"""
    trade_date = _today_jst()
    now = _ts_jst()
    rows: list[dict] = []

    for code in load_watchlist():
        try:
            t = yf.Ticker(_symbol(code))
            d = t.history(period="5d", interval="1d", auto_adjust=False)
            if d is None or len(d) < 2:
                continue

            prev = d.iloc[-2]
            today = d.iloc[-1]
            prev_close = float(prev["Close"])
            day_open = float(today["Open"])
            day_high = float(today["High"])
            today_vol = float(today.get("Volume", 0.0))
            prev_vol = float(prev.get("Volume", 0.0))

            if prev_close <= 0:
                continue

            latest_price, _ = _latest_price_and_volume(code)
            latest_price = latest_price or float(today["Close"])

            gap_up_rate = (day_open - prev_close) / prev_close
            volume_ratio = (today_vol / prev_vol) if prev_vol > 0 else None
            high_distance = ((day_high - latest_price) / day_high) if day_high > 0 else None

            if gap_up_rate < GAP_UP_RATE_MIN:
                continue
            if volume_ratio is None or volume_ratio < VOLUME_RATIO_MIN:
                continue
            if high_distance is None or high_distance > HIGH_DISTANCE_MAX:
                continue

            rows.append({
                "code": code,
                "gap_up_rate": gap_up_rate,
                "prev_close": prev_close,
                "day_open": day_open,
                "day_high": day_high,
                "latest_price": latest_price,
                "volume_ratio": volume_ratio,
                "high_distance": high_distance,
            })
        except Exception:
            LOG.exception("candidate_scan: failed for %s", code)

    rows.sort(key=lambda x: x["gap_up_rate"], reverse=True)
    rows = rows[:CANDIDATE_LIMIT]

    with get_conn() as conn:
        for r in rows:
            conn.execute(
                """
                INSERT INTO qs_candidates (
                    trade_date, code, gap_up_rate, prev_close, day_open, day_high,
                    latest_price, volume_ratio, high_distance, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'picked', ?, ?)
                ON CONFLICT(trade_date, code) DO UPDATE SET
                    gap_up_rate    = excluded.gap_up_rate,
                    prev_close     = excluded.prev_close,
                    day_open       = excluded.day_open,
                    day_high       = excluded.day_high,
                    latest_price   = excluded.latest_price,
                    volume_ratio   = excluded.volume_ratio,
                    high_distance  = excluded.high_distance,
                    status         = 'picked',
                    reject_reason  = NULL,
                    updated_at     = excluded.updated_at
                """,
                (
                    trade_date, r["code"], r["gap_up_rate"], r["prev_close"],
                    r["day_open"], r["day_high"], r["latest_price"],
                    r["volume_ratio"], r["high_distance"], now, now,
                ),
            )
        conn.commit()
    LOG.info("candidate_scan: %d candidates saved for %s", len(rows), trade_date)


def run_survival_test() -> None:
    """15:00〜15:15 — 候補銘柄の生き残りテスト（1分ごとに呼ばれる）。

    判定基準:
      - 15:00 時点価格から -2% 超の下落 → rejected
      - 直前1分の出来高増分ゼロ（歩み値枯れ）→ rejected
      - それ以外 → alive を維持
    """
    trade_date = _today_jst()
    now = _ts_jst()

    with get_conn() as conn:
        candidates = conn.execute(
            "SELECT code FROM qs_candidates WHERE trade_date=? AND status IN ('picked','alive')",
            (trade_date,),
        ).fetchall()

        for c in candidates:
            code = c["code"]
            try:
                price, cum_vol = _latest_price_and_volume(code)
                if price is None:
                    continue

                # 15:00 基準価格（初回スナップショットを流用）
                base_row = conn.execute(
                    """
                    SELECT base_price_1500 FROM qs_survival_snapshots
                    WHERE trade_date=? AND code=? AND base_price_1500 IS NOT NULL
                    ORDER BY id ASC LIMIT 1
                    """,
                    (trade_date, code),
                ).fetchone()
                base = float(base_row["base_price_1500"]) if base_row else price
                drop = (price / base) - 1.0

                # 前回スナップとの出来高差分
                prev_row = conn.execute(
                    """
                    SELECT cum_volume FROM qs_survival_snapshots
                    WHERE trade_date=? AND code=? ORDER BY id DESC LIMIT 1
                    """,
                    (trade_date, code),
                ).fetchone()
                prev_cum = float(prev_row["cum_volume"]) if prev_row and prev_row["cum_volume"] is not None else None
                delta = (cum_vol - prev_cum) if (cum_vol is not None and prev_cum is not None) else None

                conn.execute(
                    """
                    INSERT INTO qs_survival_snapshots
                        (trade_date, ts_jst, code, price, cum_volume, delta_volume,
                         base_price_1500, drop_from_1500)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (trade_date, now, code, price, cum_vol, delta, base, drop),
                )

                reject_reason = None
                if drop <= SURVIVAL_DROP_LIMIT:
                    reject_reason = f"drop {drop:.1%} from 15:00 base"
                elif delta is not None and delta <= 0:
                    reject_reason = "volume stalled"

                if reject_reason:
                    conn.execute(
                        "UPDATE qs_candidates SET status='rejected', reject_reason=?, updated_at=? "
                        "WHERE trade_date=? AND code=?",
                        (reject_reason, now, trade_date, code),
                    )
                    LOG.info("survival_test: %s rejected (%s)", code, reject_reason)
                else:
                    conn.execute(
                        "UPDATE qs_candidates SET status='alive', updated_at=? WHERE trade_date=? AND code=?",
                        (now, trade_date, code),
                    )
            except Exception:
                LOG.exception("survival_test: failed for %s", code)

        conn.commit()


def run_entry_signal() -> None:
    """15:05〜15:14 — alive 銘柄に buy シグナルを生成する（最大2銘柄/日）。

    シグナルは qs_order_signals と qs_positions に記録するのみ。実際の発注は行わない。
    """
    trade_date = _today_jst()
    now = _ts_jst()

    with get_conn() as conn:
        open_count = conn.execute(
            "SELECT COUNT(*) AS c FROM qs_positions WHERE state='open' AND entry_date=?",
            (trade_date,),
        ).fetchone()["c"]

        if open_count >= MAX_ENTRIES_PER_DAY:
            return

        alive = conn.execute(
            "SELECT code FROM qs_candidates WHERE trade_date=? AND status='alive' ORDER BY gap_up_rate DESC",
            (trade_date,),
        ).fetchall()

        for row in alive:
            if open_count >= MAX_ENTRIES_PER_DAY:
                break
            code = row["code"]

            # 既にオープンポジションがある場合はスキップ
            if conn.execute(
                "SELECT 1 FROM qs_positions WHERE code=? AND state='open' LIMIT 1", (code,)
            ).fetchone():
                continue

            try:
                price, _ = _latest_price_and_volume(code)
                if price is None:
                    continue

                conn.execute(
                    """
                    INSERT INTO qs_order_signals
                        (trade_date, ts_jst, code, side, signal_type, price, reason)
                    VALUES (?, ?, ?, 'buy', 'entry', ?, ?)
                    """,
                    (trade_date, now, code, price, "alive at 15:05-15:14"),
                )
                conn.execute(
                    """
                    INSERT INTO qs_positions
                        (code, entry_date, entry_ts_jst, entry_price, allocation_pct, state)
                    VALUES (?, ?, ?, ?, ?, 'open')
                    """,
                    (code, trade_date, now, price, ENTRY_ALLOCATION_PCT),
                )
                open_count += 1
                LOG.info("entry_signal: BUY %s @ %.1f", code, price)
            except Exception:
                LOG.exception("entry_signal: failed for %s", code)

        conn.commit()


def run_exit_signal() -> None:
    """9:00〜9:30 — オープンポジションの決済シグナルを生成する。

    決済条件:
      +5% 以上 → take_profit_5%
      -2% 以下 → stop_loss_2%
      9:30 到達 → time_stop_9:30 （強制撤退）
    """
    now_dt = _now_jst()
    trade_date = now_dt.date().isoformat()
    now = now_dt.isoformat(timespec="seconds")
    force_close = now_dt.hour == 9 and now_dt.minute >= 30

    with get_conn() as conn:
        positions = conn.execute(
            "SELECT id, code, entry_price FROM qs_positions WHERE state='open'",
        ).fetchall()

        for p in positions:
            pos_id = int(p["id"])
            code = p["code"]
            entry_price = float(p["entry_price"])

            try:
                price, _ = _latest_price_and_volume(code)
                if price is None or entry_price <= 0:
                    continue

                pnl = (price / entry_price) - 1.0
                reason: str | None = None
                if pnl >= TAKE_PROFIT:
                    reason = f"take_profit_{TAKE_PROFIT:.0%}"
                elif pnl <= STOP_LOSS:
                    reason = f"stop_loss_{STOP_LOSS:.0%}"
                elif force_close:
                    reason = "time_stop_9:30"

                if not reason:
                    continue

                conn.execute(
                    """
                    INSERT INTO qs_order_signals
                        (trade_date, ts_jst, code, side, signal_type, price, reason)
                    VALUES (?, ?, ?, 'sell', 'exit', ?, ?)
                    """,
                    (trade_date, now, code, price, reason),
                )
                conn.execute(
                    """
                    UPDATE qs_positions
                    SET state='closed', exit_date=?, exit_ts_jst=?, exit_price=?, exit_reason=?
                    WHERE id=?
                    """,
                    (trade_date, now, price, reason, pos_id),
                )
                LOG.info("exit_signal: SELL %s @ %.1f (%s, pnl=%.1f%%)", code, price, reason, pnl * 100)
            except Exception:
                LOG.exception("exit_signal: failed for %s", code)

        conn.commit()
