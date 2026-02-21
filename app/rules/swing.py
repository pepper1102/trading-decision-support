from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from .models import RuleResult, StockJudgment


class SwingRuleEngine:
    """短期（スイング）ルール判定エンジン。"""

    LIQUIDITY_THRESHOLD = 1_000_000_000.0  # 10億円
    LIQUIDITY_WINDOW = 20
    MA_WINDOW = 25
    ENTRY_LOOKBACK = 20
    STOP_LOSS_THRESHOLD = -0.06   # -6%
    TAKE_PROFIT_THRESHOLD = 0.12  # +12%
    EARNINGS_AVOID_DAYS = 5

    # ルール重み（合計で正規化される加重平均用）
    # 大きいほど判定スコアへの影響が強い
    WEIGHT_LIQUIDITY = 1.5   # 流動性: エントリー前提条件
    WEIGHT_TREND = 2.5       # トレンド: 上位足方向性（最重要）
    WEIGHT_ENTRY = 2.0       # エントリー条件: タイミングの核心
    WEIGHT_STOP_LOSS = 1.0   # 損切り: 保有中判定（スクリーニング時はデータなし）
    WEIGHT_TAKE_PROFIT = 1.0 # 利確: 同上
    WEIGHT_EARNINGS = 2.0    # 決算回避: 必須回避条件

    def evaluate(
        self,
        code: str,
        name: str,
        quotes: list[dict[str, Any]],
        announcements: list[dict[str, Any]],
    ) -> StockJudgment:
        """
        スイング戦略のルールを評価して売買シグナルを返す。

        Args:
            code: 銘柄コード
            name: 銘柄名
            quotes: 日足データ（Date/Open/High/Low/Close/Volume/TurnoverValue など）
            announcements: 決算発表予定データ
        """
        ordered_quotes = self._sort_by_date(quotes)
        latest = ordered_quotes[-1] if ordered_quotes else {}
        latest_close = self._to_float(latest.get("Close"))
        as_of = self._extract_date_str(latest) or date.today().isoformat()
        as_of_date = self._parse_date(as_of)

        rule_results: list[RuleResult] = []

        liquidity_result = self._rule_liquidity(ordered_quotes)
        liquidity_result.weight = self.WEIGHT_LIQUIDITY
        rule_results.append(liquidity_result)

        trend_result, _ = self._rule_trend(ordered_quotes, latest_close)
        trend_result.weight = self.WEIGHT_TREND
        rule_results.append(trend_result)

        entry_result = self._rule_entry(ordered_quotes, latest_close)
        entry_result.weight = self.WEIGHT_ENTRY
        rule_results.append(entry_result)

        stop_loss_result, take_profit_result = self._rule_position_exit(ordered_quotes, latest_close)
        stop_loss_result.weight = self.WEIGHT_STOP_LOSS
        take_profit_result.weight = self.WEIGHT_TAKE_PROFIT
        rule_results.append(stop_loss_result)
        rule_results.append(take_profit_result)

        earnings_result = self._rule_earnings_avoid(code, announcements, as_of_date)
        earnings_result.weight = self.WEIGHT_EARNINGS
        rule_results.append(earnings_result)

        total_weight = sum(r.weight for r in rule_results)
        weighted_passed = sum(r.weight for r in rule_results if r.passed)
        score = (weighted_passed / total_weight) if total_weight else 0.0

        clear_sell = stop_loss_result.passed or take_profit_result.passed
        if score >= 0.7 and earnings_result.passed and not clear_sell:
            signal = "buy"
        elif clear_sell:
            signal = "sell"
        else:
            signal = "hold"

        return StockJudgment(
            code=code,
            name=name,
            strategy="swing",
            signal=signal,
            score=round(score, 4),
            price=latest_close,
            rule_results=rule_results,
            as_of=as_of,
        )

    def _rule_liquidity(self, quotes: list[dict[str, Any]]) -> RuleResult:
        """20日平均売買代金が10億円以上かを判定する。"""
        if len(quotes) < self.LIQUIDITY_WINDOW:
            return RuleResult(
                rule_name="対象流動性",
                value=None,
                threshold="20日平均売買代金 >= 10億円",
                passed=False,
                reason="データなし",
            )

        values: list[float] = []
        for q in quotes[-self.LIQUIDITY_WINDOW:]:
            turnover = self._to_float(q.get("TurnoverValue"))
            if turnover is None:
                close = self._to_float(q.get("Close"))
                volume = self._to_float(q.get("Volume"))
                if close is not None and volume is not None:
                    turnover = close * volume
            if turnover is not None:
                values.append(turnover)

        if not values:
            return RuleResult(
                rule_name="対象流動性",
                value=None,
                threshold="20日平均売買代金 >= 10億円",
                passed=False,
                reason="データなし",
            )

        avg_turnover = sum(values) / len(values)
        passed = avg_turnover >= self.LIQUIDITY_THRESHOLD
        return RuleResult(
            rule_name="対象流動性",
            value=avg_turnover,
            threshold="20日平均売買代金 >= 10億円",
            passed=passed,
            reason=f"20日平均売買代金={avg_turnover / 1e8:.1f}億円",
        )

    def _rule_trend(
        self, quotes: list[dict[str, Any]], latest_close: float | None
    ) -> tuple[RuleResult, float | None]:
        """直近終値が25日移動平均線より上かを判定する。"""
        closes = [self._to_float(q.get("Close")) for q in quotes]
        valid_closes = [c for c in closes if c is not None]

        if latest_close is None or len(valid_closes) < self.MA_WINDOW:
            return (
                RuleResult(
                    rule_name="トレンド",
                    value=None,
                    threshold="直近終値 > 25日移動平均",
                    passed=False,
                    reason="データなし",
                ),
                None,
            )

        ma25 = sum(valid_closes[-self.MA_WINDOW:]) / self.MA_WINDOW
        passed = latest_close > ma25
        return (
            RuleResult(
                rule_name="トレンド",
                value=latest_close - ma25,
                threshold="直近終値 > 25日移動平均",
                passed=passed,
                reason=f"終値={latest_close:.0f}, MA25={ma25:.0f}",
            ),
            ma25,
        )

    def _rule_entry(
        self, quotes: list[dict[str, Any]], latest_close: float | None
    ) -> RuleResult:
        """押し目買いまたは高値更新のエントリー条件を判定する。"""
        if latest_close is None or len(quotes) < 2:
            return RuleResult(
                rule_name="エントリー条件",
                value=None,
                threshold="押し目(-5%〜-10%) or 高値更新",
                passed=False,
                reason="データなし",
            )

        lookback_quotes = quotes[-self.ENTRY_LOOKBACK:]
        lookback_highs = [self._to_float(q.get("High")) for q in lookback_quotes]
        lookback_highs = [h for h in lookback_highs if h is not None]
        if not lookback_highs:
            return RuleResult(
                rule_name="エントリー条件",
                value=None,
                threshold="押し目(-5%〜-10%) or 高値更新",
                passed=False,
                reason="データなし",
            )

        recent_high = max(lookback_highs)
        drawdown = (latest_close / recent_high) - 1.0
        is_pullback = -0.10 <= drawdown <= -0.05

        prev_quotes = lookback_quotes[:-1] if len(lookback_quotes) > 1 else quotes[:-1]
        prev_highs = [self._to_float(q.get("High")) for q in prev_quotes]
        prev_highs = [h for h in prev_highs if h is not None]
        is_breakout = bool(prev_highs) and latest_close > max(prev_highs)

        passed = is_pullback or is_breakout
        if is_breakout:
            reason = f"高値更新: 終値={latest_close:.0f}"
        elif is_pullback:
            reason = f"押し目: 直近高値比={drawdown * 100:.1f}%"
        else:
            reason = f"条件外: 直近高値比={drawdown * 100:.1f}%"

        return RuleResult(
            rule_name="エントリー条件",
            value=drawdown * 100.0,
            threshold="押し目(-5%〜-10%) or 高値更新",
            passed=passed,
            reason=reason,
        )

    def _rule_position_exit(
        self,
        quotes: list[dict[str, Any]],
        latest_close: float | None,
    ) -> tuple[RuleResult, RuleResult]:
        """損切り・利確の売り条件を判定する。（取得単価キーがあれば使用）"""
        acquisition_price = self._extract_acquisition_price(quotes)
        if latest_close is None or acquisition_price is None:
            no_data_sl = RuleResult(
                rule_name="損切り",
                value=None,
                threshold="取得単価から -6% 以下",
                passed=False,
                reason="取得単価なし（スクリーニング用）",
            )
            no_data_tp = RuleResult(
                rule_name="利確第1目標",
                value=None,
                threshold="取得単価から +12% 以上",
                passed=False,
                reason="取得単価なし（スクリーニング用）",
            )
            return no_data_sl, no_data_tp

        pnl = (latest_close / acquisition_price) - 1.0
        stop_loss = RuleResult(
            rule_name="損切り",
            value=pnl * 100.0,
            threshold="取得単価から -6% 以下",
            passed=pnl <= self.STOP_LOSS_THRESHOLD,
            reason=f"騰落率={pnl * 100:.2f}%",
        )
        take_profit = RuleResult(
            rule_name="利確第1目標",
            value=pnl * 100.0,
            threshold="取得単価から +12% 以上",
            passed=pnl >= self.TAKE_PROFIT_THRESHOLD,
            reason=f"騰落率={pnl * 100:.2f}%",
        )
        return stop_loss, take_profit

    def _rule_earnings_avoid(
        self,
        code: str,
        announcements: list[dict[str, Any]],
        as_of: date | None,
    ) -> RuleResult:
        """直近5営業日以内の決算発表予定がなければOK。"""
        if as_of is None:
            return RuleResult(
                rule_name="決算回避",
                value=None,
                threshold="直近5営業日以内に決算予定なし",
                passed=True,
                reason="判定日不明のため通過",
            )

        near_event = False
        for ann in announcements:
            ann_code = str(ann.get("Code") or ann.get("LocalCode") or "").strip()
            if ann_code and ann_code != code:
                continue
            ann_date = self._extract_announcement_date(ann)
            if ann_date is None or ann_date < as_of:
                continue
            if self._business_days_between(as_of, ann_date) <= self.EARNINGS_AVOID_DAYS:
                near_event = True
                break

        return RuleResult(
            rule_name="決算回避",
            value=None,
            threshold="直近5営業日以内に決算予定なし",
            passed=not near_event,
            reason="決算予定あり（新規エントリー禁止）" if near_event else "決算予定なし",
        )

    @staticmethod
    def _extract_acquisition_price(quotes: list[dict[str, Any]]) -> float | None:
        candidates = ("AcquisitionPrice", "CostBasis", "AveragePrice", "EntryPrice")
        for q in reversed(quotes):
            for k in candidates:
                v = SwingRuleEngine._to_float(q.get(k))
                if v is not None and v > 0:
                    return v
        return None

    @staticmethod
    def _extract_announcement_date(ann: dict[str, Any]) -> date | None:
        for key in ("AnnouncementDate", "Date", "DisclosedDate", "ScheduledDate"):
            d = SwingRuleEngine._parse_date(ann.get(key))
            if d is not None:
                return d
        return None

    @staticmethod
    def _sort_by_date(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(rows, key=lambda r: SwingRuleEngine._parse_date(r.get("Date")) or date.min)

    @staticmethod
    def _extract_date_str(row: dict[str, Any]) -> str | None:
        v = row.get("Date")
        if isinstance(v, str) and v:
            return v[:10]
        return None

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if not isinstance(value, str) or not value:
            return None
        text = value.strip().replace("/", "-")[:10]
        try:
            return datetime.strptime(text, "%Y-%m-%d").date()
        except ValueError:
            return None

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _business_days_between(start: date, end: date) -> int:
        if end <= start:
            return 0
        days = 0
        cursor = start
        while cursor < end:
            cursor += timedelta(days=1)
            if cursor.weekday() < 5:
                days += 1
        return days
