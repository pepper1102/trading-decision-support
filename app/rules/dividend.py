from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .models import RuleResult, StockJudgment


class DividendRuleEngine:
    """配当重視（インカム）ルール判定エンジン。"""

    YIELD_MIN = 0.030   # 配当利回り 3.0%以上
    YIELD_MAX = 0.050   # 配当利回り 5.0%以下（高すぎる利回りは要注意）
    PAYOUT_MIN = 0.30   # 配当性向 30%以上
    PAYOUT_MAX = 0.60   # 配当性向 60%以下

    # ルール重み（加重平均用）
    WEIGHT_YIELD = 2.0         # 配当利回り: インカム目的の核心
    WEIGHT_PAYOUT = 1.5        # 配当性向: 持続性の安定性指標
    WEIGHT_CONSECUTIVE = 2.0   # 連続配当: 長期安定性の確認
    WEIGHT_NO_CUT = 3.0        # 減配リスク: 最重要（即退場条件）

    def evaluate(
        self,
        code: str,
        name: str,
        quotes: list[dict[str, Any]],
        statements: list[dict[str, Any]],
        dividend_data: list[dict[str, Any]],
    ) -> StockJudgment:
        """
        配当戦略のルールを評価して売買シグナルを返す。

        Args:
            code: 銘柄コード
            name: 銘柄名
            quotes: 日足データ
            statements: 財務諸表データ
            dividend_data: 配当情報データ
        """
        ordered_quotes = sorted(
            quotes, key=lambda q: self._parse_date(q.get("Date")) or date.min
        )
        latest_quote = ordered_quotes[-1] if ordered_quotes else {}
        latest_close = self._to_float(latest_quote.get("Close"))
        as_of = (latest_quote.get("Date") or date.today().isoformat())[:10]

        ordered_divs = sorted(
            dividend_data,
            key=lambda d: self._parse_date(d.get("RecordDate") or d.get("Date")) or date.min,
        )
        ordered_stmts = sorted(
            statements,
            key=lambda s: self._parse_date(s.get("DisclosedDate")) or date.min,
        )

        rule_results: list[RuleResult] = []

        div_yield = self._rule_dividend_yield(latest_close, ordered_divs)
        div_yield.weight = self.WEIGHT_YIELD
        rule_results.append(div_yield)

        payout = self._rule_payout_ratio(ordered_stmts, ordered_divs)
        payout.weight = self.WEIGHT_PAYOUT
        rule_results.append(payout)

        consecutive = self._rule_consecutive_dividend(ordered_divs)
        consecutive.weight = self.WEIGHT_CONSECUTIVE
        rule_results.append(consecutive)

        no_cut = self._rule_no_cut(ordered_divs)
        no_cut.weight = self.WEIGHT_NO_CUT
        rule_results.append(no_cut)

        total_weight = sum(r.weight for r in rule_results)
        weighted_passed = sum(r.weight for r in rule_results if r.passed)
        score = (weighted_passed / total_weight) if total_weight else 0.0

        # 連続配当なしや減配は売り（no_cutはすでに上で定義済み）
        if score >= 0.7:
            signal = "buy"
        elif not no_cut.passed:
            signal = "sell"
        else:
            signal = "hold"

        return StockJudgment(
            code=code,
            name=name,
            strategy="dividend",
            signal=signal,
            score=round(score, 4),
            price=latest_close,
            rule_results=rule_results,
            as_of=as_of,
        )

    def _rule_dividend_yield(
        self, latest_close: float | None, divs: list[dict[str, Any]]
    ) -> RuleResult:
        """配当利回りが3.0%〜5.0%の範囲かを判定する。"""
        if latest_close is None or latest_close == 0:
            return RuleResult("配当利回り", None, "3.0%〜5.0%", False, "データなし")

        annual_div = self._calc_annual_dividend(divs)
        if annual_div is None:
            return RuleResult("配当利回り", None, "3.0%〜5.0%", False, "配当データなし")

        yield_rate = annual_div / latest_close
        passed = self.YIELD_MIN <= yield_rate <= self.YIELD_MAX
        reason = f"配当利回り={yield_rate * 100:.2f}% (年間配当={annual_div:.0f}円)"
        if yield_rate > self.YIELD_MAX:
            reason += " ※高すぎる利回りに注意"
        return RuleResult(
            rule_name="配当利回り",
            value=yield_rate * 100.0,
            threshold="3.0%〜5.0%",
            passed=passed,
            reason=reason,
        )

    def _rule_payout_ratio(
        self, stmts: list[dict[str, Any]], divs: list[dict[str, Any]]
    ) -> RuleResult:
        """配当性向が30%〜60%の範囲かを判定する。"""
        latest_stmt = stmts[-1] if stmts else None
        if not latest_stmt:
            return RuleResult("配当性向", None, "30%〜60%", False, "財務データなし")

        eps = self._get_eps(latest_stmt)
        annual_div = self._calc_annual_dividend(divs)
        if eps is None or annual_div is None or eps == 0:
            return RuleResult("配当性向", None, "30%〜60%", False, "データなし")

        payout = annual_div / eps
        passed = self.PAYOUT_MIN <= payout <= self.PAYOUT_MAX
        return RuleResult(
            rule_name="配当性向",
            value=payout * 100.0,
            threshold="30%〜60%",
            passed=passed,
            reason=f"配当性向={payout * 100:.2f}%",
        )

    def _rule_consecutive_dividend(self, divs: list[dict[str, Any]]) -> RuleResult:
        """連続配当実績を確認する（3期以上）。"""
        if len(divs) < 3:
            return RuleResult("連続配当", None, "3期以上の配当実績", False, "データ不足")

        consecutive = 0
        for d in divs:
            amount = self._get_dividend_amount(d)
            if amount is not None and amount > 0:
                consecutive += 1
            else:
                break

        passed = consecutive >= 3
        return RuleResult(
            rule_name="連続配当",
            value=float(consecutive),
            threshold="3期以上の配当実績",
            passed=passed,
            reason=f"連続配当{consecutive}期",
        )

    def _rule_no_cut(self, divs: list[dict[str, Any]]) -> RuleResult:
        """直近で減配・無配がないかを確認する。"""
        if len(divs) < 2:
            return RuleResult("減配リスク", None, "減配・無配なし", True, "データ不足（通過）")

        recent = divs[-2:]
        amounts = [self._get_dividend_amount(d) for d in recent]
        if any(a is None for a in amounts):
            return RuleResult("減配リスク", None, "減配・無配なし", True, "データなし（通過）")

        prev, curr = amounts[0], amounts[1]
        if curr == 0:
            return RuleResult("減配リスク", curr, "減配・無配なし", False, "無配")
        if prev is not None and curr < prev:
            return RuleResult("減配リスク", curr, "減配・無配なし", False, f"減配: {prev:.0f}→{curr:.0f}円")
        return RuleResult(
            rule_name="減配リスク",
            value=curr,
            threshold="減配・無配なし",
            passed=True,
            reason=f"配当維持・増配: 直近={curr:.0f}円",
        )

    @staticmethod
    def _calc_annual_dividend(divs: list[dict[str, Any]]) -> float | None:
        """直近1〜2期の配当から年間配当額を推計する。"""
        if not divs:
            return None
        amounts = []
        for d in divs[-4:]:
            a = DividendRuleEngine._get_dividend_amount(d)
            if a is not None:
                amounts.append(a)
        if not amounts:
            return None
        return sum(amounts)

    @staticmethod
    def _get_dividend_amount(d: dict[str, Any]) -> float | None:
        for key in ("DividendPayableDate", "ForecastDividendPerShare", "AnnualDividendPerShare", "DividendPerShare", "Dividend"):
            v = DividendRuleEngine._to_float(d.get(key))
            if v is not None:
                return v
        return None

    @staticmethod
    def _get_eps(stmt: dict[str, Any]) -> float | None:
        for key in ("EarningsPerShare", "BasicEarningsPerShare", "EPS"):
            v = DividendRuleEngine._to_float(stmt.get(key))
            if v is not None:
                return v
        net_income = DividendRuleEngine._to_float(stmt.get("NetIncome"))
        shares = DividendRuleEngine._to_float(stmt.get("NumberOfShares") or stmt.get("IssuedSharesTotalNumber"))
        if net_income and shares and shares > 0:
            return net_income / shares
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
