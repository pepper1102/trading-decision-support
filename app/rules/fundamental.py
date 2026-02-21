from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .models import RuleResult, StockJudgment


class FundamentalRuleEngine:
    """中長期（ファンダ）ルール判定エンジン。"""

    SALES_CAGR_THRESHOLD = 0.05       # 3年CAGR +5%
    OPERATING_MARGIN_THRESHOLD = 0.10  # 営業利益率 10%
    EQUITY_RATIO_THRESHOLD = 0.40      # 自己資本比率 40%
    ROE_THRESHOLD = 0.08               # ROE 8%
    MA_WINDOW = 25

    # ルール重み（加重平均用）
    WEIGHT_SALES_CAGR = 2.0       # 売上CAGR: 成長性の基本
    WEIGHT_OP_MARGIN = 2.5        # 営業利益率: 競争優位の最重要指標
    WEIGHT_EQUITY_RATIO = 1.5     # 自己資本比率: 財務健全性（業種差あり）
    WEIGHT_ROE = 2.0              # ROE: 資本効率の核心
    WEIGHT_MOMENTUM = 1.0         # モメンタム(MA25): テクニカル補助

    def evaluate(
        self,
        code: str,
        name: str,
        quotes: list[dict[str, Any]],
        statements: list[dict[str, Any]],
    ) -> StockJudgment:
        """
        ファンダ戦略のルールを評価して売買シグナルを返す。

        Args:
            code: 銘柄コード
            name: 銘柄名
            quotes: 日足データ
            statements: 財務諸表データ
        """
        ordered_quotes = sorted(
            quotes, key=lambda q: self._parse_date(q.get("Date")) or date.min
        )
        latest_quote = ordered_quotes[-1] if ordered_quotes else {}
        latest_close = self._to_float(latest_quote.get("Close"))
        as_of = (latest_quote.get("Date") or date.today().isoformat())[:10]

        ordered_statements = sorted(
            statements,
            key=lambda s: self._parse_date(s.get("DisclosedDate")) or date.min,
        )

        rule_results: list[RuleResult] = []
        sales_cagr = self._rule_sales_cagr(ordered_statements)
        sales_cagr.weight = self.WEIGHT_SALES_CAGR
        rule_results.append(sales_cagr)

        op_margin = self._rule_operating_margin(ordered_statements)
        op_margin.weight = self.WEIGHT_OP_MARGIN
        rule_results.append(op_margin)

        equity_ratio = self._rule_equity_ratio(ordered_statements)
        equity_ratio.weight = self.WEIGHT_EQUITY_RATIO
        rule_results.append(equity_ratio)

        roe = self._rule_roe(ordered_statements)
        roe.weight = self.WEIGHT_ROE
        rule_results.append(roe)

        momentum = self._rule_momentum(ordered_quotes, latest_close)
        momentum.weight = self.WEIGHT_MOMENTUM
        rule_results.append(momentum)

        total_weight = sum(r.weight for r in rule_results)
        weighted_passed = sum(r.weight for r in rule_results if r.passed)
        score = (weighted_passed / total_weight) if total_weight else 0.0

        if score >= 0.7:
            signal = "buy"
        elif score <= 0.3:
            signal = "sell"
        else:
            signal = "hold"

        return StockJudgment(
            code=code,
            name=name,
            strategy="fundamental",
            signal=signal,
            score=round(score, 4),
            price=latest_close,
            rule_results=rule_results,
            as_of=as_of,
        )

    def _rule_sales_cagr(self, statements: list[dict[str, Any]]) -> RuleResult:
        """売上3年CAGRが+5%以上かを判定する。"""
        points: list[tuple[date, float]] = []
        for st in statements:
            sales = self._get_net_sales(st)
            d = self._parse_date(st.get("DisclosedDate"))
            if sales and d and sales > 0:
                points.append((d, sales))

        if len(points) < 2:
            return RuleResult("売上CAGR", None, "3年CAGR >= +5%", False, "データなし")

        start_d, start_s = points[0]
        end_d, end_s = points[-1]
        years = (end_d - start_d).days / 365.25
        if years <= 0:
            return RuleResult("売上CAGR", None, "3年CAGR >= +5%", False, "データなし")

        cagr = (end_s / start_s) ** (1.0 / years) - 1.0
        passed = cagr >= self.SALES_CAGR_THRESHOLD
        return RuleResult(
            rule_name="売上CAGR",
            value=cagr * 100.0,
            threshold="3年CAGR >= +5%",
            passed=passed,
            reason=f"CAGR={cagr * 100:.2f}%",
        )

    def _rule_operating_margin(self, statements: list[dict[str, Any]]) -> RuleResult:
        """最新期の営業利益率が10%以上かを判定する。"""
        latest = statements[-1] if statements else None
        if not latest:
            return RuleResult("営業利益率", None, ">= 10%", False, "データなし")

        sales = self._get_net_sales(latest)
        op = self._to_float(latest.get("OperatingProfit"))
        if sales is None or op is None or sales == 0:
            return RuleResult("営業利益率", None, ">= 10%", False, "データなし")

        margin = op / sales
        passed = margin >= self.OPERATING_MARGIN_THRESHOLD
        return RuleResult(
            rule_name="営業利益率",
            value=margin * 100.0,
            threshold=">= 10%",
            passed=passed,
            reason=f"営業利益率={margin * 100:.2f}%",
        )

    def _rule_equity_ratio(self, statements: list[dict[str, Any]]) -> RuleResult:
        """最新期の自己資本比率が40%以上かを判定する。"""
        latest = statements[-1] if statements else None
        if not latest:
            return RuleResult("自己資本比率", None, ">= 40%", False, "データなし")

        equity = self._to_float(latest.get("Equity"))
        total_assets = self._to_float(latest.get("TotalAssets"))
        if equity is None or total_assets is None or total_assets == 0:
            return RuleResult("自己資本比率", None, ">= 40%", False, "データなし")

        ratio = equity / total_assets
        passed = ratio >= self.EQUITY_RATIO_THRESHOLD
        return RuleResult(
            rule_name="自己資本比率",
            value=ratio * 100.0,
            threshold=">= 40%",
            passed=passed,
            reason=f"自己資本比率={ratio * 100:.2f}%",
        )

    def _rule_roe(self, statements: list[dict[str, Any]]) -> RuleResult:
        """最新期のROEが8%以上かを判定する。"""
        latest = statements[-1] if statements else None
        if not latest:
            return RuleResult("ROE", None, ">= 8%", False, "データなし")

        net_income = self._to_float(latest.get("NetIncome"))
        equity = self._to_float(latest.get("Equity"))
        if net_income is None or equity is None or equity == 0:
            return RuleResult("ROE", None, ">= 8%", False, "データなし")

        roe = net_income / equity
        passed = roe >= self.ROE_THRESHOLD
        return RuleResult(
            rule_name="ROE",
            value=roe * 100.0,
            threshold=">= 8%",
            passed=passed,
            reason=f"ROE={roe * 100:.2f}%",
        )

    def _rule_momentum(
        self, quotes: list[dict[str, Any]], latest_close: float | None
    ) -> RuleResult:
        """直近終値が25日移動平均線より上かを判定する。"""
        closes = [self._to_float(q.get("Close")) for q in quotes]
        closes = [c for c in closes if c is not None]
        if latest_close is None or len(closes) < self.MA_WINDOW:
            return RuleResult("モメンタム(MA25)", None, "終値 > 25日MA", False, "データなし")

        ma25 = sum(closes[-self.MA_WINDOW:]) / self.MA_WINDOW
        passed = latest_close > ma25
        return RuleResult(
            rule_name="モメンタム(MA25)",
            value=latest_close - ma25,
            threshold="終値 > 25日MA",
            passed=passed,
            reason=f"終値={latest_close:.0f}, MA25={ma25:.0f}",
        )

    @staticmethod
    def _get_net_sales(st: dict[str, Any]) -> float | None:
        for key in ("NetSales", "NetSalesAmount", "Revenue"):
            v = FundamentalRuleEngine._to_float(st.get(key))
            if v is not None:
                return v
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
