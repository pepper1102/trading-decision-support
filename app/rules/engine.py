from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from .dividend import DividendRuleEngine
from .fundamental import FundamentalRuleEngine
from .models import StockJudgment
from .swing import SwingRuleEngine


class RulesOrchestrator:
    """3戦略のルール判定を統合するオーケストレーター。"""

    def __init__(self) -> None:
        self.swing = SwingRuleEngine()
        self.fundamental = FundamentalRuleEngine()
        self.dividend = DividendRuleEngine()

    async def evaluate_all(
        self,
        code: str,
        name: str,
        quotes: list[dict[str, Any]],
        statements: list[dict[str, Any]],
        dividend_data: list[dict[str, Any]],
        announcements: list[dict[str, Any]],
    ) -> dict[str, StockJudgment]:
        """
        3戦略すべてを評価して結果を返す。

        Returns:
            {'swing': ..., 'fundamental': ..., 'dividend': ...} の辞書
        """
        return {
            "swing": self.swing.evaluate(code, name, quotes, announcements),
            "fundamental": self.fundamental.evaluate(code, name, quotes, statements),
            "dividend": self.dividend.evaluate(code, name, quotes, statements, dividend_data),
        }

    @staticmethod
    def date_range(days: int = 100) -> tuple[str, str]:
        """今日からdays日前〜今日の日付範囲を返す。"""
        today = date.today()
        date_from = today - timedelta(days=days)
        return date_from.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")
