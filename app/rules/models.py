from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(slots=True)
class RuleResult:
    """各ルールの判定結果。"""

    rule_name: str
    value: float | None
    threshold: str
    passed: bool
    reason: str
    weight: float = field(default=1.0)


@dataclass(slots=True)
class StockJudgment:
    """銘柄の最終判定結果。"""

    code: str
    name: str
    strategy: str
    signal: Literal["buy", "sell", "hold"]
    score: float
    price: float | None
    rule_results: list[RuleResult]
    as_of: str
