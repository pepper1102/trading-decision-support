"""センチメント分析モジュール。

現在サポートするモード:
  rule   - キーワードベース（デフォルト、外部依存なし）
  hybrid - キーワード + MLモデル（将来対応。現状は rule にフォールバック）
  model  - MLモデル単独（将来対応。現状は rule にフォールバック）

.env に SENTIMENT_MODE=hybrid と設定するだけでモードを切り替えられる。
"""
from __future__ import annotations

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


def score_rule(title: str, summary: str) -> float:
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


def score_hybrid(
    title: str,
    summary: str,
    mode: str = "rule",
) -> dict[str, object]:
    """センチメントスコアとメタデータを返す。

    戻り値:
        score       : float  (-1.0〜+1.0)
        method      : str    ("rule" | "rule_fallback")
        model_version: str | None
        confidence  : float | None
    """
    if mode in ("hybrid", "model"):
        # 将来: transformers モデルを呼び出す
        # 現状はルールにフォールバック
        score = score_rule(title, summary)
        return {
            "score": score,
            "method": "rule_fallback",
            "model_version": None,
            "confidence": None,
        }

    score = score_rule(title, summary)
    return {
        "score": score,
        "method": "rule",
        "model_version": None,
        "confidence": None,
    }
