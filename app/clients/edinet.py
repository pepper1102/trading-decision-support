from __future__ import annotations

import logging
import re
from typing import Any

import requests

EDINETDB_BASE = "https://edinetdb.jp/v1"


def _clean_code(code: str) -> str:
    """watchlist コード（例: "7203.T"）から API 用コードを生成する。"""
    return str(code).replace(".T", "").strip()


def _is_edinet_code(code: str) -> bool:
    """E02144 形式の EDINET コードかどうかを判定する。"""
    return bool(re.fullmatch(r"E\d{5}", str(code).strip().upper()))


class EdinetDbClient:
    """EDINET DB API クライアント（edinetdb.jp）。

    認証は X-API-Key ヘッダーで行う。
    エンドポイントは証券コードではなく EDINETコード（E02144等）を要求するため、
    resolve_edinet_code() で事前に変換してからアクセスする。
    """

    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._session.headers.update({"X-API-Key": api_key})
        # 証券コード → EDINETコード のキャッシュ（バッチ内重複呼び出し削減）
        self._edinet_code_cache: dict[str, str] = {}

    def close(self) -> None:
        self._session.close()

    def _get(self, path: str, **params: Any) -> Any:
        resp = self._session.get(
            f"{EDINETDB_BASE}{path}",
            params=params or None,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    # ──────────────────────────────────────────────
    # レスポンスキー揺れ吸収ヘルパー
    # ──────────────────────────────────────────────

    def _extract_items(self, result: Any) -> list[dict[str, Any]]:
        """リスト or ネスト dict から要素リストを取り出す。"""
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
        if isinstance(result, dict):
            for key in ("data", "results", "companies", "items"):
                v = result.get(key)
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
        return []

    def _pick_edinet_code(self, item: dict[str, Any]) -> str | None:
        """レスポンス dict から EDINETコードを取り出す。"""
        for key in ("edinet_code", "edinetCode", "edinetcode"):
            v = item.get(key)
            if isinstance(v, str) and _is_edinet_code(v):
                return v.upper()
        # "code" が EDINET コードの場合もある
        v = item.get("code")
        if isinstance(v, str) and _is_edinet_code(v):
            return v.upper()
        return None

    def _pick_security_code(self, item: dict[str, Any]) -> str | None:
        """レスポンス dict から証券コードを取り出す。

        EDINET DB は sec_code を「72030」のように末尾に 0 を付けた5桁で返すため、
        末尾が「0」かつ4桁になる場合はそれを除去して4桁コードに正規化する。
        """
        for key in ("security_code", "securities_code", "securitiesCode", "secCode", "sec_code", "ticker"):
            v = item.get(key)
            if v is not None:
                s = _clean_code(str(v))
                if s:
                    # 5桁末尾0 → 4桁に正規化（例: "72030" → "7203"）
                    if len(s) == 5 and s.endswith("0") and s[:4].isdigit():
                        s = s[:4]
                    return s
        return None

    # ──────────────────────────────────────────────
    # 証券コード → EDINET コード 変換
    # ──────────────────────────────────────────────

    def resolve_edinet_code(self, security_code: str) -> str | None:
        """証券コードを EDINET コードに変換する。

        1) /v1/search?q=証券コード で検索
        2) 失敗時は /v1/companies を全ページ走査（フォールバック）
        結果はキャッシュして同一バッチ内の重複 API 呼び出しを削減する。
        """
        raw = _clean_code(security_code)
        if not raw:
            return None

        # すでに EDINET コード形式であればそのまま返す
        if _is_edinet_code(raw):
            return raw.upper()

        if raw in self._edinet_code_cache:
            return self._edinet_code_cache[raw]

        # 1) /v1/search で解決
        try:
            result = self._get("/search", q=raw)
            for item in self._extract_items(result):
                sec = self._pick_security_code(item)
                if sec == raw:
                    edinet = self._pick_edinet_code(item)
                    if edinet:
                        self._edinet_code_cache[raw] = edinet
                        return edinet
        except Exception as exc:
            logging.warning("EdinetDB: search failed [%s]: %s", raw, exc)

        # 2) /v1/companies をフォールバック（ページング）
        try:
            page = 1
            while page <= 20:
                result = self._get("/companies", page=page, per_page=100)
                items = self._extract_items(result)
                if not items:
                    break

                for item in items:
                    sec = self._pick_security_code(item)
                    if sec == raw:
                        edinet = self._pick_edinet_code(item)
                        if edinet:
                            self._edinet_code_cache[raw] = edinet
                            return edinet

                # ページネーション: キー揺れ吸収
                found_next = False
                if isinstance(result, dict):
                    next_page = result.get("next_page")
                    has_next = result.get("has_next")
                    if isinstance(next_page, int) and next_page > page:
                        page = next_page
                        found_next = True
                    elif has_next is True:
                        page += 1
                        found_next = True
                if not found_next:
                    page += 1
        except Exception as exc:
            logging.warning("EdinetDB: companies fallback failed [%s]: %s", raw, exc)

        return None

    # ──────────────────────────────────────────────
    # API メソッド
    # ──────────────────────────────────────────────

    def get_company(self, code: str) -> dict[str, Any] | None:
        """企業情報を取得する。失敗した場合は None を返す。"""
        try:
            edinet_code = self.resolve_edinet_code(code)
            if not edinet_code:
                return None
            return self._get(f"/companies/{edinet_code}")
        except Exception as exc:
            logging.warning("EdinetDB: get_company failed [%s]: %s", code, exc)
            return None

    def get_financials(self, code: str) -> list[dict[str, Any]]:
        """財務時系列データを取得する。失敗した場合は空リストを返す。"""
        try:
            edinet_code = self.resolve_edinet_code(code)
            if not edinet_code:
                logging.warning("EdinetDB: EDINET code not found for [%s]", code)
                return []

            result = self._get(f"/companies/{edinet_code}/financials")
            if isinstance(result, list):
                return result
            if isinstance(result, dict):
                for key in ("data", "financials", "results"):
                    if isinstance(result.get(key), list):
                        return result[key]
        except Exception as exc:
            logging.warning("EdinetDB: get_financials failed [%s]: %s", code, exc)
        return []


# ──────────────────────────────────────────────
# statements 形式への変換
# ──────────────────────────────────────────────

def to_statements(financials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """EdinetDB の財務データを batch.py の statements 形式に変換する。"""
    result = []
    for f in financials:
        disclosed_date = _to_date(f.get("fiscal_year"))
        if not disclosed_date:
            continue
        result.append({
            "DisclosedDate": disclosed_date,
            "NetSales": f.get("revenue"),
            "OperatingProfit": f.get("operating_income"),
            "NetIncome": f.get("net_income"),
            "TotalAssets": f.get("total_assets"),
            "Equity": f.get("net_assets"),
            "EarningsPerShare": f.get("eps"),
            "_source": "edinetdb",
        })
    return result


def _to_date(fiscal_year: Any) -> str | None:
    """fiscal_year 値を YYYY-MM-DD 形式に変換する。"""
    if fiscal_year is None:
        return None
    s = str(fiscal_year).strip()
    if len(s) == 10 and s[4] == "-":   # YYYY-MM-DD
        return s
    if len(s) == 7 and s[4] == "-":    # YYYY-MM
        return s + "-31"
    if len(s) == 4 and s.isdigit():    # YYYY（年度）→ 翌年3月末
        return f"{int(s) + 1}-03-31"
    return s if s else None
