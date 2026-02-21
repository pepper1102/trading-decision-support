from pydantic_settings import BaseSettings
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent


class Settings(BaseSettings):
    edinet_api_key: str = ""

    class Config:
        env_file = str(BASE_DIR / ".env")


settings = Settings()


def load_watchlist() -> list[str]:
    """watchlist.txtから銘柄コード一覧を読み込む。
    - 行末コメント（#以降）を除去
    - .T サフィックスを除去して4桁コードに正規化
    - 重複排除（出現順を維持）
    """
    path = BASE_DIR / "watchlist.txt"
    if not path.exists():
        return []
    seen: set[str] = set()
    codes: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip().replace(".T", "")
        if line and line not in seen:
            seen.add(line)
            codes.append(line)
    return codes
