"""スケジューラ単独プロセス起動エントリーポイント。

run.py（Webサーバ）とは別プロセスで起動することで、
多重実行事故を防ぎジョブ管理を明確化する。

  python run_scheduler.py
"""
from __future__ import annotations

import asyncio
import logging
import sys

from app.db import init_db
from app.scheduler import start_scheduler, stop_scheduler


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    async def _forever() -> None:
        init_db()
        start_scheduler()
        logging.info("Scheduler process started. Press Ctrl+C to stop.")
        try:
            while True:
                await asyncio.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            stop_scheduler()
            logging.info("Scheduler process stopped.")

    asyncio.run(_forever())


if __name__ == "__main__":
    main()
