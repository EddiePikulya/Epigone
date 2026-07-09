"""Stream process skeleton: the tracked-wallet poller arrives with issue #4."""

import asyncio
import logging

from epigone.config import Settings
from epigone.db import apply_schema, create_pool

HEARTBEAT_SECONDS = 30


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger(__name__)
    settings = Settings.from_env()
    pool = await create_pool(settings.database_url)
    await apply_schema(pool)
    while True:
        log.info("stream: heartbeat (no poller implemented yet)")
        await asyncio.sleep(HEARTBEAT_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
