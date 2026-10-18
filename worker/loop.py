import asyncio
import logging

from worker.provider import ProviderClient, provider_url
from worker.worker_db import WorkerDB

IDLE_SLEEP_SECONDS = 5

log = logging.getLogger(__name__)


async def run() -> None:
    provider = ProviderClient(provider_url())
    worker_db = WorkerDB()
    await provider.open()
    await worker_db.open()
    try:
        while True:
            action = await worker_db.pick_one()
            if action is None:
                await asyncio.sleep(IDLE_SLEEP_SECONDS)
                continue
            outcome = await provider.send(action)
            # TODO: decide what to do with the outcome (worker_db.update, Retry-After, /sent).
            log.info("action %s attempt %d -> %s", action.id, action.attempt_n, outcome)
    finally:
        await provider.close()
        await worker_db.close()
