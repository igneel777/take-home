import asyncio

from worker.provider import ProviderClient
from worker.worker_db import WorkerDB

IDLE_SLEEP_SECONDS = 5


async def run() -> None:
    worker_db = WorkerDB()
    provider = ProviderClient(worker_db)
    await worker_db.open()
    await provider.open()
    try:
        while True:
            action = await worker_db.pick_one()
            if action is None:
                await asyncio.sleep(IDLE_SLEEP_SECONDS)
                continue
            outcome = await provider.send(action)
            # TODO: decide what to do with the outcome (worker_db.update, Retry-After, /sent).
            print(f"action {action.id} attempt {action.attempt_n} -> {outcome}")
    finally:
        await provider.close()
        await worker_db.close()
