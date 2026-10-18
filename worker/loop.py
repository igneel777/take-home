import asyncio

from shared.models import NotSent
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
                # Idle workers must still release rows a dead worker left in SENDING.
                await _release_stale(worker_db)
                await asyncio.sleep(IDLE_SLEEP_SECONDS)
                continue
            outcome = await provider.send(action)
            updated = await worker_db.update(action, outcome)
            print(f"action {action.id} attempt {action.attempt_n} -> {outcome} (saved={updated})")
            await _release_stale(worker_db)
            if isinstance(outcome, NotSent) and outcome.retry_after is not None:
                await asyncio.sleep(outcome.retry_after)
    finally:
        await provider.close()
        await worker_db.close()


async def _release_stale(worker_db: WorkerDB) -> None:
    released = await worker_db.release_stale()
    if released:
        print(f"released {released} stale SENDING rows")
