import asyncio
import traceback

from shared.models import Action, NotSent
from worker.provider import ProviderClient
from worker.worker_db import WorkerDB

IDLE_SLEEP_SECONDS = 5
# After an error, wait this long before the next iteration, doubling up to the max.
MIN_BACKOFF_SECONDS = 0.5
MAX_BACKOFF_SECONDS = 5.0


async def run() -> None:
    """Send actions one at a time until the process is stopped.

    Each pick is sent and its outcome saved, then rows that dead workers left in SENDING are
    released. Sleeps IDLE_SLEEP_SECONDS when nothing is PENDING, and retry_after when rate
    limited. An error in an iteration is logged and the loop carries on after a backoff, so a
    database or network blip does not take the worker down.
    """
    worker_db = WorkerDB()
    provider = ProviderClient(worker_db)
    await worker_db.open()
    await provider.open()
    backoff = MIN_BACKOFF_SECONDS
    try:
        while True:
            # The pick whose outcome is not saved yet, so an error knows what to give up.
            action = None
            try:
                action = await worker_db.pick_one()
                if action is None:
                    # Idle workers must still release rows a dead worker left in SENDING.
                    await _release_stale(worker_db)
                    await asyncio.sleep(IDLE_SLEEP_SECONDS)
                else:
                    outcome = await provider.send(action)
                    updated = await worker_db.update(action, outcome)
                    print(
                        f"action {action.id} attempt {action.attempt_n} -> {outcome}"
                        f" (saved={updated})"
                    )
                    action = None
                    await _release_stale(worker_db)
                    if isinstance(outcome, NotSent) and outcome.retry_after is not None:
                        await asyncio.sleep(outcome.retry_after)
            except Exception as e:  # not BaseException: cancellation and Ctrl-C still stop us
                traceback.print_exc()
                if action is not None:
                    await _abandon(worker_db, action, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            else:
                backoff = MIN_BACKOFF_SECONDS
    finally:
        await provider.close()
        await worker_db.close()


async def _release_stale(worker_db: WorkerDB) -> None:
    released = await worker_db.release_stale()
    if released:
        print(f"released {released} stale SENDING rows")


async def _abandon(worker_db: WorkerDB, action: Action, error: Exception) -> None:
    """Count `action`'s pick as a failed attempt after an error left its outcome unsaved.

    The send may already have gone out, so the row must not go back to PENDING with attempt_n
    unchanged: the next pick would skip the /sent check and send it again. If this fails too,
    release_stale counts the attempt once the pick is stale.

    Args:
        action: The action as pick_one returned it.
        error: The error that ended the iteration, recorded as the failure reason.
    """
    try:
        await worker_db.update(action, NotSent(reason=f"worker error: {type(error).__name__}"))
    except Exception:
        traceback.print_exc()
