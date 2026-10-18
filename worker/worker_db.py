from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from shared.db_connector import NOW_MS, DBConnector
from shared.models import Action
from worker.provider import NotSent, Sent

MAX_ATTEMPTS = 5

# SKIP LOCKED lets concurrent workers pass over a row another worker is claiming.
PICK_ONE = f"""
UPDATE actions
SET status = 'SENDING', attempt_n = attempt_n + 1, attempt_started_at = {NOW_MS}
WHERE id = (
    SELECT id FROM actions
    WHERE status = 'PENDING'
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *
"""

# Every update is guarded by attempt_n, so it applies only to the attempt this worker picked.
MARK_SENT = f"""
UPDATE actions
SET status = 'SENT', receipt_id = %(receipt_id)s, sent_at = {NOW_MS}, failure_reason = NULL
WHERE id = %(id)s AND attempt_n = %(attempt_n)s AND status = 'SENDING'
"""

MARK_NOT_SENT = """
UPDATE actions
SET status = %(status)s, failure_reason = %(reason)s
WHERE id = %(id)s AND attempt_n = %(attempt_n)s AND status = 'SENDING'
"""

Transaction = AsyncConnection[DictRow]


class WorkerDB:
    """Database operations for the worker, on a connection pool as the worker role.

    Each method takes an optional `transaction`. When given, the method runs inside it and
    leaves committing to the caller; otherwise it runs in its own transaction and commits.
    """

    def __init__(self, min_size: int = 2, max_size: int = 10) -> None:
        self.db_connector = DBConnector(role="worker", min_size=min_size, max_size=max_size)

    async def open(self) -> None:
        await self.db_connector.open()

    async def close(self) -> None:
        await self.db_connector.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        async with self.db_connector.transaction() as conn:
            yield conn

    @asynccontextmanager
    async def _use(self, transaction: Transaction | None) -> AsyncIterator[Transaction]:
        if transaction is not None:
            yield transaction
        else:
            async with self.transaction() as conn:
                yield conn

    async def pick_one(self, transaction: Transaction | None = None) -> Action | None:
        """Claim the oldest PENDING action as SENDING and return it."""
        async with self._use(transaction) as conn:
            cur = await conn.execute(PICK_ONE)
            row = await cur.fetchone()
        return Action.model_validate(row) if row is not None else None

    async def update(
        self, action: Action, outcome: Sent | NotSent, transaction: Transaction | None = None
    ) -> bool:
        """Record the outcome of the attempt in `action`.

        A failure goes back to PENDING, or to FAILED once MAX_ATTEMPTS attempts are used.
        Returns False when the row no longer carries this attempt.
        """
        params: dict[str, object] = {"id": action.id, "attempt_n": action.attempt_n}
        if isinstance(outcome, Sent):
            query, params["receipt_id"] = MARK_SENT, outcome.receipt_id
        else:
            query = MARK_NOT_SENT
            params["status"] = "FAILED" if action.attempt_n >= MAX_ATTEMPTS else "PENDING"
            params["reason"] = outcome.reason
        async with self._use(transaction) as conn:
            cur = await conn.execute(query, params)
            return cur.rowcount == 1
