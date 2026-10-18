from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from psycopg import AsyncConnection, errors
from psycopg.rows import DictRow

from shared.db_connector import NOW_MS, DBConnector
from shared.models import Action, NotSent, Receipt, Sent

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

# Receipt ids are unique, so a page read twice inserts nothing new. Receipts for action ids
# that are not in actions are skipped rather than failing the foreign key.
INSERT_RECEIPT = """
INSERT INTO receipts (receipt_id, action_id, recipient, sent_at)
SELECT %(receipt_id)s, %(action_id)s, %(recipient)s, %(sent_at)s
WHERE EXISTS (SELECT 1 FROM actions WHERE id = %(action_id)s)
ON CONFLICT (receipt_id) DO NOTHING
"""

# If an action was ever sent twice, its first receipt is the one we keep.
FIND_RECEIPT = """
SELECT * FROM receipts
WHERE action_id = %(action_id)s
ORDER BY sent_at
LIMIT 1
"""

GET_SENT_CURSOR = "SELECT sent_cursor FROM provider_stats"

# GREATEST keeps the cursor from moving back if two syncs ever overlap.
SAVE_SENT_CURSOR = """
UPDATE provider_stats
SET sent_cursor = GREATEST(sent_cursor, %(cursor)s)
"""

# Key of the advisory lock that lets one worker at a time sync /sent.
SYNC_LOCK_KEY = 1

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

    async def insert_receipts(
        self, receipts: list[Receipt], transaction: Transaction | None = None
    ) -> None:
        if not receipts:
            return
        async with self._use(transaction) as conn, conn.cursor() as cur:
            await cur.executemany(INSERT_RECEIPT, [r.model_dump() for r in receipts])

    async def find_receipt(
        self, action_id: UUID, transaction: Transaction | None = None
    ) -> Receipt | None:
        async with self._use(transaction) as conn:
            cur = await conn.execute(FIND_RECEIPT, {"action_id": action_id})
            row = await cur.fetchone()
        return Receipt.model_validate(row) if row is not None else None

    async def get_sent_cursor(self, transaction: Transaction | None = None) -> int:
        async with self._use(transaction) as conn:
            cur = await conn.execute(GET_SENT_CURSOR)
            row = await cur.fetchone()
        return row["sent_cursor"]

    async def save_sent_cursor(self, cursor: int, transaction: Transaction | None = None) -> None:
        async with self._use(transaction) as conn:
            await conn.execute(SAVE_SENT_CURSOR, {"cursor": cursor})

    @asynccontextmanager
    async def sync_lock(self) -> AsyncIterator[Transaction | None]:
        """Hold the session-level advisory lock for syncing /sent, waiting for it if needed.

        Yields the connection holding the lock, or None when the wait ran out (the worker
        role's 5s lock_timeout and statement_timeout). The connection is in autocommit so no
        transaction stays open across HTTP calls; group statements on it with
        `conn.transaction()`. If the process dies, Postgres releases the lock when the
        connection closes.
        """
        async with self.db_connector.pool.connection() as conn:
            await conn.set_autocommit(True)
            try:
                try:
                    await conn.execute("SELECT pg_advisory_lock(%s)", (SYNC_LOCK_KEY,))
                except (errors.LockNotAvailable, errors.QueryCanceled):
                    yield None
                    return
                try:
                    yield conn
                finally:
                    await conn.execute("SELECT pg_advisory_unlock(%s)", (SYNC_LOCK_KEY,))
            finally:
                await conn.set_autocommit(False)
