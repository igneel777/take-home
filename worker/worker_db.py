from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from psycopg import AsyncConnection, errors
from psycopg.rows import DictRow

from config import STALE_AFTER_MS, SYNC_LOCK_KEY, WORKER_FAILED_REASON
from shared.db_connector import NOW_MS, DBConnector
from shared.models import Action, NotSent, Receipt, Sent

MAX_ATTEMPTS = 5

# SKIP LOCKED lets concurrent workers pass over a row another worker is claiming.
PICK_ONE = f"""
UPDATE actions
SET status = 'SENDING', attempt_started_at = {NOW_MS}
WHERE id = (
    SELECT id FROM actions
    WHERE status = 'PENDING'
    ORDER BY created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING *
"""

# Every update is guarded by attempt_started_at, which pick_one sets, so it applies only to
# the pick this worker made.
CLAIMED = """
WHERE id = %(id)s AND status = 'SENDING' AND attempt_started_at = %(attempt_started_at)s
"""

MARK_SENT = f"""
UPDATE actions
SET status = 'SENT', receipt_id = %(receipt_id)s, sent_at = {NOW_MS}, failure_reason = NULL
{CLAIMED}
"""

# Rate limited before the request could go out: back to PENDING without using an attempt.
MARK_WAITED = f"""
UPDATE actions
SET status = 'PENDING'
{CLAIMED}
"""

# A failed attempt: count it, and give up once MAX_ATTEMPTS are used. The right-hand sides
# all read the row as it was before this UPDATE.
MARK_FAILED_ATTEMPT = f"""
UPDATE actions
SET attempt_n = attempt_n + 1,
    status = CASE WHEN attempt_n + 1 >= %(max_attempts)s
                  THEN 'FAILED'::action_status ELSE 'PENDING'::action_status END,
    failure_reason = %(reason)s
{CLAIMED}
"""

# Counts the dead worker's pick as a failed attempt. It may have reached the provider, which is
# safe: with attempt_n above 0, send checks /sent before sending again. SKIP LOCKED passes over
# rows another transaction is updating right now.
RELEASE_STALE = f"""
UPDATE actions
SET attempt_n = attempt_n + 1,
    status = CASE WHEN attempt_n + 1 >= %(max_attempts)s
                  THEN 'FAILED'::action_status ELSE 'PENDING'::action_status END,
    failure_reason = %(reason)s
WHERE id IN (
    SELECT id FROM actions
    WHERE status = 'SENDING' AND attempt_started_at < {NOW_MS} - %(stale_after_ms)s
    FOR UPDATE SKIP LOCKED
)
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

Transaction = AsyncConnection[DictRow]


class WorkerDB:
    """Database operations for the worker.

    They run on a connection pool as the worker role. Each method takes an optional `transaction`.
    When given, the method runs inside it and leaves committing to the caller; otherwise it runs in
    its own transaction and commits.
    """

    def __init__(self, min_size: int = 2, max_size: int = 10) -> None:
        """Create the pool, closed; call open() before use.

        Args:
            min_size: Connections the pool keeps open.
            max_size: Most connections the pool opens.
        """
        self.db_connector = DBConnector(role="worker", min_size=min_size, max_size=max_size)

    async def open(self) -> None:
        """Open the connection pool."""
        await self.db_connector.open()

    async def close(self) -> None:
        """Close the connection pool."""
        await self.db_connector.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Transaction]:
        """Lend a pooled connection for one transaction.

        The transaction commits when the block exits cleanly and rolls back on an exception.

        Yields:
            The connection.
        """
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
        """Claim the oldest PENDING action as SENDING.

        Args:
            transaction: The caller's transaction, if any; see the class docstring.

        Returns:
            The claimed action, or None when no PENDING action is free to claim.
        """
        async with self._use(transaction) as conn:
            cur = await conn.execute(PICK_ONE)
            row = await cur.fetchone()
        return Action.model_validate(row) if row is not None else None

    async def update(
        self, action: Action, outcome: Sent | NotSent, transaction: Transaction | None = None
    ) -> bool:
        """Record the outcome of the pick in `action`.

        Sent: SENT with its receipt. NotSent with a wait: back to PENDING, attempt_n unchanged,
        since the request never went out. Any other NotSent: attempt_n goes up by one, and the
        row goes back to PENDING, or to FAILED once MAX_ATTEMPTS attempts are used.

        Args:
            action: The action as pick_one returned it.
            outcome: What sending it gave.
            transaction: The caller's transaction, if any; see the class docstring.

        Returns:
            True when the row was updated, False when it is no longer held by this pick.
        """
        params: dict[str, object] = {
            "id": action.id,
            "attempt_started_at": action.attempt_started_at,
        }
        if isinstance(outcome, Sent):
            query, params["receipt_id"] = MARK_SENT, outcome.receipt_id
        elif outcome.retry_after is not None:
            query = MARK_WAITED
        else:
            query, params["reason"] = MARK_FAILED_ATTEMPT, outcome.reason
            params["max_attempts"] = MAX_ATTEMPTS
        async with self._use(transaction) as conn:
            cur = await conn.execute(query, params)
            return cur.rowcount == 1

    async def release_stale(self, transaction: Transaction | None = None) -> int:
        """Release SENDING rows picked more than STALE_AFTER_MS ago.

        Each counts as a failed attempt: back to PENDING, or FAILED once MAX_ATTEMPTS are used.

        Args:
            transaction: The caller's transaction, if any; see the class docstring.

        Returns:
            How many rows were released.
        """
        params = {
            "max_attempts": MAX_ATTEMPTS,
            "reason": WORKER_FAILED_REASON,
            "stale_after_ms": STALE_AFTER_MS,
        }
        async with self._use(transaction) as conn:
            cur = await conn.execute(RELEASE_STALE, params)
            return cur.rowcount

    async def insert_receipts(
        self, receipts: list[Receipt], transaction: Transaction | None = None
    ) -> None:
        """Store receipts, skipping known ones and ones for unknown actions.

        Args:
            receipts: Receipts from one page of /sent.
            transaction: The caller's transaction, if any; see the class docstring.
        """
        if not receipts:
            return
        async with self._use(transaction) as conn, conn.cursor() as cur:
            await cur.executemany(INSERT_RECEIPT, [r.model_dump() for r in receipts])

    async def find_receipt(
        self, action_id: UUID, transaction: Transaction | None = None
    ) -> Receipt | None:
        """Find an action's receipt, the earliest if it was sent more than once.

        Args:
            action_id: The action's id.
            transaction: The caller's transaction, if any; see the class docstring.

        Returns:
            The receipt, or None when receipts has none for `action_id`.
        """
        async with self._use(transaction) as conn:
            cur = await conn.execute(FIND_RECEIPT, {"action_id": action_id})
            row = await cur.fetchone()
        return Receipt.model_validate(row) if row is not None else None

    async def get_sent_cursor(self, transaction: Transaction | None = None) -> int:
        """Read how far /sent has been synced into receipts.

        Args:
            transaction: The caller's transaction, if any; see the class docstring.

        Returns:
            provider_stats.sent_cursor, the `since` to ask /sent for next.
        """
        async with self._use(transaction) as conn:
            cur = await conn.execute(GET_SENT_CURSOR)
            row = await cur.fetchone()
        return row["sent_cursor"]

    async def save_sent_cursor(self, cursor: int, transaction: Transaction | None = None) -> None:
        """Move provider_stats.sent_cursor forward to `cursor`, never back.

        Args:
            cursor: The `next` of the page just saved.
            transaction: The caller's transaction, if any; see the class docstring.
        """
        async with self._use(transaction) as conn:
            await conn.execute(SAVE_SENT_CURSOR, {"cursor": cursor})

    @asynccontextmanager
    async def sync_lock(self) -> AsyncIterator[Transaction | None]:
        """Hold the advisory lock for syncing /sent, waiting for it if needed.

        The lock is session-level. The connection is in autocommit so no transaction stays
        open across HTTP calls; group statements on it with `conn.transaction()`. If the
        process dies, Postgres releases the lock when the connection closes.

        Yields:
            The connection holding the lock, or None when the wait ran out at the worker role's
            lock_timeout or statement_timeout, set in DB_ROLE_CONFIG.
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
