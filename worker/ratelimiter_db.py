from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.rows import DictRow

from shared.db_connector import NOW_MS, DBConnector
from shared.models import ProviderStats

# FOR UPDATE serialises rate limiters on the single row until the transaction ends.
LOCK_STATS = f"""
SELECT *, {NOW_MS} AS now
FROM provider_stats
FOR UPDATE
"""

SAVE_BUCKET = """
UPDATE provider_stats
SET tokens = %(tokens)s, last_added_at = %(now)s
"""

# GREATEST keeps a longer window another worker already saved.
SAVE_BLOCKED_UNTIL = f"""
UPDATE provider_stats
SET blocked_until = GREATEST(blocked_until, {NOW_MS} + %(wait_ms)s)
"""

Transaction = AsyncConnection[DictRow]


class RateLimiterDB:
    """Database operations for the rate limiter.

    They run on a connection pool as the ratelimiter role. Each method takes an optional
    `transaction`. When given, the method runs inside it and leaves committing to the caller;
    otherwise it runs in its own transaction and commits.
    """

    def __init__(self, min_size: int = 1, max_size: int = 4) -> None:
        """Create the pool, closed; call open() before use.

        Args:
            min_size: Connections the pool keeps open.
            max_size: Most connections the pool opens.
        """
        self.db_connector = DBConnector(role="ratelimiter", min_size=min_size, max_size=max_size)

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

    async def lock_stats(self, transaction: Transaction | None = None) -> tuple[ProviderStats, int]:
        """Lock and read the provider_stats row, with the database's time.

        Only useful inside a caller's `transaction`; on its own the lock ends at once.

        Args:
            transaction: The caller's transaction, which holds the lock until it ends.

        Returns:
            The row, and the database's current time in epoch ms.
        """
        async with self._use(transaction) as conn:
            cur = await conn.execute(LOCK_STATS)
            row = await cur.fetchone()
        return ProviderStats.model_validate(row), row["now"]

    async def save_bucket(
        self, tokens: float, now: int, transaction: Transaction | None = None
    ) -> None:
        """Store the bucket's tokens as of `now`.

        Args:
            tokens: Tokens left in the bucket.
            now: Epoch ms the tokens were counted at; saved as last_added_at.
            transaction: The caller's transaction, if any; see the class docstring.
        """
        async with self._use(transaction) as conn:
            await conn.execute(SAVE_BUCKET, {"tokens": tokens, "now": now})

    async def save_blocked_until(
        self, wait_ms: int, transaction: Transaction | None = None
    ) -> None:
        """Push provider_stats.blocked_until to at least `wait_ms` from now, never back.

        Args:
            wait_ms: Milliseconds from the database's current time to block for.
            transaction: The caller's transaction, if any; see the class docstring.
        """
        async with self._use(transaction) as conn:
            await conn.execute(SAVE_BLOCKED_UNTIL, {"wait_ms": wait_ms})
