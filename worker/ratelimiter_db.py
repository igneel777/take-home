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

Transaction = AsyncConnection[DictRow]


class RateLimiterDB:
    """Database operations for the rate limiter, on a connection pool as the ratelimiter role.

    Each method takes an optional `transaction`. When given, the method runs inside it and
    leaves committing to the caller; otherwise it runs in its own transaction and commits.
    """

    def __init__(self, min_size: int = 1, max_size: int = 4) -> None:
        self.db_connector = DBConnector(role="ratelimiter", min_size=min_size, max_size=max_size)

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

    async def lock_stats(self, transaction: Transaction | None = None) -> tuple[ProviderStats, int]:
        """Lock the provider_stats row and return it with the database's current epoch ms.

        Only useful inside a caller's `transaction`; on its own the lock ends at once.
        """
        async with self._use(transaction) as conn:
            cur = await conn.execute(LOCK_STATS)
            row = await cur.fetchone()
        return ProviderStats.model_validate(row), row["now"]

    async def save_bucket(
        self, tokens: float, now: int, transaction: Transaction | None = None
    ) -> None:
        """Store the bucket's tokens as of `now`."""
        async with self._use(transaction) as conn:
            await conn.execute(SAVE_BUCKET, {"tokens": tokens, "now": now})
