import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from config import DB_ROLE_CONFIG

DEFAULT_DATABASE_URL = "postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch"

# SQL expression for the current time in epoch milliseconds; clock_timestamp() so it is not
# frozen at the start of the transaction.
NOW_MS = "(extract(epoch FROM clock_timestamp()) * 1000)::bigint"


def database_url(role: str) -> str:
    """Return DATABASE_URL with its user and password set to `role`'s.

    Args:
        role: The role to log in as, a key of DB_ROLE_CONFIG.

    Returns:
        A libpq connection string for `role`.

    Raises:
        KeyError: `role` is not in DB_ROLE_CONFIG.
    """
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    return make_conninfo(url, user=role, password=DB_ROLE_CONFIG[role]["password"])


class DBConnector:
    """An async connection pool that logs in as one database role."""

    def __init__(self, role: str, min_size: int = 2, max_size: int = 10) -> None:
        """Create the pool, closed; call open() before use.

        Args:
            role: The role to log in as, a key of DB_ROLE_CONFIG.
            min_size: Connections the pool keeps open.
            max_size: Most connections the pool opens.
        """
        self.pool = AsyncConnectionPool(
            conninfo=database_url(role),
            min_size=min_size,
            max_size=max_size,
            timeout=5,
            kwargs={"row_factory": dict_row},
            open=False,
        )

    async def open(self) -> None:
        """Open the pool and wait until min_size connections are ready."""
        await self.pool.open(wait=True)

    async def close(self) -> None:
        """Close the pool and its connections."""
        await self.pool.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection[DictRow]]:
        """Lend a pooled connection for one transaction.

        The transaction commits when the block exits cleanly and rolls back on an exception.

        Yields:
            The connection, which returns rows as dicts.
        """
        async with self.pool.connection() as conn:
            yield conn
