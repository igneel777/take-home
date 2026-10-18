import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg import AsyncConnection
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

DEFAULT_DATABASE_URL = "postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch"

# SQL expression for the current time in epoch milliseconds; clock_timestamp() so it is not
# frozen at the start of the transaction.
NOW_MS = "(extract(epoch FROM clock_timestamp()) * 1000)::bigint"


def database_url(role: str = "worker") -> str:
    """DATABASE_URL with its user swapped for `role`.

    The password is kept, so `role` must share the URL user's password.
    """
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    return make_conninfo(url, user=role)


class DBConnector:
    def __init__(self, role: str = "worker", min_size: int = 2, max_size: int = 10) -> None:
        self.pool = AsyncConnectionPool(
            conninfo=database_url(role),
            min_size=min_size,
            max_size=max_size,
            timeout=5,
            kwargs={"row_factory": dict_row},
            open=False,
        )

    async def open(self) -> None:
        await self.pool.open(wait=True)

    async def close(self) -> None:
        await self.pool.close()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection[DictRow]]:
        # The pool commits when the block exits cleanly and rolls back on an exception.
        async with self.pool.connection() as conn:
            yield conn
