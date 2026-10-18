import math

from config import RATE_LIMIT_RPS, RATE_LIMIT_TOKENS
from worker.ratelimiter_db import RateLimiterDB


class RateLimiter:
    """Token bucket shared by every worker through the single provider_stats row."""

    def __init__(self) -> None:
        self.db = RateLimiterDB()

    async def open(self) -> None:
        await self.db.open()

    async def close(self) -> None:
        await self.db.close()

    async def get_token(self) -> int:
        """Take one token from the shared bucket in provider_stats.

        Returns 0 when a token was taken and the request can go now, or the whole seconds to
        wait before trying again when a Retry-After window is open or the bucket is empty.
        """
        async with self.db.transaction() as conn:
            stats, now = await self.db.lock_stats(conn)
            if stats.blocked_until > now:
                return math.ceil((stats.blocked_until - now) / 1000)

            elapsed = (now - stats.last_added_at) / 1000
            tokens = min(float(RATE_LIMIT_TOKENS), stats.tokens + elapsed * RATE_LIMIT_RPS)
            wait = 0
            if tokens >= 1.0:
                tokens -= 1.0
            else:
                wait = math.ceil((1.0 - tokens) / RATE_LIMIT_RPS)
            await self.db.save_bucket(tokens, now, conn)
            return wait
