import math

from config import RATE_LIMIT_RPS, RATE_LIMIT_TOKENS
from worker.ratelimiter_db import RateLimiterDB


class RateLimiter:
    """Token bucket shared by every worker through the provider_stats row."""

    def __init__(self) -> None:
        self.db = RateLimiterDB()

    async def open(self) -> None:
        """Open the bucket's connection pool."""
        await self.db.open()

    async def close(self) -> None:
        """Close the bucket's connection pool."""
        await self.db.close()

    async def get_token(self) -> int:
        """Take one token from the shared bucket in provider_stats.

        Runs in its own transaction. Never call it while holding the provider_stats row lock:
        it would wait on that lock.

        Returns:
            0 when a token was taken and the request can go now. Otherwise the whole seconds to
            wait before trying again, because provider_stats.blocked_until is still ahead or
            the bucket is empty.
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

    async def block(self, retry_after: float | None) -> None:
        """Stop every worker taking tokens until the provider's Retry-After window closes.

        Call it on any 429. The provider's window is global, so one worker's 429 has to hold
        back the others too, or their requests land inside the window.

        Args:
            retry_after: The response's Retry-After in seconds; 1 when it had none.
        """
        seconds = retry_after if retry_after is not None else 1.0
        await self.db.save_blocked_until(math.ceil(seconds * 1000))
