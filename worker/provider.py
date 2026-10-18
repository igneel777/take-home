import os
from dataclasses import dataclass

import httpx

from shared.models import Action
from worker.ratelimiter import RateLimiter

DEFAULT_PROVIDER_URL = "http://127.0.0.1:8080"
DEFAULT_TIMEOUT_SECONDS = 5.0


def provider_url() -> str:
    return os.environ.get("PROVIDER_URL", DEFAULT_PROVIDER_URL).rstrip("/")


@dataclass(frozen=True)
class Sent:
    receipt_id: str


@dataclass(frozen=True)
class NotSent:
    """No receipt came back. After a timeout or a dropped connection the provider may still
    have sent it, so this is not proof that nothing went out."""

    reason: str
    retry_after: float | None = None


Outcome = Sent | NotSent


class ProviderClient:
    """HTTP client for the provider, on a pool of one keep-alive connection.

    Every send takes a token from the shared RateLimiter first. Call open() before sending.
    """

    def __init__(self, base_url: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.base_url = base_url
        self.client = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )
        self.rate_limiter = RateLimiter()

    async def open(self) -> None:
        await self.rate_limiter.open()

    async def send(self, action: Action) -> Outcome:
        payload = {
            "action_id": str(action.id),
            "recipient": action.recipient,
            "body": action.body,
        }
        wait = await self.rate_limiter.get_token()
        if wait > 0:
            # Nothing went on the wire, so this one is definitely not sent.
            return NotSent("rate limited", retry_after=float(wait))
        try:
            response = await self.client.post("/send", json=payload)
            response.raise_for_status()
            return Sent(response.json()["receipt_id"])
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            return NotSent(f"http {status}", _retry_after(e.response))
        except httpx.TimeoutException as e:
            return NotSent(f"timeout: {type(e).__name__}")
        except httpx.HTTPError as e:
            return NotSent(f"transport error: {type(e).__name__}")
        except (ValueError, KeyError) as e:
            return NotSent(f"bad response body: {type(e).__name__}")

    async def close(self) -> None:
        await self.client.aclose()
        await self.rate_limiter.close()


def _retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None
