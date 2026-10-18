import math
import os

import httpx

from shared.models import Action, NotSent, Outcome, Receipt, Sent, SyncResult
from worker.ratelimiter import RateLimiter
from worker.worker_db import WorkerDB

DEFAULT_PROVIDER_URL = "http://127.0.0.1:8080"
DEFAULT_TIMEOUT_SECONDS = 5.0
SENT_PAGE_SIZE = 100  # the most /sent returns per page


def provider_url() -> str:
    return os.environ.get("PROVIDER_URL", DEFAULT_PROVIDER_URL).rstrip("/")


class ProviderClient:
    """HTTP client for the provider, on a pool of one keep-alive connection.

    Every send takes a token from the shared RateLimiter first. Call open() before sending.
    `worker_db` is used to sync /sent into receipts; its owner opens and closes it.
    """

    def __init__(self, worker_db: WorkerDB, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.base_url = provider_url()
        self.worker_db = worker_db
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )
        self.rate_limiter = RateLimiter()

    async def open(self) -> None:
        await self.rate_limiter.open()

    async def send(self, action: Action) -> Outcome:
        """Send `action` unless an earlier attempt already reached the provider.

        attempt_n counts failed attempts, so at 0 nothing can have gone out and it is sent
        straight away. Otherwise /sent is synced to the end first, and an existing receipt is
        returned as Sent. Being rate limited, by our bucket or by a 429 on /send or /sent,
        returns NotSent with the seconds to wait.
        """
        if action.attempt_n > 0:
            while not (result := await self.sync_one_page()).done:
                if result.wait > 0:
                    return NotSent(reason="rate limited syncing /sent", retry_after=result.wait)
            receipt = await self.worker_db.find_receipt(action.id)
            if receipt is not None:
                return Sent(receipt_id=receipt.receipt_id)
        return await self._post(action)

    async def _post(self, action: Action) -> Outcome:
        payload = {
            "action_id": str(action.id),
            "recipient": action.recipient,
            "body": action.body,
        }
        wait = await self.rate_limiter.get_token()
        if wait > 0:
            # Nothing went on the wire, so this one is definitely not sent.
            return NotSent(reason="rate limited", retry_after=wait)
        try:
            response = await self.client.post("/send", json=payload)
            response.raise_for_status()
            return Sent(receipt_id=response.json()["receipt_id"])
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            return NotSent(reason=f"http {status}", retry_after=_retry_after(e.response))
        except httpx.TimeoutException as e:
            return NotSent(reason=f"timeout: {type(e).__name__}")
        except httpx.HTTPError as e:
            return NotSent(reason=f"transport error: {type(e).__name__}")
        except (ValueError, KeyError) as e:
            return NotSent(reason=f"bad response body: {type(e).__name__}")

    async def sync_one_page(self) -> SyncResult:
        """Copy the next page of /sent, from provider_stats.sent_cursor, into receipts.

        Only the worker holding the sync advisory lock reads a page. Reading the cursor, taking
        a token, and saving the page are separate transactions, so no lock is held across the
        HTTP call; the page's receipts and the new cursor commit together.
        """
        worker_db = self.worker_db
        async with worker_db.sync_lock() as conn:
            if conn is None:
                # Already waited up to the lock timeout for another worker's page.
                return SyncResult(done=False)
            cursor = await worker_db.get_sent_cursor(conn)
            wait = await self.rate_limiter.get_token()
            if wait > 0:
                return SyncResult(done=False, wait=wait)
            try:
                response = await self.client.get(
                    "/sent", params={"since": cursor, "limit": SENT_PAGE_SIZE}
                )
                response.raise_for_status()
                page = response.json()
                receipts = [Receipt.model_validate(r) for r in page["receipts"]]
                next_cursor = int(page["next"])
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                retry_after = _retry_after(e.response)
                print(f"sync_one_page failed at cursor {cursor}: http {status}")
                if status == 429 or retry_after is not None:
                    return SyncResult(done=False, wait=max(1, math.ceil(retry_after or 0)))
                return SyncResult(done=False)
            except (httpx.HTTPError, ValueError, KeyError) as e:
                print(f"sync_one_page failed at cursor {cursor}: {type(e).__name__}: {e}")
                return SyncResult(done=False)
            async with conn.transaction():
                await worker_db.insert_receipts(receipts, conn)
                await worker_db.save_sent_cursor(next_cursor, conn)
            return SyncResult(done=len(receipts) < SENT_PAGE_SIZE)

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
