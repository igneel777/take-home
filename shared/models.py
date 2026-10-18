from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel

# All timestamps are epoch milliseconds (bigint in Postgres).


class ActionStatus(StrEnum):
    PENDING = "PENDING"
    SENDING = "SENDING"
    SENT = "SENT"
    FAILED = "FAILED"


class Action(BaseModel):
    id: UUID
    recipient: str
    body: str
    created_at: int
    attempt_n: int = 0
    status: ActionStatus = ActionStatus.PENDING
    attempt_started_at: int | None = None
    receipt_id: str | None = None
    sent_at: int | None = None
    failure_reason: str | None = None


class Receipt(BaseModel):
    receipt_id: str
    action_id: UUID
    recipient: str
    sent_at: int


class ProviderStats(BaseModel):
    id: int = 1
    sent_cursor: int = 0
    blocked_until: int = 0
    tokens: float = 0.0
    refilled_at: int = 0
    requests_total: int = 0
    responses_ok: int = 0
    responses_429: int = 0
    responses_500: int = 0
    timeouts: int = 0
    resets: int = 0
