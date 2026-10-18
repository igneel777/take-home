from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

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

    @field_validator("sent_at", mode="before")
    @classmethod
    def _to_epoch_ms(cls, value: object) -> object:
        # The provider reports sent_at as an ISO 8601 string.
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        return value


class ProviderStats(BaseModel):
    sent_cursor: int = 0
    tokens: float = 0.0
    last_added_at: int = 0
    blocked_until: int = 0


class Sent(BaseModel):
    model_config = ConfigDict(frozen=True)

    receipt_id: str


class NotSent(BaseModel):
    """No receipt came back. After a timeout or a dropped connection the provider may still
    have sent it, so this is not proof that nothing went out."""

    model_config = ConfigDict(frozen=True)

    reason: str
    retry_after: float | None = None


Outcome = Sent | NotSent


class SyncResult(BaseModel):
    """What one sync_one_page call did.

    done: the page was short, so receipts held everything in /sent when it was read.
    wait: whole seconds to wait before the next call; 0 means call again at once.
    """

    model_config = ConfigDict(frozen=True)

    done: bool
    wait: int = 0
