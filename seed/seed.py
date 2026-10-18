import asyncio
import json
from itertools import batched
from pathlib import Path

from psycopg import AsyncConnection, sql

from shared.db_connector import DBConnector

ACTIONS_JSON = Path(__file__).resolve().parent / "actions.json"

WORKER_ROLE = "worker"
WORKER_PASSWORD = "dispatch"
WORKER_TIMEOUT = "5s"
INSERT_BATCH_SIZE = 100

NOW_MS = "(extract(epoch FROM clock_timestamp()) * 1000)::bigint"

DROP_TABLES = """
DROP TABLE IF EXISTS actions, receipts, provider_stats;
DROP TYPE IF EXISTS action_status;
"""

CREATE_TABLES = f"""
CREATE TYPE action_status AS ENUM ('PENDING', 'SENDING', 'SENT', 'FAILED');

CREATE TABLE actions (
    id                 uuid PRIMARY KEY,
    recipient          text NOT NULL,
    body               text NOT NULL,
    created_at         bigint NOT NULL DEFAULT {NOW_MS},
    attempt_n          integer NOT NULL DEFAULT 0,
    status             action_status NOT NULL DEFAULT 'PENDING',
    attempt_started_at bigint,
    receipt_id         text,
    sent_at            bigint,
    failure_reason     text,
    CONSTRAINT sent_has_receipt CHECK (status <> 'SENT' OR receipt_id IS NOT NULL),
    CONSTRAINT failed_has_reason CHECK (status <> 'FAILED' OR failure_reason IS NOT NULL),
    CONSTRAINT sending_has_start CHECK (status <> 'SENDING' OR attempt_started_at IS NOT NULL)
);

CREATE TABLE receipts (
    receipt_id text PRIMARY KEY,
    action_id  uuid NOT NULL REFERENCES actions (id),
    recipient  text NOT NULL,
    sent_at    bigint NOT NULL
);

CREATE TABLE provider_stats (
    id             integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    sent_cursor    bigint NOT NULL DEFAULT 0,
    blocked_until  bigint NOT NULL DEFAULT 0,
    tokens         double precision NOT NULL DEFAULT 0,
    refilled_at    bigint NOT NULL DEFAULT 0,
    requests_total bigint NOT NULL DEFAULT 0,
    responses_ok   bigint NOT NULL DEFAULT 0,
    responses_429  bigint NOT NULL DEFAULT 0,
    responses_500  bigint NOT NULL DEFAULT 0,
    timeouts       bigint NOT NULL DEFAULT 0,
    resets         bigint NOT NULL DEFAULT 0
);
"""

# provider_stats is a single row, so its primary key is the only index it needs.
CREATE_INDEXES = """
CREATE INDEX actions_claimable_idx ON actions (status, attempt_started_at)
    WHERE status IN ('PENDING', 'SENDING');
CREATE INDEX receipts_action_id_idx ON receipts (action_id);
"""


async def create_worker_role(conn: AsyncConnection) -> None:
    role = sql.Identifier(WORKER_ROLE)
    cur = await conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (WORKER_ROLE,))
    if await cur.fetchone() is not None:
        # Keep anything the role owns, then revoke its grants so DROP ROLE is allowed.
        await conn.execute(sql.SQL("REASSIGN OWNED BY {} TO CURRENT_USER").format(role))
        await conn.execute(sql.SQL("DROP OWNED BY {}").format(role))
        await conn.execute(sql.SQL("DROP ROLE {}").format(role))
    await conn.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(role, sql.Literal(WORKER_PASSWORD))
    )
    # Postgres 16 has no transaction_timeout (added in 17); these two bound a transaction instead.
    for setting in ("lock_timeout", "statement_timeout", "idle_in_transaction_session_timeout"):
        await conn.execute(
            sql.SQL("ALTER ROLE {} SET {} = {}").format(
                role, sql.Identifier(setting), sql.Literal(WORKER_TIMEOUT)
            )
        )
    await conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
    await conn.execute(
        sql.SQL(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON actions, receipts, provider_stats TO {}"
        ).format(role)
    )


async def main() -> None:
    actions = json.loads(ACTIONS_JSON.read_text(encoding="utf-8"))
    db = DBConnector(role="dispatch", min_size=1, max_size=1)
    await db.open()
    try:
        async with db.transaction() as conn:
            await conn.execute(DROP_TABLES)
            await conn.execute(CREATE_TABLES)
            await conn.execute(CREATE_INDEXES)
            async with conn.cursor() as cur:
                for batch in batched(actions, INSERT_BATCH_SIZE):
                    await cur.executemany(
                        "INSERT INTO actions (id, recipient, body) VALUES (%s, %s, %s)",
                        [(a["id"], a["recipient"], a["body"]) for a in batch],
                    )
            await conn.execute("INSERT INTO provider_stats DEFAULT VALUES")
            await create_worker_role(conn)
    finally:
        await db.close()
    print(f"seeded {len(actions)} actions")


if __name__ == "__main__":
    asyncio.run(main())
