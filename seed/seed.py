import asyncio
import json
from itertools import batched
from pathlib import Path

from psycopg import AsyncConnection, sql

from config import DB_ROLE_CONFIG, RATE_LIMIT_TOKENS
from shared.db_connector import NOW_MS, DBConnector

ACTIONS_JSON = Path(__file__).resolve().parent / "actions.json"

INSERT_BATCH_SIZE = 100

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
    sent_cursor   bigint NOT NULL DEFAULT 0,
    tokens        double precision NOT NULL DEFAULT {RATE_LIMIT_TOKENS},
    last_added_at bigint NOT NULL DEFAULT {NOW_MS},
    blocked_until bigint NOT NULL DEFAULT 0
);
"""

CREATE_INDEXES = """
-- A unique index on a constant caps provider_stats at a single row.
CREATE UNIQUE INDEX provider_stats_single_row_idx ON provider_stats ((true));
-- pick_one reads the oldest PENDING row; release_stale reads SENDING rows picked too long ago.
CREATE INDEX actions_status_created_at_idx ON actions (status, created_at);
CREATE INDEX actions_status_attempt_started_at_idx ON actions (status, attempt_started_at);
CREATE INDEX receipts_action_id_idx ON receipts (action_id);
"""


async def create_role(conn: AsyncConnection, name: str, role_config: dict) -> None:
    """Create `name` as a login role, dropping it first if it already exists.

    Args:
        conn: A connection as the owner role, to run the statements on.
        name: The role's name.
        role_config: The role's DB_ROLE_CONFIG entry. Its grants is a GRANT statement with {}
            in place of the role.
    """
    role = sql.Identifier(name)
    cur = await conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (name,))
    if await cur.fetchone() is not None:
        # Keep anything the role owns, then revoke its grants so DROP ROLE is allowed.
        await conn.execute(sql.SQL("REASSIGN OWNED BY {} TO CURRENT_USER").format(role))
        await conn.execute(sql.SQL("DROP OWNED BY {}").format(role))
        await conn.execute(sql.SQL("DROP ROLE {}").format(role))
    password = sql.Literal(role_config["password"])
    await conn.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(role, password))
    for setting, value in role_config["timeouts"].items():
        await conn.execute(
            sql.SQL("ALTER ROLE {} SET {} = {}").format(
                role, sql.Identifier(setting), sql.Literal(value)
            )
        )
    await conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
    await conn.execute(sql.SQL(role_config["grants"]).format(role))


async def main() -> None:
    """Recreate the schema, load the actions, and create the roles.

    Loads seed/actions.json, and creates every DB_ROLE_CONFIG role that has grants. It all runs in
    one transaction as dispatch, so a seed that fails changes nothing.
    """
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
            for name, role_config in DB_ROLE_CONFIG.items():
                if role_config["grants"] is not None:
                    await create_role(conn, name, role_config)
    finally:
        await db.close()
    print(f"seeded {len(actions)} actions")


if __name__ == "__main__":
    asyncio.run(main())
