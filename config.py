# RATELIMIT CONSTANTS
RATE_LIMIT_TOKENS = 10  # bucket capacity, and the tokens it starts with
RATE_LIMIT_RPS = 10.0  # tokens added back per second

# DB ROLE CONSTANTS
# Postgres 16 has no transaction_timeout (added in 17); these bound a transaction instead.
ROLE_TIMEOUTS = {
    "lock_timeout": "3s",
    "statement_timeout": "3s",
    "idle_in_transaction_session_timeout": "6s",
}
DB_ROLE_CONFIG: dict[str, dict] = {
    "dispatch": {"password": "dispatch", "grants": None, "timeouts": None},
    "worker": {
        "password": "dispatch",
        "grants": "GRANT SELECT, INSERT, UPDATE, DELETE ON actions, receipts, provider_stats TO {}",
        "timeouts": ROLE_TIMEOUTS,
    },
    "ratelimiter": {
        "password": "dispatch",
        "grants": "GRANT SELECT, UPDATE ON provider_stats TO {}",
        "timeouts": ROLE_TIMEOUTS,
    },
}

# WORKER CONSTANTS
# A SENDING row whose pick is older than this belongs to a worker that died before updating.
STALE_AFTER_MS = 10_000
WORKER_FAILED_REASON = "Worker failed to update"
# Key of the advisory lock that lets one worker at a time sync /sent.
SYNC_LOCK_KEY = 1

# PROVIDER CONSTANTS
DEFAULT_PROVIDER_URL = "http://127.0.0.1:8080"
DEFAULT_TIMEOUT_SECONDS = 2.0
SENT_PAGE_SIZE = 100  # the most /sent returns per page
