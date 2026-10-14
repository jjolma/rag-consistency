"""Settings and helpers shared by the consumer and the verifier."""
from datetime import datetime, timezone

DSN = "host=localhost port=5432 dbname=app user=postgres password=postgres"
QDRANT_URL = "http://localhost:6333"
SLOT = "articles_slot"
COLLECTION = "articles"


def iso_utc(ts) -> str | None:
    """Normalize a timestamptz to ISO-8601 UTC with microseconds, e.g. 2026-10-14T19:56:52.699204+00:00.

    Accepts a datetime (psycopg2) or the text test_decoding emits ('2026-10-14 19:56:52.699204+00').
    Both sides of the comparison go through this one function so the strings match exactly.
    """
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    return ts.astimezone(timezone.utc).isoformat(timespec="microseconds")
