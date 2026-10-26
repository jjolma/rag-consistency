"""Settings and helpers shared by the consumer and the verifier."""
import hashlib
import uuid
from datetime import datetime, timezone

DSN = "host=localhost port=5432 dbname=app user=postgres password=postgres"
QDRANT_URL = "http://localhost:6333"
SLOT = "articles_slot"
# The name every reader queries, one point per (article_id, chunk_index). In alias mode (the default) it is
# an alias for a physical collection article_chunks_<stamp>_<id>; with --no-alias-swap it is the physical
# collection itself. Readers never need to know which.
COLLECTION = "article_chunks"
MODEL_NAME = "all-MiniLM-L6-v2"  # 384-dim
CHUNK_SIZE = 200  # characters


def alias_target(client, alias: str = COLLECTION) -> str | None:
    """The physical collection `alias` points at, or None if it is not an alias (absent, or a physical collection)."""
    return next((a.collection_name for a in client.get_aliases().aliases if a.alias_name == alias), None)


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


def chunk_body(body: str | None) -> list[str]:
    """Split a body into fixed-size pieces of CHUNK_SIZE characters (the last may be shorter).

    Deliberately naive (no word boundaries): the consumer and the verifier both call this,
    so they always agree on what the chunks of a given body are.
    """
    body = body or ""
    return [body[i:i + CHUNK_SIZE] for i in range(0, len(body), CHUNK_SIZE)]


def chunk_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def point_id(article_id: int, chunk_index: int) -> str:
    """Deterministic Qdrant point id (Qdrant ids must be uint or UUID, so a tuple becomes a uuid5)."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, f"{article_id}:{chunk_index}"))
