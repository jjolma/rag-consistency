"""CDC consumer: Postgres logical replication (test_decoding) -> Qdrant.

Run:  python cdc_consumer.py
"""
import logging
import re

import psycopg2
import psycopg2.extras
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams
from sentence_transformers import SentenceTransformer

DSN = "host=localhost port=5432 dbname=app user=postgres password=postgres"
SLOT = "articles_slot"
COLLECTION = "articles"
MODEL_NAME = "all-MiniLM-L6-v2"  # 384-dim

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
for noisy in ("httpx", "huggingface_hub", "sentence_transformers"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger("cdc")


# --- test_decoding parsing -------------------------------------------------
# Example lines emitted by test_decoding:
#   BEGIN 742
#   table public.articles: INSERT: id[integer]:1 title[text]:'It''s a title' body[text]:'...' updated_at[timestamp with time zone]:'2026-01-01 00:00:00+00'
#   table public.articles: DELETE: id[integer]:1
#   COMMIT 742
CHANGE_RE = re.compile(r"^table (\w+)\.(\w+): (INSERT|UPDATE|DELETE):(.*)$", re.S)
# column[type]:value   where value is 'quoted ''text''' or a bare token (numbers, null, true)
COL_RE = re.compile(r"(\w+)\[([^\]]+)\]:('(?:[^']|'')*'|\S+)")


def parse_columns(s: str) -> dict:
    cols = {}
    for name, _type, raw in COL_RE.findall(s):
        if raw.startswith("'"):
            cols[name] = raw[1:-1].replace("''", "'")
        elif raw == "null":
            cols[name] = None
        else:
            cols[name] = raw
    return cols


# --- sinks -----------------------------------------------------------------
model = SentenceTransformer(MODEL_NAME)
qdrant = QdrantClient(url="http://localhost:6333")
if not qdrant.collection_exists(COLLECTION):
    qdrant.create_collection(
        COLLECTION,
        vectors_config=VectorParams(size=model.get_embedding_dimension(), distance=Distance.COSINE),
    )


def handle(payload: str) -> None:
    if payload.startswith(("BEGIN", "COMMIT")):
        log.debug(payload)
        return
    m = CHANGE_RE.match(payload)
    if not m:
        log.warning("unparsed message: %s", payload)
        return
    schema, table, op, rest = m.groups()
    if (schema, table) != ("public", "articles"):
        return
    cols = parse_columns(rest)
    article_id = int(cols["id"])

    if op == "DELETE":
        qdrant.delete(COLLECTION, points_selector=[article_id])
        log.info("DELETE id=%s -> deleted point", article_id)
        return

    text = f"{cols.get('title') or ''}\n{cols.get('body') or ''}"
    vector = model.encode(text).tolist()
    qdrant.upsert(
        COLLECTION,
        points=[PointStruct(id=article_id, vector=vector, payload={"updated_at": cols.get("updated_at")})],
    )
    log.info("%s id=%s title=%r updated_at=%s -> upserted point", op, article_id, cols.get("title"), cols.get("updated_at"))


# --- replication -----------------------------------------------------------
def ensure_slot() -> None:
    """Create the logical replication slot if it doesn't exist.

    A slot is a server-side bookmark into the WAL. Postgres retains all WAL
    from the slot's confirmed_flush_lsn onward until we acknowledge it, so we
    never miss changes even if the consumer is down (the flip side: an unused
    slot makes WAL pile up on disk, so drop it when you're done).
    The output plugin ('test_decoding') is fixed at slot creation and decides
    how WAL records are rendered; test_decoding renders human-readable text.
    """
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
        if cur.fetchone():
            log.info("slot %s already exists", SLOT)
        else:
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'test_decoding')", (SLOT,))
            log.info("created slot %s", SLOT)
    conn.close()


def main() -> None:
    ensure_slot()
    # A *replication* connection speaks the streaming replication protocol,
    # which is what lets us START_REPLICATION on a logical slot.
    conn = psycopg2.connect(DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = conn.cursor()
    # decode=True gives str payloads. Starting without start_lsn resumes from the
    # slot's confirmed position, i.e. everything we haven't acknowledged yet.
    cur.start_replication(slot_name=SLOT, decode=True, options={"include-xids": "0", "skip-empty-xacts": "1"})
    log.info("streaming from slot %s ...", SLOT)

    def consume(msg):
        handle(msg.payload.strip())
        # Acknowledge: tells Postgres it may discard WAL up to this LSN.
        # Done after handling, so a crash mid-change means redelivery (at-least-once).
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    try:
        cur.consume_stream(consume)
    except KeyboardInterrupt:
        log.info("stopped")


if __name__ == "__main__":
    main()
