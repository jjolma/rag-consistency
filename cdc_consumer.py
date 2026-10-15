"""CDC consumer: Postgres logical replication (test_decoding) -> Qdrant.

Run:  python cdc_consumer.py [--mode naive|correct]

Each article body is split into chunks; every chunk is one Qdrant point. On UPDATE:
  correct (default): delete all points of the article, then insert the new chunks
  naive:             just upsert the new chunks (leaves orphans if the text got shorter)
DELETE removes all the article's points in both modes.
"""
import argparse
import logging
import re
import signal

import psycopg2
import psycopg2.extras
from qdrant_client import QdrantClient
from qdrant_client.models import (Distance, FieldCondition, Filter, FilterSelector, MatchValue,
                                  PayloadSchemaType, PointStruct, VectorParams)
from sentence_transformers import SentenceTransformer

from common import COLLECTION, DSN, MODEL_NAME, QDRANT_URL, SLOT, chunk_body, chunk_hash, iso_utc, point_id

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
qdrant = QdrantClient(url=QDRANT_URL)
if not qdrant.collection_exists(COLLECTION):
    qdrant.create_collection(
        COLLECTION,
        vectors_config=VectorParams(size=model.get_embedding_dimension(), distance=Distance.COSINE),
    )
    qdrant.create_payload_index(COLLECTION, "article_id", PayloadSchemaType.INTEGER)  # for delete-by-article


def delete_article_points(article_id: int) -> None:
    qdrant.delete(
        COLLECTION,
        points_selector=FilterSelector(
            filter=Filter(must=[FieldCondition(key="article_id", match=MatchValue(value=article_id))])
        ),
    )


def handle(payload: str, mode: str) -> None:
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
        delete_article_points(article_id)
        log.info("DELETE id=%s -> deleted all points", article_id)
        return

    updated_at = iso_utc(cols.get("updated_at"))
    chunks = chunk_body(cols.get("body"))
    if op == "UPDATE" and mode == "correct":
        delete_article_points(article_id)  # drops chunks beyond the new count (and any stale ones)
    if chunks:
        vectors = model.encode(chunks)
        qdrant.upsert(
            COLLECTION,
            points=[
                PointStruct(
                    id=point_id(article_id, i),
                    vector=vec.tolist(),
                    payload={"article_id": article_id, "chunk_index": i, "updated_at": updated_at,
                             "chunk_hash": chunk_hash(text)},
                )
                for i, (text, vec) in enumerate(zip(chunks, vectors))
            ],
        )
    log.info("%s id=%s -> %d chunk(s) upserted (%s)", op, article_id, len(chunks), mode)


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
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["naive", "correct"], default="correct", help="how UPDATE treats existing chunks")
    mode = ap.parse_args().mode
    ensure_slot()
    # A *replication* connection speaks the streaming replication protocol,
    # which is what lets us START_REPLICATION on a logical slot.
    conn = psycopg2.connect(DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    cur = conn.cursor()
    # decode=True gives str payloads. Starting without start_lsn resumes from the
    # slot's confirmed position, i.e. everything we haven't acknowledged yet.
    cur.start_replication(slot_name=SLOT, decode=True, options={"include-xids": "0", "skip-empty-xacts": "1"})
    log.info("streaming from slot %s (mode=%s) ...", SLOT, mode)

    last_flushed = [0]  # LSN of the last message fully handled

    def consume(msg):
        handle(msg.payload.strip(), mode)
        # Acknowledge: tells Postgres it may discard WAL up to this LSN.
        # Done after handling, so a crash mid-change means redelivery (at-least-once).
        # Note send_feedback() only *records* the LSN; psycopg2 transmits it when the
        # status interval (~10 s) elapses, so a quick shutdown must force a final send.
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        last_flushed[0] = msg.data_start

    def request_stop(*_):
        raise KeyboardInterrupt  # route SIGTERM through the same clean-shutdown path as Ctrl+C

    signal.signal(signal.SIGTERM, request_stop)
    try:
        cur.consume_stream(consume)
    except KeyboardInterrupt:
        if last_flushed[0]:
            # Without this, an ack still waiting for the status interval is lost on exit and
            # the next start replays everything since the last transmitted ack.
            cur.send_feedback(flush_lsn=last_flushed[0], force=True)
        lsn = last_flushed[0]
        log.info("stopped; final flush at LSN %X/%X", lsn >> 32, lsn & 0xFFFFFFFF)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
