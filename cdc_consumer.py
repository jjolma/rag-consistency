"""CDC consumer: Postgres logical replication (test_decoding) -> Qdrant.

Run:  python cdc_consumer.py [--mode naive|correct] [--backfill]

--backfill first indexes every row that already exists (snapshot-then-stream, see backfill()),
then streams. Use it for the first run, or to rebuild the index from scratch.

Each article body is split into chunks; every chunk is one Qdrant point. On UPDATE:
  correct (default): delete all points of the article, then insert the new chunks
  naive:             just upsert the new chunks (leaves orphans if the text got shorter)
DELETE removes all the article's points in both modes.
"""
import argparse
import logging
import os
import re
import signal
import time

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

UPSERT_BATCH = int(os.environ.get("UPSERT_BATCH", "256"))  # points per Qdrant upsert request; lowered by fault experiments

# --- fault injection (experiment_faults.py) --------------------------------
# hook(name) marks a stage boundary. It does nothing unless one of these is set:
#   STAGE_FILE  file that always holds the last boundary crossed. A kill -9 can't lose a write
#               that already reached the OS, so after a crash it says which stage the process died in.
#   FAULT, FAULT_AT  SIGKILL ourselves the FAULT_AT-th time boundary FAULT is reached.
STAGE_FILE = os.environ.get("STAGE_FILE")
FAULT = os.environ.get("FAULT")
FAULT_AT = int(os.environ.get("FAULT_AT", "1"))
_stage_fd = os.open(STAGE_FILE, os.O_WRONLY | os.O_CREAT) if STAGE_FILE else None
_hits: dict[str, int] = {}


def hook(name: str) -> None:
    if _stage_fd is not None:
        os.pwrite(_stage_fd, name.encode().ljust(32), 0)
    if name == FAULT:
        _hits[name] = _hits.get(name, 0) + 1
        if _hits[name] == FAULT_AT:
            os.kill(os.getpid(), signal.SIGKILL)  # same as an external kill -9: no handlers, no flush


# --- test_decoding parsing -------------------------------------------------
# Example lines emitted by test_decoding:
#   BEGIN 742
#   table public.articles: INSERT: id[integer]:1 title[text]:'It''s a title' body[text]:'...' updated_at[timestamp with time zone]:'2026-01-01 00:00:00+00'
#   table public.articles: DELETE: id[integer]:1
#   COMMIT 742
CHANGE_RE = re.compile(r"^table (\w+)\.(\w+): (INSERT|UPDATE|DELETE):(.*)$", re.S)
# column[type]:value   where value is 'quoted ''text''' or a bare token (numbers, null, true)
COL_RE = re.compile(r"(\w+)\[([^\]]+)\]:('(?:[^']|'')*'|\S+)")


# For an UPDATE that doesn't touch a large (TOASTed) column, Postgres doesn't log its value and
# test_decoding prints this bare placeholder instead. It is not data.
UNCHANGED_TOAST = "unchanged-toast-datum"


def parse_columns(s: str) -> dict:
    """Parse columns into {name: value}. Columns that arrived as the TOAST placeholder are left out
    entirely, so `"body" in cols` means "we were sent the body" and the placeholder never becomes a value."""
    cols = {}
    for name, _type, raw in COL_RE.findall(s):
        if raw == UNCHANGED_TOAST:
            continue
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


def ensure_collection(recreate: bool = False) -> None:
    if recreate and qdrant.collection_exists(COLLECTION):
        qdrant.delete_collection(COLLECTION)
    if not qdrant.collection_exists(COLLECTION):
        qdrant.create_collection(
            COLLECTION,
            vectors_config=VectorParams(size=model.get_embedding_dimension(), distance=Distance.COSINE),
        )
        qdrant.create_payload_index(COLLECTION, "article_id", PayloadSchemaType.INTEGER)  # for delete-by-article


ensure_collection()


def article_filter(article_id: int) -> Filter:
    return Filter(must=[FieldCondition(key="article_id", match=MatchValue(value=article_id))])


def upsert_chunks(items: list[tuple[int, int, str, str | None]], batch: int = UPSERT_BATCH) -> None:
    """Embed and upsert chunks. items are (article_id, chunk_index, text, updated_at)."""
    for start in range(0, len(items), batch):
        part = items[start:start + batch]
        vectors = model.encode([text for _, _, text, _ in part])
        hook("after_embed")
        qdrant.upsert(
            COLLECTION,
            points=[
                PointStruct(
                    id=point_id(article_id, i),
                    vector=vec.tolist(),
                    payload={"article_id": article_id, "chunk_index": i, "updated_at": updated_at,
                             "chunk_hash": chunk_hash(text)},
                )
                for (article_id, i, text, updated_at), vec in zip(part, vectors)
            ],
        )
        if start + batch < len(items):
            hook("mid_upsert")
    hook("after_upsert")


def delete_article_points(article_id: int) -> None:
    hook("before_delete")
    qdrant.delete(COLLECTION, points_selector=FilterSelector(filter=article_filter(article_id)))
    hook("after_delete")


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
    if "body" not in cols:  # body arrived as the TOAST placeholder
        if op == "INSERT":  # an insert always carries every column; don't index a row we can't read
            log.error("INSERT id=%s arrived with an unchanged-toast body; skipping (not indexed)", article_id)
            return
        # Body untouched: the chunks are still right, so don't re-chunk or re-embed. Refresh the payload only.
        hook("before_set_payload")
        qdrant.set_payload(COLLECTION, payload={"updated_at": updated_at}, points=article_filter(article_id))
        hook("after_set_payload")
        log.info("UPDATE id=%s -> body unchanged, payload refreshed (%s)", article_id, mode)
        return
    chunks = chunk_body(cols.get("body"))
    if op == "UPDATE" and mode == "correct":
        delete_article_points(article_id)  # drops chunks beyond the new count (and any stale ones)
    upsert_chunks([(article_id, i, text, updated_at) for i, text in enumerate(chunks)])
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


def drop_slot_if_exists() -> None:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
        if cur.rowcount:
            log.warning("dropped existing slot %s (its position is meaningless for a rebuild)", SLOT)
    conn.close()


def backfill(variant: str = "snapshot") -> psycopg2.extensions.connection:
    """Index every existing row, then hand over to streaming with no gap and no overlap.

    The handoff problem: rows already in the table are not in the WAL we will stream, and a
    plain SELECT races with concurrent writes (a change committed during the SELECT could be
    both read here and streamed later, or fall between the two). Postgres solves it by making
    one point in the WAL the dividing line, and giving us both halves of it:

      1. CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT, on a replication connection, creates the
         slot and picks a consistent_point LSN. The slot will deliver every transaction that
         commits after that LSN. It also exports a snapshot: a view of the table containing
         exactly the transactions committed before that LSN. Same boundary, both sides.
      2. A second, ordinary connection opens a REPEATABLE READ transaction and runs
         SET TRANSACTION SNAPSHOT '<name>' as its first statement, so it reads the table as of
         that moment however long the read takes and however much is written meanwhile.
      3. When the copy is done we START_REPLICATION on the slot, which resumes at
         consistent_point. Everything the snapshot already contains is not re-sent; everything
         after it is.

    Constraints: the exported snapshot is only valid while the replication connection is idle
    (it dies at its next command), so we import it before doing anything else on that
    connection. The slot retains WAL from consistent_point on, so a long copy loses nothing.
    The rebuild starts from an empty collection so rows deleted earlier can't linger.

    `variant` exists only so experiment_backfill.py can run negative controls. "snapshot" is the
    real thing; "slot-first" and "select-first" skip the exported snapshot (see create/copy below).

    Returns the replication connection, which still owns the slot, ready to stream.
    """
    drop_slot_if_exists()
    ensure_collection(recreate=True)
    t0 = time.monotonic()
    repl = psycopg2.connect(DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    rcur = repl.cursor()

    def create_slot(export_snapshot: bool) -> str | None:
        # psycopg2's create_replication_slot() has no snapshot option, but the replication
        # cursor will run the raw command and return its result row.
        option = "EXPORT_SNAPSHOT" if export_snapshot else "NOEXPORT_SNAPSHOT"
        rcur.execute(f"CREATE_REPLICATION_SLOT {SLOT} LOGICAL test_decoding {option}")
        _slot, consistent_point, snapshot, _plugin = rcur.fetchone()
        log.info("backfill: slot %s created, consistent_point=%s, snapshot=%s", SLOT, consistent_point, snapshot)
        return snapshot

    def copy_table(snapshot: str | None) -> tuple[int, int]:
        reader = psycopg2.connect(DSN)
        if snapshot:
            reader.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with reader.cursor() as cur:
                cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot,))  # must be the transaction's first statement
        n_articles = n_chunks = 0
        with reader.cursor(name="backfill_articles") as cur:  # server-side cursor: don't load the table into memory
            cur.itersize = 200
            cur.execute("SELECT id, body, updated_at FROM articles ORDER BY id")
            while rows := cur.fetchmany(200):
                items = [(id_, i, text, iso_utc(ts)) for id_, body, ts in rows for i, text in enumerate(chunk_body(body))]
                upsert_chunks(items)
                n_articles += len(rows)
                n_chunks += len(items)
                hook("backfill_mid_copy")
        reader.rollback()
        reader.close()
        return n_articles, n_chunks

    try:
        if variant == "snapshot":
            n_articles, n_chunks = copy_table(create_slot(export_snapshot=True))
        elif variant == "slot-first":  # NEGATIVE CONTROL: slot, then a plain SELECT with no shared snapshot
            log.warning("backfill: NAIVE variant 'slot-first' (negative control, not for real use)")
            create_slot(export_snapshot=False)
            n_articles, n_chunks = copy_table(None)
        elif variant == "select-first":  # NEGATIVE CONTROL: plain SELECT, then create the slot
            log.warning("backfill: NAIVE variant 'select-first' (negative control, not for real use)")
            n_articles, n_chunks = copy_table(None)
            create_slot(export_snapshot=False)
        else:
            raise ValueError(variant)
    except BaseException:
        # Leaving the slot behind would let a plain restart stream from consistent_point on top
        # of a half-built index and look healthy. Drop it so the failure is loud.
        repl.close()
        drop_slot_if_exists()
        raise
    log.info("backfill: indexed %d articles / %d chunks in %.1fs", n_articles, n_chunks, time.monotonic() - t0)
    hook("backfill_pre_stream")
    return repl


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["naive", "correct"], default="correct", help="how UPDATE treats existing chunks")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--backfill", action="store_true", help="index all existing rows first (replaces the slot and the index)")
    group.add_argument("--naive-backfill", nargs="?", const="slot-first", choices=["slot-first", "select-first"],
                       help="NEGATIVE CONTROL: backfill without an exported snapshot (slot-first: create the slot, then "
                            "plain SELECT; select-first: plain SELECT, then create the slot). Default: slot-first")
    ap.add_argument("--ack-first", action="store_true",
                    help="NEGATIVE CONTROL: acknowledge each transaction before applying it (loses changes on a crash)")
    args = ap.parse_args()
    mode = args.mode
    if args.backfill or args.naive_backfill:
        conn = backfill("snapshot" if args.backfill else args.naive_backfill)
    else:
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

    pending: list[str] = []  # --ack-first only: a transaction's changes, held until its COMMIT arrives

    def consume_ack_first(msg):
        # NEGATIVE CONTROL. Acking a change message's own LSN would not skip its transaction (Postgres
        # replays any transaction whose commit is past the acked LSN), so to really ack before applying
        # we must ack the COMMIT, which means holding the changes back until it arrives. Forced, so the
        # ack reaches Postgres before we start the slow part.
        payload = msg.payload.strip()
        if payload.startswith("BEGIN"):
            pending.clear()
        elif payload.startswith("COMMIT"):
            msg.cursor.send_feedback(flush_lsn=msg.data_start, force=True)
            last_flushed[0] = msg.data_start
            for p in pending:
                hook("recv")
                handle(p, mode)
            pending.clear()
        else:
            pending.append(payload)

    def consume(msg):
        hook("recv")
        handle(msg.payload.strip(), mode)
        hook("before_ack")
        # Acknowledge: tells Postgres it may discard WAL up to this LSN.
        # Done after handling, so a crash mid-change means redelivery (at-least-once).
        # Note send_feedback() only *records* the LSN; psycopg2 transmits it when the
        # status interval (~10 s) elapses, so a quick shutdown must force a final send.
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        last_flushed[0] = msg.data_start
        hook("acked")

    def request_stop(*_):
        raise KeyboardInterrupt  # route SIGTERM through the same clean-shutdown path as Ctrl+C

    signal.signal(signal.SIGTERM, request_stop)
    try:
        cur.consume_stream(consume_ack_first if args.ack_first else consume)
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
