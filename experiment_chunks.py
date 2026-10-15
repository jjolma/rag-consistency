"""Chunking experiment: does the consumer leave orphaned chunks behind when text gets shorter?

  python experiment_chunks.py N

For each consumer mode (naive, correct):
  1. start the consumer in that mode
  2. insert N long articles (OLD_CHUNKS chunks each), wait until they are indexed
  3. update every article to a shorter text (NEW_CHUNKS chunks), wait until the consumer catches up
  4. count orphaned / stale / missing chunks of those N articles
  5. retrieval test: search for text that only existed in the old body and count how many
     results still return that exact old chunk ("ghost hits")
  6. delete the N articles, wait until their points are gone, append a row to results/chunks.csv

Only the experiment's own articles are measured, so other rows in the table don't matter.
"Caught up" can't mean verify.consistent (naive mode never gets there), so it means: none of
the experiment's chunks are missing or stale. Old and new text never share a chunk, so every
chunk is stale or missing until its update has been applied.
"""
import argparse
import csv
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

import verify
from common import CHUNK_SIZE, COLLECTION, DSN, MODEL_NAME, QDRANT_URL, chunk_body, chunk_hash
from experiment_outage import CONSUMER_LOG, POLL_S, ROOT, require_no_other_consumer, start_consumer, stop_consumer

CSV_PATH = ROOT / "results" / "chunks.csv"
FIELDS = ["timestamp", "mode", "n_articles", "old_chunks", "new_chunks", "orphaned", "stale", "missing",
          "ghost_hits", "ghost_queries", "catch_up_s"]
OLD_CHUNKS, NEW_CHUNKS = 6, 2
TIMEOUT_S = 180


def make_body(tag: str, label: str, n_chunks: int) -> str:
    """A body of exactly n_chunks full chunks; every chunk is distinct and contains a unique phrase."""
    return "".join(f"[{label} {i}] phrase-{tag}-{label}-{i} ".ljust(CHUNK_SIZE, ".") for i in range(n_chunks))


def mine(keys: list[tuple[int, int]], ids: set[int]) -> list[tuple[int, int]]:
    return [k for k in keys if k[0] in ids]


def wait_for(proc, cond, what: str) -> float:
    """Poll verify.check() every 100 ms until cond(result) holds; return seconds waited."""
    start = time.monotonic()
    while True:
        r = verify.check()
        if cond(r):
            return time.monotonic() - start
        if proc.poll() is not None:
            raise RuntimeError(f"consumer exited with code {proc.returncode}; see {CONSUMER_LOG}")
        if time.monotonic() - start > TIMEOUT_S:
            raise TimeoutError(f"timed out waiting for {what}")
        time.sleep(POLL_S)


def run_mode(mode: str, n: int) -> dict:
    tag = uuid.uuid4().hex[:8]
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    consumer = start_consumer("--mode", mode)
    try:
        print(f"[{mode}] inserting {n} articles of {OLD_CHUNKS} chunks ...")
        ids, old_bodies = [], {}
        for k in range(n):
            body = make_body(f"{tag}-{k}", "old", OLD_CHUNKS)  # per-article token, so no two chunks are identical
            cur.execute("INSERT INTO articles (title, body, updated_at) VALUES (%s, %s, now()) RETURNING id",
                        (f"chunk experiment {tag}", body))
            ids.append(cur.fetchone()[0])
            old_bodies[ids[-1]] = body
        idset = set(ids)
        # Slot may hold older backlog, so the consumer's startup time is included in this wait.
        wait_for(consumer, lambda r: not (mine(r.missing, idset) or mine(r.stale, idset)), "initial indexing")

        print(f"[{mode}] shortening every article to {NEW_CHUNKS} chunks ...")
        for k, id_ in enumerate(ids):
            cur.execute("UPDATE articles SET body=%s, updated_at=now() WHERE id=%s",
                        (make_body(f"{tag}-{k}", "new", NEW_CHUNKS), id_))
        catch_up = wait_for(consumer, lambda r: not (mine(r.missing, idset) or mine(r.stale, idset)), "catch-up")

        r = verify.check()
        orphaned, stale, missing = (len(mine(x, idset)) for x in (r.orphaned, r.stale, r.missing))

        # Retrieval: for each article, query with a chunk that only existed in the old body
        # (it is beyond the new chunk count) and count results that are that exact old chunk.
        # A vector search always returns its k nearest points, so raw result counts would
        # be meaningless; "still returned" has to mean "returns the deleted text".
        client = QdrantClient(url=QDRANT_URL)
        ghost_hits = 0
        for id_ in ids:
            old_only = chunk_body(old_bodies[id_])[NEW_CHUNKS]
            res = client.query_points(COLLECTION, query=model.encode(old_only).tolist(), limit=5, with_payload=True)
            ghost_hits += sum(p.payload["chunk_hash"] == chunk_hash(old_only) and p.payload["article_id"] == id_
                              for p in res.points)

        print(f"[{mode}] cleaning up: deleting the experiment's articles ...")
        cur.execute("DELETE FROM articles WHERE id = ANY(%s)", (ids,))
        wait_for(consumer, lambda r: not mine(r.orphaned, idset), "delete cleanup (points of deleted articles)")

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "mode": mode, "n_articles": n, "old_chunks": OLD_CHUNKS, "new_chunks": NEW_CHUNKS,
            "orphaned": orphaned, "stale": stale, "missing": missing,
            "ghost_hits": ghost_hits, "ghost_queries": n, "catch_up_s": round(catch_up, 2),
        }
    finally:
        stop_consumer(consumer)
        conn.close()


def append_row(row: dict) -> None:
    CSV_PATH.parent.mkdir(exist_ok=True)
    new_file = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            w.writeheader()
        w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("n", type=int, help="number of long articles to seed and then shorten")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    require_no_other_consumer()
    global model
    model = SentenceTransformer(MODEL_NAME)  # the query must be embedded the same way as the chunks
    for mode in ("naive", "correct"):
        row = run_mode(mode, args.n)
        append_row(row)
        print(f"[{mode}] " + "  ".join(f"{k}={v}" for k, v in row.items()))


if __name__ == "__main__":
    main()
