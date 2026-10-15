"""Backfill-under-load experiment: does snapshot-then-stream lose or double-apply anything?

  python experiment_backfill.py [--variant snapshot|slot-first|select-first] [--min-seconds S]

`snapshot` is the real backfill. The other two are negative controls that skip the exported snapshot:
  slot-first   create the slot, then a plain SELECT, then stream (the slot predates the read)
  select-first plain SELECT, then create the slot, then stream (changes in between are never seen)

  1. report the verifier's view of the starting state (rows that were never indexed)
  2. start a writer thread that keeps inserting / updating / deleting rows the whole time
  3. start `cdc_consumer.py --backfill` while the writer is running
  4. keep writing a little past the point where the consumer starts streaming, then stop the writer
  5. wait for the consumer to catch up and require: verify.check() fully consistent, and every
     point's payload updated_at equal to Postgres's (the verifier only compares chunk hashes)

The writer mixes in large random bodies (stored out of line by Postgres) and title-only updates
of them, because those arrive as 'unchanged-toast-datum' and are only handled correctly if the
backfill indexed the article in the first place.
"""
import argparse
import random
import signal
import string
import sys
import threading
import time

import psycopg2

import verify
from common import COLLECTION, DSN, QDRANT_URL, iso_utc
from experiment_outage import CONSUMER_LOG, POLL_S, require_no_other_consumer, start_consumer, stop_consumer
from qdrant_client import QdrantClient


IDLE_S = 5


class Writer(threading.Thread):
    """Continuously writes random changes, one autocommit transaction each."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop_event = threading.Event()
        self.ops = 0
        self.by_kind = {"insert": 0, "update_body": 0, "update_title": 0, "delete": 0}
        self.error: Exception | None = None

    def run(self) -> None:
        try:
            conn = psycopg2.connect(DSN)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT id, octet_length(body) FROM articles")
            rows = cur.fetchall()
            ids = [r[0] for r in rows]
            big = {r[0] for r in rows if (r[1] or 0) > 3000}
            while not self.stop_event.is_set():
                kind = random.choices(list(self.by_kind), weights=[25, 25, 30, 20])[0]
                if kind != "insert" and not ids:
                    kind = "insert"
                if kind == "insert":
                    body, is_big = self.body()
                    cur.execute("INSERT INTO articles (title, body, updated_at) VALUES (%s, %s, now()) RETURNING id",
                                ("writer", body))
                    ids.append(cur.fetchone()[0])
                    if is_big:
                        big.add(ids[-1])
                elif kind == "update_body":
                    body, is_big = self.body()
                    victim = random.choice(ids)
                    cur.execute("UPDATE articles SET body=%s, updated_at=now() WHERE id=%s", (body, victim))
                    (big.add if is_big else big.discard)(victim)
                elif kind == "update_title":  # body untouched; for big rows this carries the TOAST placeholder
                    victim = random.choice(list(big)) if big and random.random() < 0.6 else random.choice(ids)
                    cur.execute("UPDATE articles SET title=%s, updated_at=now() WHERE id=%s",
                                (f"title {random.random()}", victim))
                else:
                    victim = ids.pop(random.randrange(len(ids)))
                    big.discard(victim)
                    cur.execute("DELETE FROM articles WHERE id=%s", (victim,))
                self.by_kind[kind] += 1
                self.ops += 1
                time.sleep(0.005)
        except Exception as e:  # surface it in the main thread rather than dying silently
            self.error = e

    @staticmethod
    def body() -> tuple[str, bool]:
        if random.random() < 0.15:  # random text barely compresses, so Postgres stores it out of line
            return "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(3500, 6000))), True
        return "".join(random.choices(string.ascii_lowercase + " ", k=random.randint(0, 1000))), False


def log_since(offset: int) -> str:
    with open(CONSUMER_LOG, "rb") as f:
        f.seek(offset)
        return f.read().decode(errors="replace")


def payload_updated_at_mismatches() -> int:
    """Count points whose payload updated_at differs from Postgres's (the verifier doesn't look at it)."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, updated_at FROM articles")
        truth = {i: iso_utc(ts) for i, ts in cur.fetchall()}
    conn.close()
    client, bad, offset = QdrantClient(url=QDRANT_URL), 0, None
    while True:
        pts, offset = client.scroll(COLLECTION, limit=500, offset=offset, with_payload=True, with_vectors=False)
        bad += sum(truth.get(p.payload["article_id"]) != p.payload["updated_at"] for p in pts)
        if offset is None:
            return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--variant", choices=["snapshot", "slot-first", "select-first"], default="snapshot")
    ap.add_argument("--min-seconds", type=float, default=3, help="keep writing at least this long after streaming starts")
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    require_no_other_consumer()

    print(f"variant: {args.variant}")
    before = verify.check()
    print(f"before: {before.source_rows} articles, {before.index_points} indexed chunks, "
          f"{len(before.missing)} missing, {len(before.orphaned)} orphaned, {len(before.stale)} stale")

    writer = Writer()
    writer.start()
    time.sleep(1)  # let the writer get going so the backfill starts under load
    consumer = None
    try:
        consumer = start_consumer("--backfill" if args.variant == "snapshot" else f"--naive-backfill={args.variant}")
        marks = {}  # event -> writer.ops when the consumer's log first showed it
        t0 = time.monotonic()
        while "streaming" not in marks:
            text = log_since(consumer.log_offset)
            for name, marker in (("slot created", "consistent_point"), ("copy finished", "backfill: indexed"), ("streaming", "streaming from slot")):
                if name not in marks and marker in text:
                    marks[name] = writer.ops
            if consumer.poll() is not None or writer.error or time.monotonic() - t0 > 300:
                raise RuntimeError(f"backfill failed (exit {consumer.poll()}, writer {writer.error!r}); see {CONSUMER_LOG}")
            time.sleep(0.05)
        time.sleep(args.min_seconds)  # writes that arrive purely through the stream
        writer.stop_event.set()
        writer.join()
        if writer.error:
            raise writer.error

        # Wait until consistent, or (for the negative controls, which may never get there) until the
        # consumer has gone quiet: its log is one line per applied change, so a log that stops growing
        # for IDLE_S means it has caught up.
        start, idle_since, last_size = time.monotonic(), time.monotonic(), -1
        while not (r := verify.check()).consistent:
            size = CONSUMER_LOG.stat().st_size
            if size != last_size:
                last_size, idle_since = size, time.monotonic()
            if consumer.poll() is not None or time.monotonic() - start > 180:
                raise RuntimeError(f"consumer died or timed out before settling; see {CONSUMER_LOG}")
            if time.monotonic() - idle_since > IDLE_S:
                break
            time.sleep(POLL_S)
        bad_ts = payload_updated_at_mismatches()
    finally:
        writer.stop_event.set()
        stop_consumer(consumer)

    print(f"\nwriter: {writer.ops} changes total {writer.by_kind}")
    print("  changes committed when each event first showed in the consumer log (approx.; polled every 50 ms):")
    for name, ops in sorted(marks.items(), key=lambda kv: kv[1]):
        print(f"    {name:14}: {ops}")
    print("\nconsumer log:")
    print("".join(f"  {ln}\n" for ln in log_since(consumer.log_offset).splitlines()
                  if "backfill" in ln or "streaming" in ln or "ERROR" in ln or "WARNING" in ln))
    print(f"payload updated_at mismatches: {bad_ts}")
    # Same layout as verify.main(), but long lists are cut so a failing run stays readable.
    print(f"\nverifier ({args.variant}):  [full lists: python verify.py]")
    print(f"source rows: {r.source_rows}   index points: {r.index_points}")
    for name in ("missing", "orphaned", "stale"):
        ks = getattr(r, name)
        print(f"{name:9}{len(ks):4}  {ks[:5]}{' ...' if len(ks) > 5 else ''}")
    print(f"lag      {'slot not found' if r.lag is None else f'{r.lag} bytes'}")
    print("CONSISTENT" if r.consistent else "INCONSISTENT")
    return 0 if r.consistent and bad_ts == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
