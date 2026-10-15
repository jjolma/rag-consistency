"""Outage experiment: stop the consumer, write N changes, restart, time the catch-up.

  python experiment_outage.py N OUTAGE_SECONDS [--runs R]

Per run:
  1. start consumer, wait until verify.check() is consistent
  2. stop consumer (outage begins)
  3. write N changes (~50% updates, 30% inserts, 20% deletes), then hold until OUTAGE_SECONDS elapsed
  4. check: record missing / orphaned / stale counts and slot lag
  5. restart consumer, poll check() every 100 ms until consistent -> catch-up time
  6. append a row to results/outage.csv

Timing is measured from the moment the consumer process is spawned:
  startup_s  = spawn -> consumer logs "streaming from slot" (imports, model load, connect)
  replay_s   = streaming -> consistent
  catch_up_s = startup_s + replay_s
Both boundaries are observed by polling every 100 ms, so each has ~0.1 s resolution.
"""
import argparse
import csv
import random
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

import verify
from common import DSN, SLOT

ROOT = Path(__file__).parent
CSV_PATH = ROOT / "results" / "outage.csv"
CONSUMER_LOG = ROOT / "results" / "consumer.log"
FIELDS = ["timestamp", "run", "n", "updates", "inserts", "deletes", "outage_s",
          "max_missing", "max_orphaned", "max_stale", "max_lag_bytes", "startup_s", "replay_s", "catch_up_s"]
POLL_S = 0.1
READY_TIMEOUT_S = 120


# --- consumer subprocess ---------------------------------------------------
STREAMING_MARKER = b"streaming from slot"


def start_consumer(*extra_args: str) -> subprocess.Popen:
    CONSUMER_LOG.parent.mkdir(exist_ok=True)
    log = open(CONSUMER_LOG, "ab")  # consumer logs to stderr; keep it for debugging
    log_offset = log.tell()  # the log is shared across spawns; only look at what this one writes
    proc = subprocess.Popen([sys.executable, str(ROOT / "cdc_consumer.py"), *extra_args], stdout=log, stderr=log)
    log.close()  # the child holds its own copy of the fd
    proc.spawned = time.monotonic()
    proc.log_offset = log_offset
    proc.streaming_s = None  # seconds from spawn until the consumer logged it is streaming
    return proc


def note_streaming(proc: subprocess.Popen) -> None:
    """Record proc.streaming_s the first time the consumer's log shows it is streaming."""
    if proc.streaming_s is None:
        with open(CONSUMER_LOG, "rb") as f:
            f.seek(proc.log_offset)
            if STREAMING_MARKER in f.read():
                proc.streaming_s = time.monotonic() - proc.spawned


def stop_consumer(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)  # the consumer exits cleanly on KeyboardInterrupt
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def wait_consistent(proc: subprocess.Popen, timeout: float, seen: list[verify.Result] | None = None) -> float:
    """Poll check() every 100 ms until consistent; return seconds since the consumer was spawned.

    Also makes sure proc.streaming_s is recorded. Fails if the consumer dies or the timeout passes.
    """
    while True:
        note_streaming(proc)
        r = verify.check()
        if seen is not None:
            seen.append(r)
        elapsed = time.monotonic() - proc.spawned
        if r.consistent:
            break
        if proc.poll() is not None:
            raise RuntimeError(f"consumer exited with code {proc.returncode}; see {CONSUMER_LOG}")
        if elapsed > timeout:
            raise TimeoutError(f"not consistent after {timeout}s: {r}")
        time.sleep(POLL_S)
    while proc.streaming_s is None:  # consistent before we saw the log line (nothing to replay)
        if proc.poll() is not None or time.monotonic() - proc.spawned > timeout:
            raise RuntimeError(f"consumer never logged '{STREAMING_MARKER.decode()}'; see {CONSUMER_LOG}")
        time.sleep(POLL_S)
        note_streaming(proc)
    return elapsed


# --- workload --------------------------------------------------------------
def live_ids(cur) -> list[int]:
    cur.execute("SELECT id FROM articles")
    return [row[0] for row in cur.fetchall()]


def insert_row(cur, tag: str) -> int:
    cur.execute("INSERT INTO articles (title, body, updated_at) VALUES (%s, %s, now()) RETURNING id",
                (f"{tag} title", f"{tag} body {random.random()}"))
    return cur.fetchone()[0]


def write_changes(n: int) -> dict[str, int]:
    """Write n changes, each in its own transaction: ~50% update, 30% insert, 20% delete."""
    counts = {"updates": 0, "inserts": 0, "deletes": 0}
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    ids = live_ids(cur)
    for _ in range(n):
        op = random.choices(["update", "insert", "delete"], weights=[50, 30, 20])[0]
        if op != "insert" and not ids:
            op = "insert"  # nothing left to update/delete
        if op == "insert":
            ids.append(insert_row(cur, "outage"))
            counts["inserts"] += 1
        elif op == "update":
            cur.execute("UPDATE articles SET title=%s, body=%s, updated_at=now() WHERE id=%s",
                        (f"updated {random.random()}", f"updated body {random.random()}", random.choice(ids)))
            counts["updates"] += 1
        else:
            victim = ids.pop(random.randrange(len(ids)))
            cur.execute("DELETE FROM articles WHERE id=%s", (victim,))
            counts["deletes"] += 1
    conn.close()
    return counts


def seed_rows(n: int) -> None:
    """Make sure at least n rows exist so updates/deletes have targets (consumer is running)."""
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    cur = conn.cursor()
    for _ in range(max(0, n - len(live_ids(cur)))):
        insert_row(cur, "seed")
    conn.close()


def require_no_other_consumer() -> None:
    """The slot allows one reader; refuse to run if someone else holds it."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
        row = cur.fetchone()
    conn.close()
    if row and row[0]:
        sys.exit(f"slot {SLOT} is in use by another consumer; stop it before running the experiment")


# --- experiment ------------------------------------------------------------
def run_once(run: int, n: int, outage_s: float) -> dict:
    consumer = None
    try:
        print(f"[run {run}] starting consumer, waiting for consistency ...")
        consumer = start_consumer()
        wait_consistent(consumer, READY_TIMEOUT_S)
        seed_rows(n)
        wait_consistent(consumer, READY_TIMEOUT_S)

        stop_consumer(consumer)
        consumer = None
        outage_start = time.monotonic()
        print(f"[run {run}] outage begins; writing {n} changes ...")
        counts = write_changes(n)
        remaining = outage_s - (time.monotonic() - outage_start)
        if remaining > 0:
            time.sleep(remaining)
        else:
            print(f"[run {run}] warning: writes took longer than the requested outage")

        seen = [verify.check()]
        outage_actual = time.monotonic() - outage_start
        print(f"[run {run}] outage over after {outage_actual:.1f}s: missing={len(seen[0].missing)} "
              f"orphaned={len(seen[0].orphaned)} stale={len(seen[0].stale)} lag={seen[0].lag}B; restarting consumer")

        consumer = start_consumer()
        catch_up = wait_consistent(consumer, READY_TIMEOUT_S, seen)
        startup = consumer.streaming_s
        replay = max(0.0, catch_up - startup)
        print(f"[run {run}] consistent again after {catch_up:.2f}s (startup {startup:.2f}s + replay {replay:.2f}s)")
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "run": run, "n": n, **counts,
            "outage_s": round(outage_actual, 2),
            "max_missing": max(len(r.missing) for r in seen),
            "max_orphaned": max(len(r.orphaned) for r in seen),
            "max_stale": max(len(r.stale) for r in seen),
            "max_lag_bytes": max(r.lag or 0 for r in seen),
            "startup_s": round(startup, 2),
            "replay_s": round(replay, 2),
            "catch_up_s": round(catch_up, 2),
        }
    finally:
        stop_consumer(consumer)


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
    ap.add_argument("n", type=int, help="number of changes to write during the outage")
    ap.add_argument("outage", type=float, help="outage duration in seconds")
    ap.add_argument("--runs", type=int, default=1, help="repeat the experiment this many times")
    args = ap.parse_args()

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))  # make SIGTERM run the finally blocks too
    require_no_other_consumer()
    for run in range(1, args.runs + 1):
        row = run_once(run, args.n, args.outage)
        append_row(row)
        print(f"[run {run}] " + "  ".join(f"{k}={v}" for k, v in row.items()))


if __name__ == "__main__":
    main()
