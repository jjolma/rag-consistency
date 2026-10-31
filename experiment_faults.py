"""Fault-injection (kill -9) experiments against the CDC consumer.

  python experiment_faults.py calibrate [--rows 800] [--changes 100]
  python experiment_faults.py backfill --recovery plain|rebackfill [--trials 20] [--rows 800] [--no-backfill-marker] [--no-alias-swap]
  python experiment_faults.py control --ack-first [--trials 20] [--window 3]
  python experiment_faults.py steady --kill random [--trials N | --minutes M] [--window 3] [--mode trim|correct|naive]

Every trial ends the same way, so results are comparable:
  1. the consumer is SIGKILLed (by an external kill -9 at a random moment, or by itself at a
     failpoint; see hook() in cdc_consumer.py)
  2. a marker row is inserted and a healthy consumer is restarted
  3. barrier: wait until the marker is indexed. The consumer applies changes in WAL order, so once
     the marker is there everything before it has been replayed. (Polling for "consistent" would not
     do: replay passes through states that happen to match Postgres.)
  4. verify.check(); again 3 s later. clean = both consistent and no orphan collection (a leftover
     article_chunks_* the alias doesn't point at) at the second check. Anything else is a PERSISTENT
     inconsistency: the restart did not repair it.
Between the kill and the barrier the verifier is sampled, giving the TRANSIENT exposure
(recovery_s = kill -> barrier, inconsistent_s = part of that the index was wrong, absent_s = part of
that the collection did not exist, e.g. while a --backfill had it deleted).

Scenarios (the `scenario` column of results/faults.csv). Runs with the consumer's alias swap (its default)
get an "-alias" suffix (backfill-plain-alias); --no-alias-swap runs keep the plain names the older rows have.
  backfill-plain       kill --backfill mid-copy at a random batch, restart with a plain consumer
  backfill-rebackfill  same kill, but recover by running --backfill again
  ack-first            NEGATIVE CONTROL: victim is `--ack-first` (acks before applying), killed at a
                       random moment during a write burst. Must show persistent loss, or the
                       harness can't see losses.
  steady-random        normal consumer killed at a random moment during a write burst
                       (--mode trim|correct|naive picks the consumer's UPDATE mode, default trim; labelled steady-random-trim etc.)

Burst kills: a writer thread starts, then the victim is killed after a random 0.05..WINDOW seconds,
while the consumer is still working through the backlog. `kill_stage` is the last stage boundary
the consumer crossed (STAGE_FILE), so each random kill is attributed to a stage.

final_lag_bytes is the slot's lag (WAL bytes not yet confirmed) at the final verification, blank if
the slot is gone; it should be near zero after a healthy recovery. --no-backfill-marker runs every
consumer with the old behavior (no completion marker), so the original failure stays reproducible.
--no-alias-swap does the same for the alias swap: every backfill deletes and recreates the collection readers query.

A trial that leaves the index dirty is repaired by a full --backfill before the next one (recorded
as repaired=1). calibrate writes results/faults_calibration.json.
"""
import argparse
import csv
import json
import math
import random
import re
import signal
import statistics
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import psycopg2
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse

import verify
from common import COLLECTION, DSN, QDRANT_URL, SLOT, point_id
from experiment_backfill import Writer
from experiment_outage import (CONSUMER_LOG, ROOT, note_streaming, require_no_other_consumer, start_consumer,
                               stop_consumer)

RESULTS = ROOT / "results"
CSV_PATH = RESULTS / "faults.csv"
CAL_PATH = RESULTS / "faults_calibration.json"
STAGE_PATH = RESULTS / "stage"
FIELDS = ["timestamp", "scenario", "trial", "seed", "fault", "kill_stage", "kill_after_s", "written",
          "applied_before_kill", "applied_after_restart", "slot_left", "missing_at_kill",
          "recovery_s", "inconsistent_s", "absent_s", "max_missing", "max_regressed", "max_pending", "max_orphaned", "max_stale", "max_payload",
          "final_missing", "final_orphaned", "final_stale", "final_payload", "orphan_collections", "final_lag_bytes",
          "clean", "repaired"]
APPLIED_RE = re.compile(r"\b(INSERT|UPDATE|DELETE) id=")
UPSERTED_RE = re.compile(r"\b(INSERT|UPDATE) id=(\d+) -> (\d+) chunk\(s\) upserted")
DELETED_RE = re.compile(r"\bDELETE id=(\d+) -> deleted all points")
NO_BACKFILL_MARKER = False  # set by --no-backfill-marker: every consumer started gets the flag
NO_ALIAS_SWAP = False  # set by --no-alias-swap: likewise
UPDATE_MODE = "trim"  # set by steady --mode: every consumer started gets --mode <this>
STARTUP_TIMEOUT_S = 120
SETTLE_S = 3  # second verification this long after the barrier, to catch late changes
BACKFILL_BATCH = 200  # articles per fetchmany() in cdc_consumer.backfill()

qdrant = QdrantClient(url=QDRANT_URL)


# --- consumers -------------------------------------------------------------
def start(*args: str, env: dict[str, str] | None = None):
    """Start a consumer that records its current stage in STAGE_PATH."""
    RESULTS.mkdir(exist_ok=True)
    STAGE_PATH.unlink(missing_ok=True)  # a running consumer keeps writing to the inode it opened, so only unlink on a fresh start
    if NO_BACKFILL_MARKER:
        args = (*args, "--no-backfill-marker")
    if NO_ALIAS_SWAP:
        args = (*args, "--no-alias-swap")
    args = (*args, "--mode", UPDATE_MODE)
    return start_consumer(*args, env={"STAGE_FILE": str(STAGE_PATH), **(env or {})})


def label(scenario: str) -> str:
    return scenario if NO_ALIAS_SWAP else f"{scenario}-alias"


def read_stage() -> str:
    try:
        return STAGE_PATH.read_text().strip() or "none"
    except FileNotFoundError:
        return "none"


def wait_streaming(proc) -> None:
    while proc.streaming_s is None:
        if proc.poll() is not None or time.monotonic() - proc.spawned > STARTUP_TIMEOUT_S:
            raise RuntimeError(f"consumer did not start streaming (exit {proc.poll()}); see {CONSUMER_LOG}")
        note_streaming(proc)
        time.sleep(0.1)


def log_size() -> int:
    return CONSUMER_LOG.stat().st_size if CONSUMER_LOG.exists() else 0


def count_applied(offset: int) -> int:
    """Changes the consumer logged as applied since `offset` (one INFO line per change)."""
    with open(CONSUMER_LOG, "rb") as f:
        f.seek(offset)
        return len(APPLIED_RE.findall(f.read().decode(errors="replace")))


def indexed_at_kill(baseline: set[tuple[int, int]], offset: int) -> set[tuple[int, int]]:
    """Chunks the index held at the kill: `baseline` (the index before the burst, consistent and idle) with the
    changes the victim logged since `offset` replayed on it. A change in flight at the kill is not logged yet,
    so an article being rewritten still counts with its previous chunks: it was fully indexed until the kill."""
    with open(CONSUMER_LOG, "rb") as f:
        f.seek(offset)
        text = f.read().decode(errors="replace")
    by_article: dict[int, set[int]] = {}
    for a, i in baseline:
        by_article.setdefault(a, set()).add(i)
    for line in text.splitlines():
        if m := UPSERTED_RE.search(line):
            by_article[int(m[2])] = set(range(int(m[3])))
        elif m := DELETED_RE.search(line):
            by_article.pop(int(m[1]), None)
    return {(a, i) for a, idx in by_article.items() for i in idx}


def slot_state() -> tuple[bool, bool]:
    """(slot exists, slot is held by a connection)"""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
        row = cur.fetchone()
    conn.close()
    return (row is not None, bool(row and row[0]))


def wait_slot_free(timeout: float = 30) -> None:
    """After a kill -9 Postgres notices the dead connection a moment later; the slot can't be reused until it does."""
    start_t = time.monotonic()
    while slot_state()[1]:
        if time.monotonic() - start_t > timeout:
            raise RuntimeError("slot still active after the consumer was killed")
        time.sleep(0.1)


# --- data ------------------------------------------------------------------
def seed_rows(n: int) -> int:
    """Make sure at least n articles exist (never deletes). Returns the row count."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM articles")
        have = cur.fetchone()[0]
        for _ in range(max(0, n - have)):
            cur.execute("INSERT INTO articles (title, body, updated_at) VALUES (%s, %s, now())", ("seed", Writer.body()[0]))
        cur.execute("SELECT count(*) FROM articles")
        total = cur.fetchone()[0]
    conn.close()
    return total


def insert_marker() -> int:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO articles (title, body, updated_at) VALUES ('barrier', %s, now()) RETURNING id",
                    (f"barrier {uuid.uuid4()}",))
        marker = cur.fetchone()[0]
    conn.close()
    return marker


def marker_indexed(marker: int) -> bool:
    try:
        return bool(qdrant.retrieve(COLLECTION, ids=[point_id(marker, 0)]))
    except UnexpectedResponse:  # collection not created yet (a --backfill recreates it)
        return False


def await_barrier(proc, marker: int, timeout: float, samples: list | None = None, sample_s: float = 0.25) -> float:
    """Wait until the marker is indexed. If `samples` is given, append (monotonic time, verify.Result)
    every sample_s. Returns the barrier time (monotonic)."""
    t0 = last = time.monotonic()
    while True:
        note_streaming(proc)
        if marker_indexed(marker):
            return time.monotonic()
        now = time.monotonic()
        if proc.poll() is not None:
            raise RuntimeError(f"consumer exited with code {proc.returncode} before the barrier; see {CONSUMER_LOG}")
        if now - t0 > timeout:
            raise TimeoutError(f"marker {marker} not indexed after {timeout:.0f}s; see {CONSUMER_LOG}")
        if samples is not None and now - last >= sample_s:
            samples.append((time.monotonic(), verify.check()))
            last = now
        time.sleep(0.05)


class Harness:
    """Owns the one healthy consumer that may be left running between trials."""

    def __init__(self) -> None:
        self.healthy = None

    def stop_healthy(self) -> None:
        stop_consumer(self.healthy)
        self.healthy = None

    def rebuild(self) -> None:
        """Full --backfill, then wait for a healthy, consistent index."""
        self.stop_healthy()
        wait_slot_free()
        proc = start("--backfill")
        try:
            await_barrier(proc, insert_marker(), timeout=900)
        except BaseException:
            stop_consumer(proc)
            raise
        r = verify.check()
        if not r.consistent:
            stop_consumer(proc)
            raise RuntimeError(f"index not consistent after a full backfill: {r}")
        self.healthy = proc

    def victim(self, args: tuple[str, ...]):
        """The consumer to kill: the running healthy one, or a fresh one with `args`."""
        if args:
            self.stop_healthy()
            wait_slot_free()
            proc = start(*args)
        else:
            proc, self.healthy = self.healthy, None
        wait_streaming(proc)
        return proc


# --- trials ----------------------------------------------------------------
def finish_trial(h: Harness, row: dict, t_kill: float, samples: list, recovery_args: tuple[str, ...], timeout: float,
                 indexed: set[tuple[int, int]] | None = None) -> None:
    """Restart, barrier, verify (twice), and fill in the measurement columns of `row`. Leaves the recovery consumer in h.healthy.
    `indexed` (the chunks in the index at the kill) splits max_missing into max_regressed and max_pending; without it both are blank."""
    wait_slot_free()
    marker = insert_marker()
    samples.append((time.monotonic(), verify.check()))  # exposure right after the crash, marker not yet indexed
    recovery = start(*recovery_args)
    h.healthy = recovery
    t_barrier = await_barrier(recovery, marker, timeout, samples)
    first = verify.check()
    time.sleep(SETTLE_S)
    final = verify.check()
    results = [r for _, r in samples]
    inconsistent = absent = 0.0
    for i, (t, r) in enumerate(samples):
        t_next = samples[i + 1][0] if i + 1 < len(samples) else t_barrier
        if not r.consistent:
            inconsistent += t_next - t
        if r.index_absent:  # also inconsistent whenever the table has rows; this says why
            absent += t_next - t
    split = [verify.split_missing(r.missing, indexed) for r in results] if indexed is not None else []
    row.update(
        max_regressed=max(len(g) for g, _ in split) if split else "",
        max_pending=max(len(p) for _, p in split) if split else "",
        applied_after_restart=count_applied(recovery.log_offset),
        recovery_s=round(t_barrier - t_kill, 2),
        inconsistent_s=round(inconsistent, 2), absent_s=round(absent, 2),
        max_missing=max(len(r.missing) for r in results), max_orphaned=max(len(r.orphaned) for r in results),
        max_stale=max(len(r.stale) for r in results), max_payload=max(len(r.payload_stale) for r in results),
        final_missing=len(final.missing), final_orphaned=len(final.orphaned),
        final_stale=len(final.stale), final_payload=len(final.payload_stale),
        orphan_collections=len(final.orphan_collections),
        final_lag_bytes="" if final.lag is None else final.lag,
        # orphan collections only at the final check: right at the barrier the superseded index may not be deleted yet
        clean=int(first.consistent and final.consistent and not final.orphan_collections),
    )


def burst_trial(h: Harness, scenario: str, trial: int, seed: int, victim_args: tuple[str, ...], window: float) -> dict:
    random.seed(seed)
    kill_after = random.uniform(0.05, window)
    victim = h.victim(victim_args)
    mark = log_size()
    baseline = set(verify.read_qdrant()[0])  # idle and consistent: nothing is being written yet
    writer = Writer()
    writer.start()
    time.sleep(kill_after)
    victim.kill()  # SIGKILL
    victim.wait()
    t_kill = time.monotonic()
    stage = read_stage()
    writer.stop_event.set()
    writer.join()
    if writer.error:
        raise writer.error
    if victim.returncode != -signal.SIGKILL:
        raise RuntimeError(f"victim exited with {victim.returncode} before the kill; see {CONSUMER_LOG}")
    row = {"scenario": scenario, "trial": trial, "seed": seed, "fault": "random", "kill_stage": stage,
           "kill_after_s": round(kill_after, 2), "written": writer.ops,
           "applied_before_kill": count_applied(mark), "slot_left": 1, "missing_at_kill": ""}
    finish_trial(h, row, t_kill, [], (), timeout=STARTUP_TIMEOUT_S + 120, indexed=indexed_at_kill(baseline, mark))
    row["repaired"] = 0
    if not row["clean"]:
        h.rebuild()  # the next trial must start from a known-good index
        row["repaired"] = 1
    return row


def backfill_trial(h: Harness, scenario: str, trial: int, seed: int, recovery: str) -> dict:
    random.seed(seed)
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM articles")
        n_rows = cur.fetchone()[0]
    conn.close()
    n_batches = math.ceil(n_rows / BACKFILL_BATCH)
    if n_batches < 2:
        raise RuntimeError(f"{n_rows} rows is a single backfill batch; use --rows > {BACKFILL_BATCH}")
    k = random.randint(1, n_batches - 1)  # die after batch k, with at least one batch still to copy
    h.stop_healthy()
    wait_slot_free()
    victim = start("--backfill", env={"FAULT": "backfill_mid_copy", "FAULT_AT": str(k)})
    try:
        victim.wait(timeout=900)
    except BaseException:
        stop_consumer(victim)
        raise
    t_kill = time.monotonic()
    if victim.returncode != -signal.SIGKILL:
        raise RuntimeError(f"backfill exited with {victim.returncode} instead of being killed after batch {k}; see {CONSUMER_LOG}")
    row = {"scenario": scenario, "trial": trial, "seed": seed, "fault": f"backfill_mid_copy@{k}/{n_batches}",
           "kill_stage": read_stage(), "kill_after_s": round(t_kill - victim.spawned, 2), "written": 0,
           "applied_before_kill": 0, "slot_left": int(slot_state()[0]), "repaired": 0}
    row["missing_at_kill"] = len(verify.check().missing)
    finish_trial(h, row, t_kill, [], ("--backfill",) if recovery == "rebackfill" else (), timeout=900)
    return row


def append_row(row: dict) -> None:
    RESULTS.mkdir(exist_ok=True)
    if CSV_PATH.exists() and CSV_PATH.read_text().split("\n", 1)[0] != ",".join(FIELDS):
        old = CSV_PATH.with_name(f"faults_{datetime.now():%Y%m%d_%H%M%S}.csv")  # columns changed: don't append misaligned rows
        CSV_PATH.rename(old)
        print(f"{CSV_PATH.name} has an older column layout; moved it to {old.name}")
    new_file = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if new_file:
            w.writeheader()
        w.writerow({"timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"), **row})


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return (max(0.0, centre - half), min(1.0, centre + half))


def summarize(scenario: str, rows: list[dict]) -> None:
    n = len(rows)
    if not n:
        return
    bad = [r for r in rows if not r["clean"]]
    lo, hi = wilson(len(bad), n)
    print(f"\n=== {scenario}: {n} trials ===")
    print(f"persistent inconsistency after recovery: {len(bad)}/{n} = {len(bad) / n:.0%}   (95% CI {lo:.0%}-{hi:.0%})")
    for key in ("final_missing", "final_orphaned", "final_stale", "final_payload", "orphan_collections"):
        hit = [r[key] for r in rows if r[key]]
        if hit:
            print(f"  {key:15} in {len(hit)}/{n} trials, median {statistics.median(hit)} chunks when present")
    print(f"recovery_s (kill -> barrier): median {statistics.median(r['recovery_s'] for r in rows):.1f}   "
          f"inconsistent_s: median {statistics.median(r['inconsistent_s'] for r in rows):.1f}   "
          f"max_missing: median {statistics.median(r['max_missing'] for r in rows)}")
    for key in ("max_regressed", "max_pending"):
        vals = [r[key] for r in rows if r[key] != ""]
        if vals:
            print(f"  {key:13} median {statistics.median(vals)}, max {max(vals)}, nonzero in {sum(1 for x in vals if x)}/{len(vals)} trials")
    stages = Counter(r["kill_stage"] for r in rows)
    if len(stages) > 1 or "random" == rows[0]["fault"]:
        print("kill landed in (last stage crossed):  " + "  ".join(f"{s} {c}" for s, c in stages.most_common()))
        for s, c in stages.most_common():
            sbad = sum(1 for r in rows if r["kill_stage"] == s and not r["clean"])
            if sbad:
                print(f"  persistent inconsistency after a kill in {s}: {sbad}/{c}")


def run_trials(name: str, trial_fn, trials: int | None, minutes: float | None) -> None:
    deadline = time.monotonic() + minutes * 60 if minutes else None
    rows, durations = [], []
    i = 0
    while (trials is None or i < trials) and (deadline is None or time.monotonic() + (max(durations, default=0)) < deadline):
        i += 1
        t0 = time.monotonic()
        row = trial_fn(i)
        durations.append(time.monotonic() - t0)
        append_row(row)
        rows.append(row)
        print(f"[{name} {i}{'/' + str(trials) if trials else ''}] {durations[-1]:.0f}s  stage={row['kill_stage']}  "
              f"recovery={row['recovery_s']}s inconsistent={row['inconsistent_s']}s  "
              f"max regressed/pending={row['max_regressed']}/{row['max_pending']}  "
              f"final m/o/s/p={row['final_missing']}/{row['final_orphaned']}/{row['final_stale']}/{row['final_payload']}  "
              f"orphan_colls={row['orphan_collections']}  "
              f"lag={'slot gone' if row['final_lag_bytes'] == '' else str(row['final_lag_bytes']) + 'B'}  "
              f"{'clean' if row['clean'] else 'DIRTY'}{' (repaired)' if row['repaired'] else ''}", flush=True)
    summarize(name, rows)


# --- subcommands -----------------------------------------------------------
def cmd_calibrate(args) -> None:
    n_rows = seed_rows(args.rows)
    print(f"table has {n_rows} articles")
    proc = start("--backfill")
    t0 = proc.spawned
    try:
        t_done = await_barrier(proc, insert_marker(), 900)
        wait_streaming(proc)  # the marker is indexed by the copy, before streaming starts
    except BaseException:
        stop_consumer(proc)
        raise
    text = open(CONSUMER_LOG, "rb").read()[proc.log_offset:].decode(errors="replace")
    m = re.search(r"backfill: indexed (\d+) articles / (\d+) chunks in ([\d.]+)s", text)
    rebuild_s, copy_s = t_done - t0, float(m.group(3))
    n_chunks = int(m.group(2))
    stop_consumer(proc)
    print(f"full rebuild: {rebuild_s:.1f}s (startup {proc.streaming_s - copy_s:.1f}s + copy {copy_s:.1f}s for {n_chunks} chunks)")

    startups = []
    for _ in range(3):
        p = start()
        wait_streaming(p)
        startups.append(p.streaming_s)
        stop_consumer(p)
    startup_s = statistics.median(startups)
    print(f"consumer startup (spawn -> streaming): {', '.join(f'{s:.1f}' for s in startups)}s, median {startup_s:.1f}s")

    # Throughput and where the time goes: queue a backlog while no consumer runs, then watch STAGE_FILE
    # at ~1 ms while it drains. The share of samples per stage is the chance a random kill lands there.
    writer = Writer()
    writer.start()
    while writer.ops < args.changes:
        time.sleep(0.01)
    writer.stop_event.set()
    writer.join()
    marker = insert_marker()
    p = start()
    counts, t_first, t_end, last_check = Counter(), None, None, 0.0
    try:
        while t_end is None:
            now = time.monotonic()
            stage = read_stage()
            if t_first is None and stage != "none":
                t_first = now
            if t_first is not None:
                counts[stage] += 1
            if t_first is not None and now - last_check > 0.1:
                last_check = now
                if marker_indexed(marker):
                    t_end = time.monotonic()
            if p.poll() is not None or now - p.spawned > STARTUP_TIMEOUT_S + 300:
                raise RuntimeError(f"consumer died or stalled during calibration; see {CONSUMER_LOG}")
            time.sleep(0.001)
    finally:
        stop_consumer(p)
    busy_s = t_end - t_first
    throughput = (writer.ops + 1) / busy_s
    total = sum(counts.values())
    shares = {s: c / total for s, c in counts.most_common()}
    print(f"drained {writer.ops + 1} changes in {busy_s:.1f}s = {throughput:.1f} changes/s "
          f"(writer by kind: {writer.by_kind})")
    print("share of busy time by last stage crossed (= where a random kill lands):")
    for s, share in shares.items():
        print(f"  {s:20}{share:6.1%}")
    cal = {"rows": n_rows, "chunks": n_chunks, "rebuild_s": round(rebuild_s, 1), "copy_s": round(copy_s, 1),
           "startup_s": round(startup_s, 1), "throughput_cps": round(throughput, 1),
           "stage_share": {s: round(v, 4) for s, v in shares.items()}}
    CAL_PATH.write_text(json.dumps(cal, indent=2))
    print(f"\nwrote {CAL_PATH.relative_to(ROOT)}")
    print(f"rough trial cost: backfill-plain ~{copy_s / 2 + startup_s + startup_s + 3:.0f}s, backfill-rebackfill ~{copy_s / 2 + rebuild_s + 3:.0f}s, "
          f"burst trial ~{startup_s + args.window / 2 + 8:.0f}s clean, ~{startup_s + args.window / 2 + 8 + rebuild_s:.0f}s dirty (includes a full rebuild)")


def cmd_backfill(args) -> None:
    seed_rows(args.rows)
    h = Harness()
    h.rebuild()  # trial 1 must kill a rebuild of a healthy index too, not of whatever the last run left behind
    base = args.seed
    print(f"base seed {base}")
    scenario = label(f"backfill-{args.recovery}")
    try:
        run_trials(scenario, lambda i: backfill_trial(h, scenario, i, base + i, args.recovery), args.trials, None)
    finally:
        h.stop_healthy()


def burst_command(args, scenario: str, victim_args: tuple[str, ...]) -> None:
    seed_rows(args.rows)
    h = Harness()
    h.rebuild()
    print(f"base seed {args.seed}")
    try:
        run_trials(scenario, lambda i: burst_trial(h, scenario, i, args.seed + i, victim_args, args.window),
                   args.trials, getattr(args, "minutes", None))
    finally:
        h.stop_healthy()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, trials: int | None = 20, with_trials: bool = True):
        p.add_argument("--rows", type=int, default=800, help="make sure the table has at least this many articles")
        p.add_argument("--no-backfill-marker", action="store_true",
                       help="OLD BEHAVIOR: run every consumer with --no-backfill-marker, so an interrupted backfill is streamed from")
        p.add_argument("--no-alias-swap", action="store_true",
                       help="OLD BEHAVIOR: run every consumer with --no-alias-swap, so a backfill empties and refills the collection readers query")
        p.add_argument("--seed", type=int, default=int(time.time()) % 100000, help="trial i uses seed+i (recorded in the CSV)")
        if with_trials:
            p.add_argument("--trials", type=int, default=trials)

    p = sub.add_parser("calibrate", help="measure startup, rebuild time, throughput and where a random kill would land")
    common(p, with_trials=False)
    p.add_argument("--changes", type=int, default=100, help="backlog size for the throughput measurement")
    p.add_argument("--window", type=float, default=3, help="(only used for the trial-cost estimate)")

    p = sub.add_parser("backfill", help="kill --backfill mid-copy, then recover")
    common(p)
    p.add_argument("--recovery", choices=["plain", "rebackfill"], required=True,
                   help="plain: restart the consumer normally; rebackfill: run --backfill again")

    p = sub.add_parser("control", help="negative control: harness must see losses from a consumer that acks before applying")
    common(p)
    p.add_argument("--ack-first", action="store_true", required=True)
    p.add_argument("--window", type=float, default=3, help="kill at a random 0.05..WINDOW seconds into a write burst")

    p = sub.add_parser("steady", help="kill the normal consumer at random moments during a write burst")
    common(p, trials=None)
    p.add_argument("--kill", choices=["random"], required=True)
    p.add_argument("--mode", choices=["trim", "correct", "naive"], default="trim", help="how the consumer treats UPDATEs")
    p.add_argument("--minutes", type=float, help="keep running trials until this much time has passed (alternative to --trials)")
    p.add_argument("--window", type=float, default=3, help="kill at a random 0.05..WINDOW seconds into a write burst")

    args = ap.parse_args()
    if args.cmd == "steady" and not (args.trials or args.minutes):
        ap.error("steady needs --trials or --minutes")
    global NO_BACKFILL_MARKER, NO_ALIAS_SWAP, UPDATE_MODE
    UPDATE_MODE = getattr(args, "mode", "trim")
    NO_BACKFILL_MARKER = args.no_backfill_marker
    NO_ALIAS_SWAP = args.no_alias_swap
    if not NO_ALIAS_SWAP and NO_BACKFILL_MARKER:
        ap.error("--no-backfill-marker needs --no-alias-swap")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    require_no_other_consumer()
    if args.cmd == "calibrate":
        cmd_calibrate(args)
    elif args.cmd == "backfill":
        cmd_backfill(args)
    elif args.cmd == "control":
        burst_command(args, label("ack-first"), ("--ack-first",))
    else:
        burst_command(args, label(f"steady-random-{UPDATE_MODE}"), ())


if __name__ == "__main__":
    main()
