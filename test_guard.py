"""Tests for the version guard, the paired versions collection and --repair.

  python test_guard.py [--keep] [name ...]       # no names: all of them

Everything runs against a throwaway database (rag_guard_test), replication slot and Qdrant collections
(rag_guard_*), selected through the RAG_* environment variables before anything is imported, so the real
`app` database, its slot and its index are never touched. Needs the docker-compose Postgres and Qdrant up.

  test_guard_semantics    in-process, one handle() call per replayed change: strictly older is skipped, equal is
                          applied again, the version is stamped last, deletes leave a tombstone, an absent version
                          means apply, payload-only refreshes are guarded; the same calls with the guard off rewind
  test_rewind_detector    check_rewind counts and WARNs an older-than-version change (REWIND_FATAL: raises first)
  test_paired_swap        the chunks and versions aliases move together (polled while swapping), and a drop keeps pairs
  test_replay_rewind      consumer kill -9'd after applying "shrink to 5 chunks" then "grow to 30" with the acks still
                          unsent; on restart the article must never drop below 30 chunks (the same run without the
                          guard must, or the test proves nothing)
  test_tombstone          same kill after "update, then delete": replay must never bring the deleted article back
  test_repair             tamper with a stamped index (missing, stale, payload-stale, orphan chunks, an article deleted
                          behind the consumer's back); replay can't fix it, `--repair` does, and the consumer then
                          streams on without undoing it

  test_background_sweeps  stale tombstones and an orphan collection are removed after streaming starts; a tombstone
                          at or above the slot's confirmed position is kept
  test_sweep_failure_is_nonfatal  both sweeps raise (SWEEP_FAULT): each failure is logged, the consumer stays up
                          and keeps applying changes

test_replay_rewind and test_tombstone read the two consumer runs (guard on, guard off) of one scenario.
"""
import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

DB = "rag_guard_test"
os.environ.update(RAG_DSN=f"host=localhost port=5432 dbname={DB} user=postgres password=postgres",
                  RAG_SLOT="rag_guard_slot", RAG_COLLECTION="rag_guard_chunks", RAG_VERSIONS="rag_guard_versions",
                  RAG_STATE="rag_guard_state")
ADMIN_DSN = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"
ROOT = Path(__file__).parent

import psycopg2  # noqa: E402  (after the environment is set: common reads it at import)
from qdrant_client import QdrantClient  # noqa: E402
from qdrant_client.models import (DeleteAlias, DeleteAliasOperation, FieldCondition, Filter, MatchValue,  # noqa: E402
                                  PointIdsList, PointStruct)

import verify  # noqa: E402
from common import COLLECTION, QDRANT_URL, SLOT, VERSIONS, alias_target, chunk_body, iso_utc, point_id  # noqa: E402

client = QdrantClient(url=QDRANT_URL)
LOGDIR = Path(tempfile.mkdtemp(prefix="rag_guard_"))
_cdc = None


def cdc():
    """cdc_consumer, imported on first use (it loads the embedding model)."""
    global _cdc
    if _cdc is None:
        import cdc_consumer
        logging.getLogger("cdc").setLevel(logging.WARNING)
        _cdc = cdc_consumer
    return _cdc


# --- throwaway environment -------------------------------------------------
def reset_postgres() -> None:
    """A fresh rag_guard_test database with the articles table, and no rag_guard_slot."""
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True  # DROP/CREATE DATABASE can't run in a transaction (and `with conn` starts one)
    cur = conn.cursor()
    for _ in range(50):
        cur.execute("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
        row = cur.fetchone()
        if row and row[0]:
            cur.execute("SELECT pg_terminate_backend(%s)", (row[0],))
            time.sleep(0.2)
            continue
        if row:
            cur.execute("SELECT pg_drop_replication_slot(%s)", (SLOT,))
        break
    cur.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
    cur.execute(f"CREATE DATABASE {DB}")
    conn.close()
    with psycopg2.connect(os.environ["RAG_DSN"]) as conn, conn.cursor() as cur:
        cur.execute((ROOT / "init.sql").read_text())
    conn.close()


def reset_qdrant() -> None:
    gone = [DeleteAliasOperation(delete_alias=DeleteAlias(alias_name=a))
            for a in (COLLECTION, VERSIONS) if alias_target(client, a)]
    if gone:
        client.update_collection_aliases(gone)
    for c in client.get_collections().collections:
        if c.name.startswith("rag_guard_"):
            client.delete_collection(c.name)


def reset_all() -> None:
    reset_postgres()
    reset_qdrant()


# --- postgres helpers ------------------------------------------------------
def sql(query: str, params=()):
    with psycopg2.connect(os.environ["RAG_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone() if cur.description else None
    conn.close()
    return row


def body(chunks: int, tag: str) -> str:
    """A body of exactly `chunks` chunks whose text depends on `tag`."""
    return (f"{tag}|" * (chunks * 200))[:chunks * 200]


def insert(text: str, title: str = "t") -> int:
    return sql("INSERT INTO articles (title, body, updated_at) VALUES (%s, %s, now()) RETURNING id", (title, text))[0]


def update(article_id: int, text: str) -> None:
    sql("UPDATE articles SET body = %s, updated_at = now() WHERE id = %s", (text, article_id))


def delete(article_id: int) -> None:
    sql("DELETE FROM articles WHERE id = %s", (article_id,))


def wal_lsn() -> int:
    return int(sql("SELECT pg_current_wal_lsn() - '0/0'::pg_lsn")[0])


def confirmed_lsn() -> int:
    row = sql("SELECT confirmed_flush_lsn - '0/0'::pg_lsn FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))
    return int(row[0])


# --- qdrant helpers --------------------------------------------------------
def article_filter(article_id: int) -> Filter:
    return Filter(must=[FieldCondition(key="article_id", match=MatchValue(value=article_id))])


def chunk_count(article_id: int) -> int:
    return client.count(COLLECTION, count_filter=article_filter(article_id), exact=True).count


def chunk_points(article_id: int) -> dict[int, dict]:
    pts, _ = client.scroll(COLLECTION, scroll_filter=article_filter(article_id), limit=100, with_payload=True)
    return {p.payload["chunk_index"]: p.payload for p in pts}


def version_of(article_id: int) -> tuple[int, bool] | None:
    """(lsn, deleted) straight from Qdrant, bypassing the consumer's cache."""
    found = client.retrieve(VERSIONS, ids=[article_id], with_payload=True)
    return (found[0].payload["lsn"], found[0].payload["deleted"]) if found else None


def indexed(article_id: int) -> bool:
    return bool(client.retrieve(COLLECTION, ids=[point_id(article_id, 0)]))


def wait_indexed(article_id: int, timeout: float = 90) -> None:
    t0 = time.monotonic()
    while not indexed(article_id):
        assert time.monotonic() - t0 < timeout, f"article {article_id} was not indexed within {timeout:.0f}s"
        time.sleep(0.05)


# --- consumer processes ----------------------------------------------------
class Consumer:
    def __init__(self, *args: str) -> None:
        self.log_path = LOGDIR / f"consumer_{time.time_ns()}.log"
        with open(self.log_path, "wb") as log:
            self.proc = subprocess.Popen([sys.executable, str(ROOT / "cdc_consumer.py"), *args], stdout=log, stderr=log)

    @property
    def log(self) -> str:
        return self.log_path.read_text(errors="replace")

    def wait_for(self, needle: str, timeout: float = 180) -> None:
        t0 = time.monotonic()
        while needle not in self.log:
            assert self.proc.poll() is None, f"consumer exited ({self.proc.returncode}) before logging {needle!r}:\n{self.log[-2000:]}"
            assert time.monotonic() - t0 < timeout, f"consumer did not log {needle!r} within {timeout:.0f}s"
            time.sleep(0.1)

    def kill9(self) -> None:
        self.proc.kill()
        self.proc.wait()

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)  # clean exit: forces the final ack
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.kill9()


def start_consumer(*args: str) -> Consumer:
    """A consumer that is streaming. The single-instance lock of a just-killed one may take a moment to go."""
    for _ in range(10):
        c = Consumer(*args)
        try:
            c.wait_for("streaming from slot")
            return c
        except AssertionError:
            if "advisory lock" not in c.log:
                raise
            time.sleep(1)
    raise AssertionError("the single-instance lock never became free")


def wait_slot_free(timeout: float = 30) -> None:
    t0 = time.monotonic()
    while sql("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (SLOT,))[0]:
        assert time.monotonic() - t0 < timeout, "slot still active after the consumer was killed"
        time.sleep(0.1)


# --- test_decoding messages for handle() -----------------------------------
def msg(op: str, article_id: int, text: str | None = "", ts: str = "2026-01-01 00:00:00+00") -> str:
    """What test_decoding prints for a change; text=None is the unchanged-TOAST placeholder."""
    if op == "DELETE":
        return f"table public.articles: DELETE: id[integer]:{article_id}"
    col = "unchanged-toast-datum" if text is None else f"'{text}'"
    return (f"table public.articles: {op}: id[integer]:{article_id} title[text]:'t' body[text]:{col} "
            f"updated_at[timestamp with time zone]:'{ts}'")


def fresh_index() -> str:
    """Create a chunks+versions pair and point both aliases at it."""
    c = cdc()
    reset_qdrant()
    build = c.new_collection_name()
    c.create_index_collection(build)
    c.swap_alias(build)
    return build


# --- in-process tests ------------------------------------------------------
def test_guard_semantics() -> None:
    c = cdc()
    fresh_index()
    c.GUARD = True
    c._versions.clear()
    h = lambda op, a, text, lsn, ts="2026-01-01 00:00:00+00": c.handle(msg(op, a, text, ts), "trim", lsn)  # noqa: E731

    # strictly older is skipped
    a = 1001
    h("INSERT", a, body(22, "v1"), 100)
    assert len(chunk_points(a)) == 22 and version_of(a) == (100, False)
    h("UPDATE", a, body(5, "v0"), 90)
    assert len(chunk_points(a)) == 22, "a replayed change older than the index's version rewound the article"
    assert version_of(a) == (100, False), "a skipped change must not touch the version"

    # equal is applied again: a redelivery of the last change repairs a crash right after a write
    client.delete(COLLECTION, points_selector=PointIdsList(points=[point_id(a, 3)]))
    assert len(chunk_points(a)) == 21
    h("INSERT", a, body(22, "v1"), 100)
    assert len(chunk_points(a)) == 22, "a redelivered change with the same LSN was not re-applied"

    # newer is applied (trim drops the tail)
    h("UPDATE", a, body(5, "v2"), 110)
    assert sorted(chunk_points(a)) == [0, 1, 2, 3, 4] and version_of(a) == (110, False)

    # the version is stamped last: a crash before it leaves the old version, and the same change redone completes it
    real_stamp = c.stamp_version

    def crash(*_a, **_k):
        raise RuntimeError("simulated crash before the stamp")
    c.stamp_version = crash
    try:
        h("UPDATE", a, body(30, "v3"), 120)
        raise AssertionError("the stamp was not reached")
    except RuntimeError:
        pass
    finally:
        c.stamp_version = real_stamp
    assert len(chunk_points(a)) == 30, "the chunks must be written before the stamp"
    assert version_of(a) == (110, False), "the version advanced although the stamp never ran"
    c._versions.clear()  # a restarted process
    h("UPDATE", a, body(30, "v3"), 120)
    assert len(chunk_points(a)) == 30 and version_of(a) == (120, False)

    # a delete leaves a tombstone that blocks an older replayed change; a newer insert of the id replaces it
    h("DELETE", a, None, 130)
    assert chunk_points(a) == {} and version_of(a) == (130, True)
    h("UPDATE", a, body(30, "v3"), 120)
    assert chunk_points(a) == {}, "an older replayed update resurrected a deleted article"
    h("DELETE", a, None, 130)
    assert chunk_points(a) == {} and version_of(a) == (130, True), "redelivering the delete must be idempotent"
    h("INSERT", a, body(7, "v4"), 140)
    assert len(chunk_points(a)) == 7 and version_of(a) == (140, False)

    # payload-only refreshes (body arrived as the TOAST placeholder) are guarded too
    b, t1, t2, t3 = 1002, "2026-01-01 00:00:00+00", "2026-01-02 00:00:00+00", "2026-01-03 00:00:00+00"
    h("INSERT", b, body(6, "b1"), 200, t1)
    h("UPDATE", b, body(6, "b2"), 210, t2)
    h("UPDATE", b, None, 205, t1)  # replay of an older refresh
    assert {p["updated_at"] for p in chunk_points(b).values()} == {iso_utc(t2)}, "an older refresh overwrote updated_at"
    h("UPDATE", b, None, 220, t3)
    assert {p["updated_at"] for p in chunk_points(b).values()} == {iso_utc(t3)} and version_of(b) == (220, False)

    # no version means apply: what a backfill leaves behind
    d = 1003
    c.upsert_chunks([(d, i, t, "x") for i, t in enumerate(chunk_body(body(9, "bf")))])
    assert version_of(d) is None
    h("UPDATE", d, body(4, "bf2"), 1)
    assert len(chunk_points(d)) == 4 and version_of(d) == (1, False)

    # control: the same replay with the guard off rewinds and resurrects, and writes no versions
    c.GUARD = False
    try:
        e = 1004
        h("INSERT", e, body(22, "v1"), 100)
        h("UPDATE", e, body(5, "v0"), 90)
        assert len(chunk_points(e)) == 5, "control: without the guard the older update should have been applied"
        h("DELETE", e, None, 130)
        h("UPDATE", e, body(30, "v3"), 120)
        assert len(chunk_points(e)) == 30, "control: without the guard the older update should resurrect the article"
        assert version_of(e) is None, "control: no versions are written with the guard off"
    finally:
        c.GUARD = True


def test_rewind_detector() -> None:
    """check_rewind: a change older than the article's version is counted and WARNed (guard off: against the
    process's own last applied LSN); with REWIND_FATAL it raises before writing anything. The guard on never trips it."""
    c = cdc()
    fresh_index()
    h = lambda a, text, lsn: c.handle(msg("UPDATE", a, text, "2026-01-01 00:00:00+00"), "trim", lsn)  # noqa: E731
    a = 1101
    c.GUARD, c.REWIND_FATAL = False, False
    c._applied.clear()
    c.REWINDS = 0
    try:
        h(a, body(22, "v1"), 100)
        h(a, body(22, "v1"), 100)
        assert c.REWINDS == 0, "an equal re-apply is not a rewind"
        h(a, body(5, "v0"), 90)
        assert c.REWINDS == 1 and len(chunk_points(a)) == 5, "guard off: the older change should apply and count as a rewind"
        c.REWIND_FATAL = True
        try:
            h(a, body(9, "v0"), 80)
            raise AssertionError("REWIND_FATAL did not raise")
        except RuntimeError as e:
            assert "REWIND" in str(e)
        assert len(chunk_points(a)) == 5, "a fatal rewind must not write anything"
        c.GUARD = True
        c._versions.clear()
        c.REWINDS = 0
        b = 1102
        h(b, body(22, "v1"), 100)
        h(b, body(5, "v0"), 90)  # skipped by the guard, so never a rewind, even in fatal mode
        assert c.REWINDS == 0 and len(chunk_points(b)) == 22
    finally:
        c.GUARD, c.REWIND_FATAL = True, False


def test_paired_swap() -> None:
    c = cdc()
    reset_qdrant()
    a, b = c.new_collection_name(), c.new_collection_name()
    for name in (a, b):
        c.create_index_collection(name)
    c.swap_alias(a)
    assert alias_target(client, COLLECTION) == a and alias_target(client, VERSIONS) == c.versions_name(a)

    # While the aliases move back and forth, every listing (one request) must show a matching pair.
    pairs, stop = set(), threading.Event()

    def poll() -> None:
        while not stop.is_set():
            al = {x.alias_name: x.collection_name for x in client.get_aliases().aliases}
            pairs.add((al.get(COLLECTION), al.get(VERSIONS)))

    t = threading.Thread(target=poll, daemon=True)
    t.start()
    for i in range(20):
        c.swap_alias(b if i % 2 == 0 else a)
    stop.set()
    t.join()
    bad = {p for p in pairs if p[1] != c.versions_name(p[0] or "")}
    assert not bad, f"a reader saw a chunks alias with the wrong versions: {bad}"
    assert len(pairs) >= 2, "the poller never saw the swap (inconclusive)"

    c.swap_alias(b)
    c.drop_collections_except(b)
    names = {x.name for x in client.get_collections().collections if x.name.startswith("rag_guard_")}
    assert names == {b, c.versions_name(b)}, f"drop_collections_except(b) left {names}"
    assert verify.read_orphan_collections() == []

    # an index built before versions existed gets an empty pair instead of failing
    client.delete_collection(c.versions_name(b))
    c.ensure_versions()
    assert client.collection_exists(c.versions_name(b)) and alias_target(client, VERSIONS) == c.versions_name(b)
    reset_qdrant()


# --- end-to-end: kill -9 with the acks unsent ------------------------------
_scenarios: dict[bool, dict] = {}


def replay_scenario(guarded: bool) -> dict:
    """One consumer run, killed after it applied:  X shrinks to 5 chunks, Z is updated, a dozen other articles
    change, X grows back to 30 chunks, Z is deleted. The acks were never sent, so the restarted consumer replays
    all of it; it is sampled every ~10 ms until it has caught up. Cached per variant (the two tests read it)."""
    if guarded not in _scenarios:
        for _ in range(3):
            result = _replay_attempt(("--no-version-guard",) if not guarded else ())
            if result:
                _scenarios[guarded] = result
                break
        else:
            raise AssertionError("the acks reached Postgres before the kill three times; nothing was replayed")
    return _scenarios[guarded]


def _replay_attempt(flags: tuple[str, ...]) -> dict | None:
    victim = start_consumer(*flags)  # the first one finds no marker and backfills the empty table, then streams
    try:
        x, z = insert(body(30, "x0")), insert(body(30, "z0"))
        others = [insert(body(6, f"o{i}")) for i in range(12)]
        wait_indexed(insert("barrier"))
        before = wal_lsn()
        update(x, body(5, "x1"))
        update(z, body(8, "z1"))
        for i, o in enumerate(others):
            update(o, body(10, f"o{i}b"))
        update(x, body(30, "x2"))
        delete(z)
        wait_indexed(insert("barrier2"))
        assert chunk_count(x) == 30 and chunk_count(z) == 0
    finally:
        victim.kill9()
    wait_slot_free()
    if confirmed_lsn() > before:
        return None  # the status interval elapsed and an ack got out: nothing to replay, try again
    barrier = insert("barrier3")
    recovery = Consumer(*flags)
    min_x, max_z = chunk_count(x), chunk_count(z)
    try:
        while not indexed(barrier):
            if recovery.proc.poll() is not None:
                assert "advisory lock" in recovery.log, f"recovery consumer died:\n{recovery.log[-2000:]}"
                time.sleep(1)  # the killed one's lock was not released yet
                recovery = Consumer(*flags)
            min_x, max_z = min(min_x, chunk_count(x)), max(max_z, chunk_count(z))
            time.sleep(0.01)
        time.sleep(1)
        consistent = verify.check().consistent
    finally:
        recovery.stop()
    return {"x": x, "z": z, "min_x": min_x, "max_z": max_z, "consistent": consistent, "log": recovery.log}


def test_replay_rewind() -> None:
    reset_all()
    on, off = replay_scenario(True), replay_scenario(False)
    assert f"UPDATE id={off['x']} -> 5 chunk" in off["log"], "control: the unguarded restart did not replay the shrinking update"
    assert off["min_x"] <= 5, f"control: without the guard the article should have dropped to 5 chunks, saw {off['min_x']}"
    assert on["min_x"] == 30, f"with the guard the article dropped to {on['min_x']} chunks during replay"
    assert f"skipped UPDATE of id={on['x']}" in on["log"], "the guard never skipped the shrinking update"
    assert on["consistent"] and off["consistent"], "replay must still converge"


def test_tombstone() -> None:
    reset_all()
    on, off = replay_scenario(True), replay_scenario(False)
    assert off["max_z"] > 0, "control: without the guard replay should have brought the deleted article back"
    assert on["max_z"] == 0, f"with the guard a deleted article came back with {on['max_z']} chunks during replay"
    assert f"skipped INSERT of id={on['z']}" in on["log"] and f"skipped UPDATE of id={on['z']}" in on["log"]
    assert on["consistent"]


def test_repair() -> None:
    reset_all()
    c = cdc()
    consumer = start_consumer()
    r, y = insert(body(10, "r0")), insert(body(6, "y0"))
    wait_indexed(insert("barrier"))
    wait_indexed(r)
    consumer.stop()
    assert verify.check().consistent
    stamped = version_of(r)[0]

    # damage it behind the consumer's back
    client.delete(COLLECTION, points_selector=PointIdsList(points=[point_id(r, 3)]))
    client.set_payload(COLLECTION, payload={"chunk_hash": "bad"}, points=[point_id(r, 5)])
    client.set_payload(COLLECTION, payload={"updated_at": "1999-01-01T00:00:00+00:00"}, points=[point_id(r, 0)])
    client.upsert(COLLECTION, points=[PointStruct(id=point_id(r, 20), vector=[1.0] + [0.0] * 383, payload={
        "article_id": r, "chunk_index": 20, "updated_at": "x", "chunk_hash": "x"})])
    delete(y)  # Y's chunks stay in the index: an orphan
    found = verify.check()
    assert (r, 3) in found.missing and (r, 5) in found.stale and (r, 0) in found.payload_stale
    assert (r, 20) in found.orphaned and any(a == y for a, _ in found.orphaned)

    # replay can't fix it: a redelivery of anything older than the stamp is skipped
    c._versions.clear()
    c.handle(msg("UPDATE", r, body(10, "r0")), "trim", stamped - 1)
    assert (r, 3) in verify.check().missing, "an older replayed change repaired a stamped article (the guard should skip it)"

    # --repair does, and stamps past what replay will still deliver
    run = subprocess.run([sys.executable, str(ROOT / "cdc_consumer.py"), "--repair"], capture_output=True, text=True, timeout=300)
    assert run.returncode == 0, f"--repair failed:\n{run.stderr[-2000:]}"
    assert verify.check().consistent, "the index is still inconsistent after --repair"
    assert version_of(r)[0] > stamped, "the repair did not advance the article's version"
    assert version_of(y)[1] is True, "the article deleted in Postgres has no tombstone after the repair"

    # the consumer streams on from its old ack position: the delete of Y and the inserts replay and are skipped
    consumer = start_consumer()
    try:
        wait_indexed(insert("barrier2"))
        time.sleep(1)
        assert verify.check().consistent and chunk_count(y) == 0
        assert f"skipped DELETE of id={y}" in consumer.log, "the repair's stamp should make replay skip the older delete"
    finally:
        consumer.stop()


def test_background_sweeps() -> None:
    reset_all()
    start_consumer().stop()  # first start backfills and leaves a streaming-ready index
    wait_slot_free()
    confirmed = confirmed_lsn()
    old_id, new_id = 10**7, 10**7 + 1
    vec = [1.0]
    client.upsert(VERSIONS, points=[PointStruct(id=old_id, vector=vec, payload={"lsn": 1, "deleted": True}),
                                    PointStruct(id=new_id, vector=vec, payload={"lsn": confirmed + 10**9, "deleted": True})])
    orphan = f"{COLLECTION}_orphan"
    cdc().create_index_collection(orphan)
    consumer = start_consumer()
    try:
        consumer.wait_for("swept 1 tombstone")
        assert not client.collection_exists(orphan), "the orphan collection survived the background sweep"
        assert version_of(old_id) is None, "a tombstone below the slot's confirmed position was kept"
        assert version_of(new_id) is not None, "a tombstone above the confirmed position was removed"
        wait_indexed(insert(body(3, "after")))  # and streaming works
    finally:
        consumer.stop()


def test_sweep_failure_is_nonfatal() -> None:
    reset_all()
    start_consumer().stop()
    wait_slot_free()
    os.environ["SWEEP_FAULT"] = "1"  # every background sweep raises
    try:
        consumer = start_consumer()
    finally:
        del os.environ["SWEEP_FAULT"]
    try:
        consumer.wait_for("background sweep_orphans failed")
        consumer.wait_for("background sweep_tombstones failed")  # one failure doesn't stop the next sweep
        a = insert(body(4, "a"))
        wait_indexed(a)
        assert consumer.proc.poll() is None, "the consumer died after a sweep failure"
        update(a, body(7, "b"))
        t0 = time.monotonic()
        while chunk_count(a) != 7:
            assert time.monotonic() - t0 < 60, "streaming stopped after a sweep failure"
            time.sleep(0.1)
    finally:
        consumer.stop()


TESTS = [test_guard_semantics, test_rewind_detector, test_paired_swap, test_replay_rewind, test_tombstone, test_repair,
         test_background_sweeps, test_sweep_failure_is_nonfatal]


def main() -> int:
    args = sys.argv[1:]
    keep = "--keep" in args
    names = [a for a in args if a != "--keep"]
    unknown = set(names) - {t.__name__ for t in TESTS}
    if unknown:
        sys.exit(f"unknown test(s): {sorted(unknown)}; choose from {[t.__name__ for t in TESTS]}")
    chosen = [t for t in TESTS if not names or t.__name__ in names]
    failed = 0
    reset_all()
    try:
        for t in chosen:
            t0 = time.monotonic()
            try:
                t()
                print(f"PASS {t.__name__} ({time.monotonic() - t0:.0f}s)", flush=True)
            except Exception as e:  # noqa: BLE001
                failed += 1
                print(f"FAIL {t.__name__} ({time.monotonic() - t0:.0f}s): {type(e).__name__}: {e}", flush=True)
    finally:
        if not keep:
            reset_qdrant()
            reset_postgres()
            conn = psycopg2.connect(ADMIN_DSN)
            conn.autocommit = True
            conn.cursor().execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
            conn.close()
        print(f"consumer logs: {LOGDIR}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
