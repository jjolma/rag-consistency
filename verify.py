"""Compare Postgres `articles` with the Qdrant chunk collection.

Every article body is re-chunked and each (article_id, chunk_index) is compared with Qdrant:
  missing  - chunk should exist but does not
  orphaned - chunk exists but should not (article deleted, or chunk_index >= current chunk count)
  stale    - chunk exists but its text hash differs from the current text
  payload_stale - chunk text is right but its updated_at payload differs from the article's
                  (what a missed payload-only refresh for an unchanged-TOAST update leaves behind)
Lists contain (article_id, chunk_index) pairs.

`missing` can be split given the set of chunks that were indexed at some earlier moment (split_missing):
  regressed - missing, but it was in the index then: the index lost something it had (e.g. a crash between
              an update's delete and its insert)
  pending   - missing, and it was not: a new article or new chunks the index has not caught up to yet

Separately, orphan collections: physical collections named article_chunks_* that the alias does not point
at (a half-built or superseded index a rebuild or recovery failed to delete). They don't affect what a
reader sees, so they are not part of `consistent` (a rebuild in progress always has one); they do make
the run not `clean`, which is what to check once things have settled.

The index is always read through COLLECTION, which is an alias in the consumer's default mode: the
verifier sees what a reader sees, including the old index while a rebuild is in progress.

Exit code 0 if clean (consistent and no orphan collections), 1 otherwise.
Replication lag is reported but does not affect the exit code: any real
inconsistency caused by lag already shows up as missing/orphaned/stale.
"""
import sys
from dataclasses import dataclass, field

import psycopg2
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse

from common import COLLECTION, DSN, QDRANT_URL, SLOT, alias_target, chunk_body, chunk_hash, iso_utc


Key = tuple[int, int]  # (article_id, chunk_index)
Chunk = tuple[str | None, str | None]  # (chunk hash, updated_at as iso_utc)


def read_postgres() -> tuple[dict[Key, Chunk], int, int | None]:
    """Return ({(id, chunk_index): (chunk hash, updated_at)}, article count, slot lag in bytes or None if the slot is missing or has no confirmed_flush_lsn yet)."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, body, updated_at FROM articles")
        articles = cur.fetchall()
        cur.execute(
            """SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
               FROM pg_replication_slots WHERE slot_name = %s""",
            (SLOT,),
        )
        row = cur.fetchone()
    conn.close()
    chunks = {(id_, i): (chunk_hash(t), iso_utc(ts)) for id_, body, ts in articles for i, t in enumerate(chunk_body(body))}
    return chunks, len(articles), (int(row[0]) if row and row[0] is not None else None)


def read_qdrant() -> tuple[dict[Key, Chunk], bool]:
    """Return (points, index_absent). With --no-alias-swap a --backfill deletes and recreates the collection,
    so it can vanish at any moment, even between pages of the scroll; a 404 anywhere means the index is absent
    (nothing indexed), and any pages already read are discarded rather than reported as a partial index.

    With an alias swap the scroll's pages are separate requests, each resolved to wherever the alias points
    at that moment. If the alias moved mid-scroll the result would be half old index, half new, so it is
    read again (a swap happens once per rebuild)."""
    client = QdrantClient(url=QDRANT_URL)
    for _ in range(5):
        target = alias_target(client)
        try:
            if not client.collection_exists(COLLECTION):  # consumer hasn't created it yet: nothing is indexed
                return {}, True
            points, offset = {}, None
            while True:
                batch, offset = client.scroll(COLLECTION, limit=500, offset=offset, with_payload=True, with_vectors=False)
                for p in batch:
                    pl = p.payload or {}
                    points[(pl.get("article_id"), pl.get("chunk_index"))] = (pl.get("chunk_hash"), pl.get("updated_at"))
                if offset is None:
                    break
        except UnexpectedResponse as e:
            if e.status_code != 404:
                raise
            return {}, True
        if alias_target(client) == target:
            return points, False
    raise RuntimeError(f"alias {COLLECTION} moved during every read attempt")


def read_orphan_collections() -> list[str]:
    """Physical collections with the article_chunks_ prefix that the alias doesn't point at."""
    client = QdrantClient(url=QDRANT_URL)
    target = alias_target(client)
    return sorted(c.name for c in client.get_collections().collections
                  if c.name.startswith(f"{COLLECTION}_") and c.name != target)


@dataclass
class Result:
    source_rows: int  # articles
    index_points: int  # chunks
    missing: list[Key]
    orphaned: list[Key]
    stale: list[Key]
    payload_stale: list[Key]
    lag: int | None  # bytes; None if the slot doesn't exist or has no confirmed position yet
    index_absent: bool = False  # the collection did not exist (or vanished mid-read)
    orphan_collections: list[str] = field(default_factory=list)

    @property
    def consistent(self) -> bool:
        return not (self.missing or self.orphaned or self.stale or self.payload_stale)

    @property
    def clean(self) -> bool:
        """Consistent, and no collection left over from a rebuild. Only meaningful once nothing is rebuilding."""
        return self.consistent and not self.orphan_collections


def split_missing(missing: list[Key], indexed: set[Key]) -> tuple[list[Key], list[Key]]:
    """(regressed, pending): the missing chunks that were in `indexed`, and those that were not."""
    return ([k for k in missing if k in indexed], [k for k in missing if k not in indexed])


def check() -> Result:
    """Compare Postgres with Qdrant right now. Importable by experiments."""
    source, n_articles, lag = read_postgres()
    index, index_absent = read_qdrant()
    return Result(
        source_rows=n_articles,
        index_points=len(index),
        missing=sorted(source.keys() - index.keys()),
        orphaned=sorted(index.keys() - source.keys()),
        stale=sorted(k for k in source.keys() & index.keys() if source[k][0] != index[k][0]),
        payload_stale=sorted(k for k in source.keys() & index.keys()
                             if source[k][0] == index[k][0] and source[k][1] != index[k][1]),
        lag=lag,
        index_absent=index_absent,
        orphan_collections=read_orphan_collections(),
    )


def main() -> int:
    r = check()
    print(f"source rows: {r.source_rows}   index points: {r.index_points}")
    for name, ids in (("missing", r.missing), ("orphaned", r.orphaned), ("stale", r.stale),
                      ("payload", r.payload_stale)):
        print(f"{name:9}{len(ids):4}  {ids}")
    print(f"orphan collections {len(r.orphan_collections)}  {r.orphan_collections}")
    print(f"lag      {'slot not found' if r.lag is None else f'{r.lag} bytes'}")
    print(("CONSISTENT" if r.consistent else "INCONSISTENT") + ("" if r.clean or not r.consistent else ", ORPHAN COLLECTIONS"))
    return 0 if r.clean else 1


if __name__ == "__main__":
    sys.exit(main())
