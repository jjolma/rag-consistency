"""Compare Postgres `articles` with the Qdrant chunk collection.

Every article body is re-chunked and each (article_id, chunk_index) is compared with Qdrant:
  missing  - chunk should exist but does not
  orphaned - chunk exists but should not (article deleted, or chunk_index >= current chunk count)
  stale    - chunk exists but its text hash differs from the current text
  payload_stale - chunk text is right but its updated_at payload differs from the article's
                  (what a missed payload-only refresh for an unchanged-TOAST update leaves behind)
Lists contain (article_id, chunk_index) pairs.

Exit code 0 if consistent (no missing / orphaned / stale / payload_stale), 1 otherwise.
Replication lag is reported but does not affect the exit code: any real
inconsistency caused by lag already shows up as missing/orphaned/stale.
"""
import sys
from dataclasses import dataclass

import psycopg2
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse

from common import COLLECTION, DSN, QDRANT_URL, SLOT, chunk_body, chunk_hash, iso_utc


Key = tuple[int, int]  # (article_id, chunk_index)
Chunk = tuple[str | None, str | None]  # (chunk hash, updated_at as iso_utc)


def read_postgres() -> tuple[dict[Key, Chunk], int, int | None]:
    """Return ({(id, chunk_index): (chunk hash, updated_at)}, article count, slot lag in bytes or None if the slot is missing)."""
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
    return chunks, len(articles), (int(row[0]) if row else None)


def read_qdrant() -> tuple[dict[Key, Chunk], bool]:
    """Return (points, index_absent). A --backfill deletes and recreates the collection, so it can
    vanish at any moment, even between pages of the scroll; a 404 anywhere means the index is absent
    (nothing indexed), and any pages already read are discarded rather than reported as a partial index."""
    client = QdrantClient(url=QDRANT_URL)
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
                return points, False
    except UnexpectedResponse as e:
        if e.status_code != 404:
            raise
        return {}, True


@dataclass
class Result:
    source_rows: int  # articles
    index_points: int  # chunks
    missing: list[Key]
    orphaned: list[Key]
    stale: list[Key]
    payload_stale: list[Key]
    lag: int | None  # bytes; None if the slot doesn't exist
    index_absent: bool = False  # the collection did not exist (or vanished mid-read)

    @property
    def consistent(self) -> bool:
        return not (self.missing or self.orphaned or self.stale or self.payload_stale)


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
    )


def main() -> int:
    r = check()
    print(f"source rows: {r.source_rows}   index points: {r.index_points}")
    for name, ids in (("missing", r.missing), ("orphaned", r.orphaned), ("stale", r.stale),
                      ("payload", r.payload_stale)):
        print(f"{name:9}{len(ids):4}  {ids}")
    print(f"lag      {'slot not found' if r.lag is None else f'{r.lag} bytes'}")
    print("CONSISTENT" if r.consistent else "INCONSISTENT")
    return 0 if r.consistent else 1


if __name__ == "__main__":
    sys.exit(main())
