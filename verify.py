"""Compare Postgres `articles` with the Qdrant chunk collection.

Every article body is re-chunked and each (article_id, chunk_index) is compared with Qdrant:
  missing  - chunk should exist but does not
  orphaned - chunk exists but should not (article deleted, or chunk_index >= current chunk count)
  stale    - chunk exists but its text hash differs from the current text
Lists contain (article_id, chunk_index) pairs.

Exit code 0 if consistent (no missing / orphaned / stale), 1 otherwise.
Replication lag is reported but does not affect the exit code: any real
inconsistency caused by lag already shows up as missing/orphaned/stale.
"""
import sys
from dataclasses import dataclass

import psycopg2
from qdrant_client import QdrantClient

from common import COLLECTION, DSN, QDRANT_URL, SLOT, chunk_body, chunk_hash


Key = tuple[int, int]  # (article_id, chunk_index)


def read_postgres() -> tuple[dict[Key, str], int, int | None]:
    """Return ({(id, chunk_index): chunk hash}, article count, slot lag in bytes or None if the slot is missing)."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, body FROM articles")
        articles = cur.fetchall()
        cur.execute(
            """SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
               FROM pg_replication_slots WHERE slot_name = %s""",
            (SLOT,),
        )
        row = cur.fetchone()
    conn.close()
    chunks = {(id_, i): chunk_hash(t) for id_, body in articles for i, t in enumerate(chunk_body(body))}
    return chunks, len(articles), (int(row[0]) if row else None)


def read_qdrant() -> dict[Key, str | None]:
    client = QdrantClient(url=QDRANT_URL)
    if not client.collection_exists(COLLECTION):  # consumer hasn't created it yet: nothing is indexed
        return {}
    points, offset = {}, None
    while True:
        batch, offset = client.scroll(COLLECTION, limit=500, offset=offset, with_payload=True, with_vectors=False)
        for p in batch:
            pl = p.payload or {}
            points[(pl.get("article_id"), pl.get("chunk_index"))] = pl.get("chunk_hash")
        if offset is None:
            return points


@dataclass
class Result:
    source_rows: int  # articles
    index_points: int  # chunks
    missing: list[Key]
    orphaned: list[Key]
    stale: list[Key]
    lag: int | None  # bytes; None if the slot doesn't exist

    @property
    def consistent(self) -> bool:
        return not (self.missing or self.orphaned or self.stale)


def check() -> Result:
    """Compare Postgres with Qdrant right now. Importable by experiments."""
    source, n_articles, lag = read_postgres()
    index = read_qdrant()
    return Result(
        source_rows=n_articles,
        index_points=len(index),
        missing=sorted(source.keys() - index.keys()),
        orphaned=sorted(index.keys() - source.keys()),
        stale=sorted(k for k in source.keys() & index.keys() if source[k] != index[k]),
        lag=lag,
    )


def main() -> int:
    r = check()
    print(f"source rows: {r.source_rows}   index points: {r.index_points}")
    for name, ids in (("missing", r.missing), ("orphaned", r.orphaned), ("stale", r.stale)):
        print(f"{name:9}{len(ids):4}  {ids}")
    print(f"lag      {'slot not found' if r.lag is None else f'{r.lag} bytes'}")
    print("CONSISTENT" if r.consistent else "INCONSISTENT")
    return 0 if r.consistent else 1


if __name__ == "__main__":
    sys.exit(main())
