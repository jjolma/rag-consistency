"""Compare Postgres `articles` with the Qdrant collection.

Exit code 0 if consistent (no missing / orphaned / stale), 1 otherwise.
Replication lag is reported but does not affect the exit code: any real
inconsistency caused by lag already shows up as missing/orphaned/stale.
"""
import sys
from dataclasses import dataclass

import psycopg2
from qdrant_client import QdrantClient

from common import COLLECTION, DSN, QDRANT_URL, SLOT, iso_utc


def read_postgres() -> tuple[dict[int, str | None], int | None]:
    """Return ({id: normalized updated_at}, slot lag in bytes or None if the slot is missing)."""
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id, updated_at FROM articles")
        rows = {id_: iso_utc(ts) for id_, ts in cur.fetchall()}
        cur.execute(
            """SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
               FROM pg_replication_slots WHERE slot_name = %s""",
            (SLOT,),
        )
        row = cur.fetchone()
    conn.close()
    return rows, (int(row[0]) if row else None)


def read_qdrant() -> dict[int, str | None]:
    client = QdrantClient(url=QDRANT_URL)
    points, offset = {}, None
    while True:
        batch, offset = client.scroll(COLLECTION, limit=500, offset=offset, with_payload=True, with_vectors=False)
        points.update({p.id: (p.payload or {}).get("updated_at") for p in batch})
        if offset is None:
            return points


@dataclass
class Result:
    source_rows: int
    index_points: int
    missing: list[int]
    orphaned: list[int]
    stale: list[int]
    lag: int | None  # bytes; None if the slot doesn't exist

    @property
    def consistent(self) -> bool:
        return not (self.missing or self.orphaned or self.stale)


def check() -> Result:
    """Compare Postgres with Qdrant right now. Importable by experiments."""
    source, lag = read_postgres()
    index = read_qdrant()
    return Result(
        source_rows=len(source),
        index_points=len(index),
        missing=sorted(source.keys() - index.keys()),
        orphaned=sorted(index.keys() - source.keys()),
        stale=sorted(i for i in source.keys() & index.keys() if source[i] != index[i]),
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
