"""Compare Postgres `articles` with the Qdrant collection.

Exit code 0 if consistent (no missing / orphaned / stale), 1 otherwise.
Replication lag is reported but does not affect the exit code: any real
inconsistency caused by lag already shows up as missing/orphaned/stale.
"""
import sys

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


def main() -> int:
    source, lag = read_postgres()
    index = read_qdrant()

    missing = sorted(source.keys() - index.keys())
    orphaned = sorted(index.keys() - source.keys())
    stale = sorted(i for i in source.keys() & index.keys() if source[i] != index[i])

    print(f"source rows: {len(source)}   index points: {len(index)}")
    for name, ids in (("missing", missing), ("orphaned", orphaned), ("stale", stale)):
        print(f"{name:9}{len(ids):4}  {ids}")
    print(f"lag      {'slot not found' if lag is None else f'{lag} bytes'}")

    consistent = not (missing or orphaned or stale)
    print("CONSISTENT" if consistent else "INCONSISTENT")
    return 0 if consistent else 1


if __name__ == "__main__":
    sys.exit(main())
