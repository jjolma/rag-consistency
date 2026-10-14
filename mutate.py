"""Insert / update / delete test rows.

  python mutate.py insert "Title" "Body text"
  python mutate.py update 1 "New title" "New body"
  python mutate.py delete 1
"""
import sys

import psycopg2

DSN = "host=localhost port=5432 dbname=app user=postgres password=postgres"

cmd, args = sys.argv[1], sys.argv[2:]
with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
    if cmd == "insert":
        cur.execute(
            "INSERT INTO articles (title, body, updated_at) VALUES (%s, %s, now()) RETURNING id",
            (args[0], args[1]),
        )
        print("inserted id", cur.fetchone()[0])
    elif cmd == "update":
        cur.execute(
            "UPDATE articles SET title=%s, body=%s, updated_at=now() WHERE id=%s",
            (args[1], args[2], int(args[0])),
        )
        print("updated", cur.rowcount, "row(s)")
    elif cmd == "delete":
        cur.execute("DELETE FROM articles WHERE id=%s", (int(args[0]),))
        print("deleted", cur.rowcount, "row(s)")
    else:
        sys.exit(__doc__)
