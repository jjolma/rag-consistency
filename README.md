# rag-consistency

A change-data-capture pipeline that keeps a Qdrant RAG index in sync with Postgres `articles` rows
through logical replication, plus fault-injection experiments that measure how the index goes wrong
and how it recovers when the consumer is killed, restarted or backfilled.

## Quickstart

Requires Docker and Python 3.11.

```bash
# 1. Start Postgres 16 (logical replication on, `articles` table created) and Qdrant
docker compose up -d

# 2. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Add a few articles (the table starts empty)
python mutate.py insert "Postgres WAL" "Logical replication streams row changes from the write-ahead log to consumers."
python mutate.py insert "Qdrant" "Qdrant stores embedding vectors and serves nearest-neighbour search."

# 4. Build the index and keep it in sync (leave this running)
python cdc_consumer.py --backfill

# 5. In a second terminal: change data, then check the index
python mutate.py update 1 "New title" "New body"
python mutate.py delete 2
python verify.py        # Postgres vs Qdrant: missing / orphaned / stale / payload_stale
python dump_qdrant.py   # list what is in the index
```

The first run needs `--backfill`: it creates the Qdrant collection and the `article_chunks` alias.
Later runs can use plain `python cdc_consumer.py`.

## Experiments and tests

```bash
python test_guard.py                                  # guard tests, against a throwaway DB (needs docker up)
python experiment_faults.py steady --kill random --minutes 10 --seed 4242
```

Results go to `results/*.csv`. Each script's module docstring documents its options, and
`docs/fault_design.md` explains the fault experiments.
