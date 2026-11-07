# rag-consistency

A change-data-capture pipeline that keeps a Qdrant RAG index in sync with Postgres `articles` rows
through logical replication, plus fault-injection experiments that measure how the index goes wrong
and how it recovers when the consumer is killed, restarted or backfilled.

```
Postgres articles ──WAL (logical slot)──> cdc_consumer.py ──chunk + embed──> Qdrant
        │                                                                      │
        └───────────────────────── verify.py compares both ────────────────────┘
```

## Prerequisites

- Docker Desktop, running
- Python 3.11
- macOS or Linux (tested on an M-series MacBook Air)
- Ports 5432 (Postgres) and 6333 (Qdrant) free
- Internet access on first run: `pip install` pulls PyTorch (a few GB), and the first consumer start downloads the embedding model (about 90 MB)

## Quickstart

### 1. Start the services

Starts Postgres 16 (logical replication on, `articles` table created) and Qdrant.

```bash
docker compose up -d
```

### 2. Set up Python

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Optionally check the setup with `python test_guard.py`. It runs against a throwaway database and slot, so
it doesn't touch the data you add below (see [Experiments and tests](#experiments-and-tests)).

### 3. Add a few articles

There is no dataset to load, and the table starts empty. For the quickstart, add rows with `mutate.py`:

```bash
python mutate.py insert "Postgres WAL" "Logical replication streams row changes from the write-ahead log to consumers."
python mutate.py insert "Qdrant" "Qdrant stores embedding vectors and serves nearest-neighbour search."
```

The fault experiments create their own corpus: `experiment_faults.py` tops the `articles` table up to
`--rows` (default 800) with generated articles before it starts, and never deletes any.

### 4. Run the pipeline (Terminal 1, leave it running)

```bash
python cdc_consumer.py
```

On the first start there is no backfill marker, so the consumer copies the existing rows into Qdrant
(creating the collection and the `article_chunks` alias) and then streams changes. It uses the default
update mode (insert, then trim stale chunks) with the version guard on. To rebuild the index from
scratch later, run it with `--backfill`.

The first start also downloads the embedding model (`all-MiniLM-L6-v2`, about 90 MB). A warning about
unauthenticated requests to the Hugging Face Hub is harmless; no token is needed.

### 5. Watch a change flow through (Terminal 2)

```bash
python mutate.py update 1 "New title" "New body"
python mutate.py delete 2
python verify.py        # Postgres vs Qdrant: missing / orphaned / stale / payload_stale
python dump_qdrant.py   # list what is in the index
```

`verify.py` re-chunks every Postgres row and compares it with Qdrant. Give the consumer a few seconds to
apply a change before verifying. A consistent index looks like this:

```
source rows: 1   index points: 1
missing     0  []
orphaned    0  []
stale       0  []
payload     0  []
orphan collections 0  []
lag      0 bytes
CONSISTENT
```

### 6. See it break and recover

The consumer is the only thing keeping the index in sync, so stop it and the index falls behind:

1. In Terminal 1, press Ctrl-C to stop the consumer.
2. In Terminal 2, add an article: `python mutate.py insert "Written while down" "Nobody is indexing this."`
3. Run `python verify.py`. The new article is reported as missing from Qdrant (`5` below is its id; yours
   will differ):

   ```
   source rows: 4   index points: 3
   missing     1  [(5, 0)]
   orphaned    0  []
   stale       0  []
   payload     0  []
   orphan collections 0  []
   lag      224 bytes
   INCONSISTENT
   ```

   Each missing entry is `(article id, chunk index)`. `lag` is the WAL the replication slot is holding for
   the consumer.
4. In Terminal 1, start the consumer again: `python cdc_consumer.py`. It replays the change from the
   replication slot, which kept the WAL while the consumer was away.
5. Wait until the consumer's log shows `streaming from slot` and the replayed change (loading the model
   makes the restart take longer than the replay itself, about 25 seconds in testing), then run
   `python verify.py` again. It prints `CONSISTENT` and the lag returns to 0.

The experiments below do this at scale, with `kill -9` at random moments, and measure what goes wrong.

## Experiments and tests

Stop your own consumer (Ctrl-C) before running an experiment: only one consumer can read a replication
slot, and the experiment starts its own. `test_guard.py` uses a throwaway database and slot, so it can run
alongside your consumer. Experiments work on the real `app` database and index, alongside any articles
you added; `docker compose down -v` gives a clean slate.

| Command | What it shows |
|---|---|
| `python test_guard.py` | The version guard, paired swap, tombstones and `--repair`, against a throwaway DB. Needs Docker up. |
| `python experiment_faults.py steady --kill random --minutes 10 --seed 4242` | Random kills under steady writes, recovery measured per trial. |
| `python experiment_faults.py backfill --recovery plain --trials 20 --seed 100` | `kill -9` of a `--backfill` mid-copy, then recovery. |
| `python experiment_faults.py calibrate` | Startup, rebuild time and throughput on your machine; use it to estimate how long a run takes. |
| `python experiment_faults.py control --ack-first` | Negative control: a consumer that acks before applying must show lost changes, or the harness can't see losses. |

`experiment_outage.py`, `experiment_backfill.py` and `experiment_chunks.py` are the earlier experiments; usage is
in each docstring.

`test_guard.py` also accepts test names (`python test_guard.py test_rewind_detector`) and `--keep` to leave
the throwaway database around.

### First experiment: random kills under steady writes

```bash
python experiment_faults.py steady --kill random --minutes 10 --seed 4242
```

A writer thread keeps inserting, updating and deleting articles while the harness repeatedly `kill -9`s the
consumer. Each trial kills the consumer, inserts a marker row, restarts a healthy consumer, waits until the
marker is indexed, then runs `verify.py` twice to measure whether and how the index recovered. Expect about
15-20 trials in 10 minutes. The run starts with an index rebuild, which takes about 2 minutes on a large
corpus.

To see what the version guard buys you, repeat the run with `--no-version-guard` and compare the results:

```bash
python experiment_faults.py steady --kill random --minutes 10 --seed 4242 --no-version-guard
```

### Backfill crash experiment

```bash
python experiment_faults.py backfill --recovery plain --trials 20 --seed 100
```

Each trial `kill -9`s a `--backfill` consumer partway through its copy, then recovers and checks the
index. `--recovery plain` restarts the consumer normally; `--recovery rebackfill` recovers by running
`--backfill` again, so you can compare the two. Expect roughly 45 minutes for 20 trials, since every trial
launches a consumer that re-embeds the corpus (`python experiment_faults.py calibrate` measures the cost
on your machine). `--seed` makes the kill points repeatable.

Results are appended to `results/*.csv` (for these experiments, `results/faults.csv`). Each script's module
docstring documents its options, and `docs/fault_design.md` explains the fault experiments.

## Troubleshooting

- **`Collection article_chunks doesn't exist` (404 from `dump_qdrant.py` or a query):** no backfill has run
  yet. Start the consumer with `python cdc_consumer.py`; it builds the collection on its first start.
- **Port 5432 or 6333 already in use:** another Postgres or Qdrant is running. Stop it, or free the port,
  before `docker compose up -d`.
- **`another consumer is already running (advisory lock ... is held); exiting`:** a consumer is already
  running against this slot. Only one can run at a time, and the experiments refuse to start while one
  does. Stop it (Ctrl-C in its terminal) and try again.
- **A warning about unauthenticated requests to the Hugging Face Hub:** harmless; no token is needed.
