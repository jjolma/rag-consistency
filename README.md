# rag-consistency

A change-data-capture pipeline that keeps a Qdrant RAG index in sync with Postgres `articles` rows
through logical replication, plus fault-injection experiments that measure how the index goes wrong
and how it recovers when the consumer is killed, restarted or backfilled.

*Everything in this README was written by Claude Code, except the Findings section, which I will write myself.*

```
+-------------------+        +-----------------+          +--------+
| Postgres articles | -WAL-> | cdc_consumer.py | -embed-> | Qdrant |
+-------------------+        +-----------------+          +--------+
          ^                                                    ^
          +------------- verify.py compares both --------------+
```

## Findings

This project found 19 pipeline issues, of which at least four silently lost or corrupted data. It also found 14 harness bugs. These were discovered through experiments (including \~380 crash trials) and design review. Most issues led to a safeguard, and the key safeguards have a test or experiment to verify that the fix works and the absence of the safeguard would cause the expected problem.

Below are three of the most significant findings.

### Replication lag alone does not measure correctness.

A flawed backfill approach (copy-first ordering) shows no lag even though data was lost. A test run showed 10,497 chunks lost even with 0 bytes of lag. Meanwhile, a healthy pipeline under load shows hundreds of KB. So a healthy pipeline may show more lag than an unhealthy one.

Also, a crashed backfill shows that there is a replication lag but not that data is lost. Test runs showed thousands of chunks lost, while the 3–15 KB of lag only indicated how much catch up was required.

### Backfill crashes could result in permanent, silent data loss. 

Experiment runs lost a median of \~6,200 chunks in 20 out of 20 trials even though no errors were logged. This was discovered during an early pipeline iteration that relied on a Postgres CDC replication slot’s existence to indicate backfill completion. The fix was to write a backfill completion marker. Experiments confirmed this prevented the data loss in 20 out of 20 runs.

### Replaying replication rewinds the index.

If the pipeline crashed and was restarted, unacknowledged changes were sent again (since these ACKs were batched). This resulted in articles reverting to older versions and deleted articles getting resurrected. The fix was to add a marker so replays couldn’t bring back a deleted article and keep track of each article’s version (the guard), ignoring older updates. Experiments verified the older version problem in 5 out of 17 runs and zero rewinds in 17 runs with the guard. A test verified the resurrection fix.

## Pipeline design

The consumer is a plain CDC loop; the safeguards around it are what the experiments pushed it toward. Each
line names the failure it answers (see `docs/fault_design.md` for the crash points behind them).

| Safeguard | What it does | Failure it answers |
|---|---|---|
| Chunking, insert-then-trim | Bodies are split into 200-character chunks (`common.chunk_body`) with deterministic point ids. An UPDATE upserts the new chunks, then deletes the article's chunks beyond the new count (`trim`, the default). | Delete-then-insert (`correct` mode) leaves the article with no points between the two steps; plain upsert (`naive`) leaves orphan chunks when the text gets shorter. A crash between trim's two steps leaves a stale tail instead. |
| Backfill completion marker | A marker in Qdrant records that a copy finished and which collection the slot feeds. Without it a plain start runs a full backfill instead of streaming. | `kill -9` mid-backfill leaves the slot behind; a plain restart would stream onto the half-built index and never fill the gap. |
| Alias-swap rebuilds | Readers query the alias `article_chunks`. A backfill builds a new physical collection and repoints the alias when done. | A rebuild that deletes and recreates the collection readers query leaves them with an empty or missing index meanwhile. |
| Version guard with tombstones | Each change's LSN is the article's version, kept in a paired versions collection that swaps with the chunks. A replayed change with a lower LSN is skipped. Deletes leave tombstones. | Acks are batched, so a crash replays up to about 10 s of already-applied changes, which can rewind an article through its history or resurrect a deleted one. |
| `--repair` | Rebuilds the articles the verifier finds wrong (or the ids you give) from Postgres, ignoring the guard. | The guard means replay no longer fixes a bad write that was already stamped with a version. |
| Single-instance lock | A Postgres advisory lock on a dedicated connection; a second consumer exits. | The marker handling, sweeps and slot all assume one consumer. The lock is released when the process dies, even on `kill -9`. |
| Background sweeps | After streaming starts, one sweep deletes orphan `article_chunks_*` collections and another drops tombstones the slot can no longer replay past. Errors are logged and never stop the stream. | Crashes mid-rebuild leave superseded collections, and tombstones would otherwise accumulate. |

## Prerequisites

- Docker Desktop, running
- Python 3.11
- macOS or Linux (tested on an M-series MacBook Air)
- Ports 5432 (Postgres) and 6333 (Qdrant) free
- Internet access on first run: `pip install` downloads about 220 MB on an Apple-silicon Mac (PyTorch is 132 MB of that) and takes about 1.1 GB of disk once installed, and the first consumer start downloads the embedding model (about 90 MB). On Linux, pip's default PyTorch wheel bundles CUDA libraries and can be several GB; a CPU-only build is much smaller (not measured here).

## Quickstart

### 1. Start the services

Starts Postgres 16 (logical replication on, `articles` table created) and Qdrant.

```bash
docker compose up -d
```

### 2. Set up Python

```bash
python3.11 -m venv .venv && source .venv/bin/activate
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
3. Run `python verify.py`. After steps 3-5 above you have one article, so there are now two rows in
   Postgres and one point in Qdrant, and the new article (id 3) is reported as missing:

   ```
   source rows: 2   index points: 1
   missing     1  [(3, 0)]
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
5. Wait until the consumer's log shows `streaming from slot` and the replayed `INSERT id=3` (about 7
   seconds in testing, mostly loading the model), then run `python verify.py` again. It prints
   `CONSISTENT` straight away. `lag` may still show a few hundred bytes: the consumer acknowledges
   changes in batches about every 10 seconds, and the lag drops to 0 after the next ack.

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

## Repo map

| File | What it is |
|---|---|
| `cdc_consumer.py` | The CDC consumer: reads a logical replication slot (`test_decoding`), chunks and embeds articles, writes them to Qdrant, and implements the safeguards above. |
| `common.py` | Shared settings and helpers: chunking, point ids, timestamp normalisation, connection settings. |
| `verify.py` | The oracle: compares Postgres with Qdrant and reports missing, orphaned, stale and payload-stale chunks plus orphan collections. |
| `mutate.py` | Insert, update or delete a test article by hand. |
| `dump_qdrant.py` | Print every point in the index. |
| `experiment_faults.py` | The `kill -9` harness: `calibrate`, `backfill`, `control` and `steady` experiments. |
| `experiment_outage.py` | Earlier experiment: stop the consumer, write N changes, restart and time the catch-up. |
| `experiment_backfill.py` | Earlier experiment: backfill under concurrent writes, with two negative controls. |
| `experiment_chunks.py` | Earlier experiment: orphaned chunks when text gets shorter, in `naive` and `correct` modes. |
| `test_guard.py` | Tests for the version guard, paired swap, tombstones, `--repair` and sweeps, against a throwaway database. |
| `docs/fault_design.md` | The fault-injection design and ranked predictions. |
| `init.sql`, `docker-compose.yml`, `requirements.txt` | The `articles` table, the Postgres 16 and Qdrant containers, and Python dependencies. |
| `CLAUDE.md` | Guidance for Claude Code when working in this repo. |

Experiment output goes to `results/`, which is created on first use and is not committed.

## Limitations

- **Research code, not production:** built to measure failure and recovery, not to run a real service.
  There is no authentication (the compose file uses the default `postgres` password), no metrics and no
  packaging.
- **One machine, small corpus:** everything runs against Docker containers on localhost. The experiments
  use hundreds of articles (`--rows` defaults to 800), and `verify.py` re-chunks every row, so it won't
  scale to a large table.
- **`test_decoding`, not `pgoutput`:** the consumer parses the plugin's human-readable text. That keeps the
  stream easy to inspect but is not what a production setup would use.
- **Only the consumer is killed:** the experiments `kill -9` the consumer. They do not crash Postgres or
  Qdrant, partition the network, or drop a single request (the design doc leaves a TCP-proxy approach for
  later).
- **Naive chunking:** fixed 200-character chunks, deliberately simple so chunk boundaries are predictable.
- **One consumer at a time:** by design, enforced by the advisory lock.

## Troubleshooting

- **`Collection article_chunks doesn't exist` (404 from `dump_qdrant.py` or a query):** no backfill has run
  yet. Start the consumer with `python cdc_consumer.py`; it builds the collection on its first start.
- **Port 5432 or 6333 already in use:** another Postgres or Qdrant is running. Stop it, or free the port,
  before `docker compose up -d`.
- **`another consumer is already running (advisory lock ... is held); exiting`:** a consumer is already
  running against this slot. Only one can run at a time, and the experiments refuse to start while one
  does. Stop it (Ctrl-C in its terminal) and try again.
- **A warning about unauthenticated requests to the Hugging Face Hub:** harmless; no token is needed.

## License

[MIT](LICENSE).
