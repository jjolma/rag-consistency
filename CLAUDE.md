# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A change-data-capture (CDC) pipeline plus fault-injection experiments: Postgres `articles` rows are chunked, embedded and kept in sync in Qdrant (a RAG index) through logical replication. The experiments measure how the index goes wrong, and how it recovers, when the consumer is killed, restarted or backfilled. There is no build step or linter; `README.md` has a quickstart, and each script's module docstring is its documentation.

## Setup and commands

```
docker compose up -d                       # Postgres 16 (wal_level=logical, init.sql creates `articles`) + Qdrant, on localhost
pip install -r requirements.txt            # use the repo's .venv

python cdc_consumer.py --backfill          # first run, or rebuild the index from scratch (snapshot-then-stream)
python cdc_consumer.py                     # stream only; a missing backfill marker forces a full backfill instead
python cdc_consumer.py --repair [ID ...]   # rebuild wrong articles (all the verifier finds, or the given ids) from Postgres
python verify.py                           # compare Postgres with Qdrant: missing / orphaned / stale / payload_stale
python mutate.py insert|update|delete ...  # hand-made test changes

python test_guard.py                       # all guard tests, against a throwaway DB/slot/collections (needs docker up)
python test_guard.py test_rewind_detector  # one test, by function name; --keep leaves the throwaway DB around

python experiment_faults.py steady --kill random --minutes 10 --seed 4242 [--no-version-guard]
python experiment_faults.py calibrate | backfill --recovery plain|rebackfill | control --ack-first
```

`experiment_outage.py`, `experiment_backfill.py` and `experiment_chunks.py` are the earlier experiments; usage is in each docstring. Experiments append to `results/*.csv` and log the consumer to `results/consumer.log`. Only one consumer may run against a slot at a time, and the harnesses refuse to start if one is running.

## Architecture

- `common.py`: the single source of truth for chunking (`chunk_body`, 200 characters, deliberately naive), point ids (`point_id`, uuid5 of article:chunk), timestamp normalisation (`iso_utc`) and connection settings. The consumer and the verifier both import these so they always agree on what the index should contain. The `RAG_*` environment variables redirect the DSN, slot and collection names; only `test_guard.py` sets them.
- `cdc_consumer.py`: reads `test_decoding` text from a replication slot, parses it (`handle()`), and writes chunks to Qdrant. Behaviour is selected by flags and the file's long section comments explain the reasoning. The mechanisms that interact:
  - **Update modes** `trim` (default), `correct`, `naive`: how an UPDATE treats the article's existing chunks.
  - **Alias swap**: readers query the alias `article_chunks`, which points at a physical `article_chunks_<stamp>_<id>`. A backfill builds a new physical collection and repoints the alias when done, so the old index keeps serving.
  - **Backfill marker**: records that a copy completed and which collection the slot feeds. Without it a plain start runs a full backfill rather than stream onto a possibly partial index.
  - **Version guard**: each change's LSN is the article's version, kept in a paired `article_versions_<x>` collection that is swapped together with the chunks. A replayed change with a lower LSN is skipped. The stamp is written last, after the chunks and the trim, so a crash leaves the old version and replay redoes the work (an equal LSN is re-applied on purpose). Deletes leave tombstones that background sweeps remove later. `--repair` and `reindex_article` ignore the guard and stamp with `pg_current_wal_lsn()` taken under `FOR SHARE`.
  - **Rewind detection**: `trace` logs an INFO line per applied or skipped change (LSN and prior version), and `check_rewind` WARNs and counts a change that would apply below the article's version. `REWIND_FATAL=1` makes it raise.
  - **Fault hooks**: `hook(name)` marks stage boundaries, writes `STAGE_FILE`, and SIGKILLs itself at `FAULT`/`FAULT_AT`. Acks are batched (about every 10 s), so after a crash "applied but not acked" is the normal state and replay redelivers recent changes.
- `verify.py`: the oracle. It re-chunks every Postgres row and compares by hash against Qdrant, and reports orphan collections separately (they do not count as inconsistent but do make a trial not `clean`).
- `experiment_faults.py`: the kill -9 harness, built on helpers from `experiment_outage.py` (process start/stop, log markers) and the `Writer` thread from `experiment_backfill.py`. Each trial does: kill, insert a marker row, restart a healthy consumer, wait until the marker is indexed (the barrier), then verify twice. `docs/fault_design.md` has the design and ranked predictions.

## Things that bite

- The trace line deliberately avoids the text `<OP> id=`: the harness counts applied changes by regex (`APPLIED_RE`, `SKIPPED_RE`, `TRACE_RE`, `REWIND_WARN_RE` in `experiment_faults.py`). When changing consumer log wording, check those regexes.
- Scenario names in the CSV encode the options (`-guard`, `-alias` suffixes), and `append_row` moves `results/faults.csv` aside to a timestamped file whenever `FIELDS` changes.
- Harness timeouts follow how a consumer was started: a `--backfill` consumer streams only after the whole copy (`BACKFILL_STARTUP_TIMEOUT_S`), and a rebuild's consumer is reused as the next trial's victim.
- `results/` (run output, ~10 MB) is untracked on purpose.
