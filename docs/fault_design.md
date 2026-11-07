# Fault-injection design

Random `kill -9` experiments against the CDC consumer, Jepsen-style. This describes the design agreed before the harness was built (`experiment_faults.py`).

Three properties of the current code shape the design:

1. **Embedding runs between the delete and the upsert.** In `handle()`, `delete_article_points()` runs first. `model.encode()` then runs inside `upsert_chunks()`, before the upsert. The delete-to-insert gap is tens of milliseconds, not just an HTTP round trip.
2. **Acks are batched.** `send_feedback()` is only transmitted about every 10 s. After any kill -9, Postgres replays up to roughly 10 s of changes that were already applied. "Applied but not acked" is the normal state at crash time, not a rare window.
3. **The backfill cleanup can't run on SIGKILL.** The `except BaseException` that drops the slot never executes on kill -9. Its comment says the cleanup exists so a half-built index can't look healthy after a plain restart. That guard covers Ctrl+C but not kill -9.

## 1. Crash points, ranked by likelihood of inconsistency

Two outcomes are kept separate:

- **Persistent**: still wrong after restart and catch-up. This is a real bug.
- **Transient**: wrong while the consumer is down or replaying. This is a freshness or availability cost.

| Rank | Crash point | Where a random busy-time kill lands | Predicted persistent | Predicted transient |
|---|---|---|---|---|
| 1 | Mid-backfill copy, then plain restart | Backfill is rare, so only if the kill is aimed there | ~100% | Missing chunks until a rebuild |
| 2 | Between delete and insert (correct mode) | Most kills, an estimated 60-80% of busy time | 0% | The article has zero points until restart and replay finish (seconds) |
| 3 | Partway through a multi-chunk upsert (>256 chunks) | Never at current sizes | 0% | The article is partly indexed, which is worse for retrieval than absent |
| 4 | Applied but not acked | Every kill | 0% in correct mode | Replay briefly rewinds the index: old versions reappear, deleted rows resurrect, then it converges |
| 5 | Payload-only refresh (unchanged TOAST) | Under 1% (one `set_payload` call, no embedding) | 0% | Tiny |

**Rank 1.** The kill leaves the slot behind. A plain restart calls `ensure_slot()`, sees the slot exists, and streams from `consistent_point` onto a half-built collection. Every row not yet copied and not touched since stays missing forever. Lag looks normal, so only the verifier notices. Recovery by re-running `--backfill` should give 0%, so recovery policy (plain restart vs re-backfill) is an experiment parameter.

Within rank 1 there is a benign sub-window: killed after the copy but before `start_replication`. Predicted 0% persistent there.

**Rank 2.** Replay redoes delete and insert, and point ids are deterministic. That is why persistent inconsistency should be 0%.

**Rank 3.** Bodies are at most about 30 chunks (3,500-6,000 characters) and the upsert batch size is 256, so a single request covers them and a partial upsert is impossible at current sizes. Testing it needs a tunable batch size (`UPSERT_BATCH`).

**Rank 4.** Replay is in order, so the final state converges. Verifier sampling during catch-up should see the time-travel.

**Rank 5.** There was a measurement gap: `verify.py` compared only chunk hashes, never the `updated_at` payload, so a bad refresh was invisible. A `payload_stale` category closes it. `set_payload` on a filter that matches zero points is also a silent no-op.

**Prediction.** Steady-state random kills: 0 persistent failures with substantial transient exposure. Backfill is the only deterministic failure. If steady-state kills do produce persistent failures, the model above is wrong.

Two further scenarios:

- **Kill during recovery** (a second kill while replaying).
- **Graceful SIGTERM** as a control, expecting zero replay.

## 2. Injection method

One small hook, `hook("name")`, called at each stage boundary in the consumer. It does two jobs:

1. If `STAGE_FILE` is set, it writes the stage name to that file. A kill -9 can't lose that write.
2. If `FAULT=name` and `FAULT_AT=n` are set, it sends `SIGKILL` to itself on the nth hit.

**Targeted kills** use the `FAULT` env vars. A self-SIGKILL has the same OS-level effect as an external `kill -9`: no handlers, no flush, sockets closed by the kernel.

**Random kills** use an external `kill -9` at a uniformly random time. The harness first writes a backlog while the consumer is busy, so kills don't land while it is idle. It then reads `STAGE_FILE` after the kill, which gives each random kill a stage label. That checks the predicted share of kills per stage instead of assuming it.

Hook points:

| Hook | Boundary |
|---|---|
| `recv` | Message received, before `handle()` |
| `before_delete` | About to delete the article's points |
| `after_delete` | Delete done; embedding and upsert follow |
| `after_embed` | Embedding done; upsert in flight |
| `mid_upsert` | Between upsert batches |
| `after_upsert` | All batches upserted |
| `before_set_payload` | Payload-only refresh in flight |
| `after_set_payload` | Refresh done |
| `before_ack` | Change fully applied, ack not yet sent |
| `acked` | Ack sent |
| `backfill_mid_copy` | After each backfill batch |
| `backfill_pre_stream` | Copy done, before `start_replication` |

**Limitation.** Failpoints fire at call boundaries, so they can't test a request that was sent but whose response was lost. The random external kills cover that case, and the stage labels show how often it happens. A TCP proxy that kills on specific requests could target it directly; that is left for a later phase.

## 3. Measurement

Each trial runs against a long-lived healthy consumer:

1. Write a seeded burst of changes (mixed insert/update/delete, with big TOAST bodies).
2. Kill the consumer, either at the failpoint or at a random time.
3. Confirm the exit status was -9.
4. Restart the consumer cleanly.
5. **Barrier**: insert a marker row, then wait until it appears in Qdrant. The consumer processes in WAL order, so once the marker is indexed, everything earlier has been replayed.
6. Run `verify.check()` and record missing, orphaned, stale and payload-stale counts. Re-check 3 s later to confirm the result is stable.

The barrier matters. Replay can pass through a briefly consistent state, such as when the first replayed change matches current Postgres, so polling for "consistent" would call that "caught up" too early.

Metrics per trial:

| Kind | Metrics |
|---|---|
| Persistent | Counts by type; whether the trial is clean |
| Transient | Verify sampled every 250 ms from kill to barrier: max missing/stale/orphaned, seconds inconsistent |
| Attribution | Stage the kill landed in; changes applied before the kill and after restart |
| Repro | The seed |

If a trial ends dirty, the harness records it, runs a full `--backfill` to reset, and continues. That keeps trials independent.

Negative controls:

- `--ack-first`: acks before applying, with a forced flush. It should lose changes at a high rate. If the harness reports 0% there, the harness is broken.
- `--mode naive`: its orphans are not crash-related.

### How many runs

Rates need k/n and a Wilson or Clopper-Pearson interval, not a bare percentage.

| Goal | Runs |
|---|---|
| Deterministic mechanisms (backfill kill, `ack-first`): expect ~0% or ~100% | 20 per scenario. If results are mixed, extend. |
| "Zero failures" claims: 0/n gives a 95% upper bound of about 3/n | 100 gives <3%; 300 gives <1% |
| Estimating a rate near 5% | +/-4.3 points at n=100; +/-2.5 at n=300; +/-1.4 at n=1000 |
| Telling two designs apart on failure rate (10% vs 5%, 80% power) | ~430 per arm, which is impractical |

Designs are therefore compared on **transient exposure** (seconds inconsistent, max missing), a continuous measure, with about 40 trials per arm. Failure rates are reserved for the "does it ever persist" question.

Each trial costs about two consumer launches, so a calibration step measures startup time and per-stage durations first. That also tests the window-width predictions.

## 4. What we'd learn

**Real bugs:**

- Any persistent inconsistency in steady-state kills where 0% is predicted. That would be a hole in the at-least-once and idempotence argument.
- Persistence that survives a second clean restart.
- A non-zero rate from the benign `backfill_pre_stream` window.

**Expected, not bugs:**

- Backfill-kill with plain restart. It is a design gap, but one the code already half-documents.
- Naive-mode orphans.
- Transient exposure.

**Design changes to compare** (after the baseline runs):

| Variant | Idea | What to compare |
|---|---|---|
| Insert-then-trim | Upsert the new chunks, then delete points with `chunk_index >= new_count`. The article never has zero points; a crash leaves a stale tail instead. | Missing-seconds vs orphan-seconds; whether mixed old/new chunks are worse for retrieval than a gap |
| Backfill completion marker | The consumer refuses to stream unless a "backfill complete" sentinel exists, and otherwise forces a re-backfill. Heavier alternative: build into a new collection and swap an alias at the end. | Persistent failure rate of backfill-kill with plain restart |
| Versioned chunks | Tag points with a generation, upsert the new one, then delete the old. Needs reader cooperation. | Deferred |

## 5. Commands

```
python experiment_faults.py calibrate
python experiment_faults.py steady --kill random --trials 300 --seed 1
python experiment_faults.py point --at after_delete --trials 100
python experiment_faults.py point --at mid_upsert --upsert-batch 4 --trials 100
python experiment_faults.py point --at before_set_payload --trials 100
python experiment_faults.py backfill --kill-frac 0.5 --recovery plain --trials 20
python experiment_faults.py backfill --kill-frac 0.5 --recovery rebackfill --trials 20
python experiment_faults.py control --ack-first --trials 100
python experiment_faults.py summarize results/faults.csv
```

Add `--variant insert-trim` to compare update strategies.

## Code changes this needs

- The `hook()` function and its call sites in `cdc_consumer.py`. It is a no-op unless the env vars are set.
- A tunable `UPSERT_BATCH` env var, defaulting to 256.
- A hidden `--ack-first` control.
- The `updated_at` check in `verify.py`.
- The new `experiment_faults.py`.

Defaults are unchanged, so existing experiments behave as before.

## Implementation status and deviations

Built: the hook and call sites, `UPSERT_BATCH`, `--ack-first`, the `payload_stale` check, and `experiment_faults.py` with `calibrate`, `backfill`, `control --ack-first` and `steady --kill random`. Deferred: the `point` subcommands, `summarize` (each run prints its own k/n and interval), and the insert-trim variant.

Deviations from the design above:

- **`--ack-first` acks the COMMIT, not each change.** Acking a change message's own LSN does not skip its transaction, because Postgres replays any transaction whose commit is past the acked LSN. The control therefore holds a transaction's changes, force-acks the COMMIT, and then applies them.
- **Backfill kills pick a random batch.** `backfill` replaces `--kill-frac` with a random choice among the backfill batches (200 articles each), always leaving at least one batch uncopied.
- **Burst kills use a time window.** A writer thread starts and the victim is killed after a random 0.05 to `--window` seconds (default 3), while it is still working through the backlog.
