# Pipeline issues

## Counting rule and definition of a silent failure

An item counts as an **issue** if it was a defect or gap in the pipeline itself (the consumer, its safeguards, or
its operation), whether it shipped or was caught in review before it could. Measurement tools, deliberate
baselines and negative controls are not pipeline issues; see "Not counted, and why" at the end.

A **silent failure** is an issue that caused data to be lost permanently, or wrong content to be served without
an error. Temporary absence (data missing for a while and then restored by replay or a rebuild) is excluded.
Silent failures are split into two kinds in the last column of the tables:

- **Permanent**: the data stays lost or wrong until someone rebuilds the index.
- **Temporary wrong content**: wrong content is served for a while with no error, then converges.

That gives 19 issues, of which 5 are silent failures (3 permanent, 2 temporary wrong content). 11 shipped and 8
were caught in review.

## Product bugs (5)

| # | Date | Issue | Found by | Status | Silent failure |
|---|---|---|---|---|---|
| 1 | Oct 14 | **Lost final acknowledgment.** psycopg2 only sends acknowledgments about every 10 s, so short-lived consumers never acked and each restart replayed a growing backlog. Fixed by forcing a final flush on shutdown. | Experiment (outage runs) | Shipped, fixed | No |
| 2 | Oct 14 | **Timestamp format mismatch.** The consumer stored `updated_at` in `test_decoding`'s text format, which didn't match Postgres's timestamps. Fixed by storing normalized ISO-8601 UTC. | Design review (flagged during the first spike) | Shipped, fixed | No |
| 3 | Oct 15 | **TOAST placeholder.** A title-only update on a large article delivered the body as the literal text `unchanged-toast-datum`, replacing its chunks while the pipeline logged success. | Design review (suspected), confirmed by experiment | Shipped, fixed | **Yes: permanent** |
| 4 | Oct 28 | **Single-instance lock taken after the slow model load.** A second consumer spent seconds loading the model before exiting. Fixed by taking the lock first. | Experiment (manual test) | Shipped, fixed | No |
| 5 | Oct 31 | **Tombstone sweep race.** The background sweep could delete a tombstone that a newer insert had just replaced, between reading and deleting it. Fixed by re-checking the filter at delete time. | Design review (while building the sweeps) | Caught in review, fixed | No |

## Design: correctness (9)

| # | Date | Issue | Found by | Status | Silent failure |
|---|---|---|---|---|---|
| 6 | Oct 15-28 | **Delete-then-insert gap.** The default `correct` update mode deleted an article's chunks before inserting the new ones, so a crash in between left the article absent from search until replay (regression in 16 of 16 crashes). Replaced by insert-then-trim on Oct 28. | Experiment | Shipped, fixed | No (temporary absence) |
| 7 | Oct 15-24 | **Backfill crash plus restart.** The pipeline treated the replication slot's existence as "backfill finished", so a crash mid-copy followed by a restart skipped straight to streaming and permanently lost the uncopied rows (20 of 20 trials, median ~6,200 chunks). Fixed by the completion marker. | Experiment (suspected in review Oct 15, confirmed Oct 23) | Shipped, fixed | **Yes: permanent** |
| 8 | Oct 24 | **First marker fix was incomplete.** It only rebuilt if the slot existed. Corrected to "no marker means rebuild". | Design review | Caught in review, fixed | No |
| 9 | Oct 26 | **Orphan sweep would delete the marker's collection.** A general orphan sweep would have deleted the collection the completion marker points to, breaking roll-forward recovery. | Design review | Caught in review, fixed | No |
| 10 | Oct 28-30 | **Replay rewinds.** After a crash, replaying an older, shorter version of an article overwrote newer content (5 of 17 trials without the guard, 0 of 17 with it). Fixed by the version guard. | Experiment | Shipped, fixed | **Yes: temporary wrong content** |
| 11 | Oct 30 | **The guard removes replay's self-healing.** A bad write stamped with a version could no longer be repaired by replay. Led to strictly-older skips, stamping last, and `--repair`. | Design review | Caught in review, fixed | No |
| 12 | Oct 30 | **Resurrected deleted articles.** Replayed old updates bring deleted articles back until the replayed delete arrives. Fixed by tombstones. | Design review, confirmed by `test_tombstone` | Shipped, fixed | **Yes: temporary wrong content** |
| 13 | Oct 30 | **Version-sentinel points would leak into search.** Sentinel points in the search collection would appear in results. Replaced by a separate, paired versions collection. | Design review | Caught in review, fixed | No |
| 14 | Nov 4 | **In-process rewind detector loses history on restart.** It can't see rewinds that cross a restart. A known limitation; the harness's cross-process count is the real measurement. | Design review | Shipped, known limitation | No |

## Design: operational (4)

| # | Date | Issue | Found by | Status | Silent failure |
|---|---|---|---|---|---|
| 15 | Oct 15-26 | **Empty rebuild window.** Rebuilds left the index empty for about a minute while copying. Fixed by alias-swap rebuilds. | Experiment (flagged Oct 15) | Shipped, fixed | No (temporary absence) |
| 16 | Oct 26 | **Orphaned collections undetected.** Crashes mid-rebuild could leave orphaned collections with nothing to detect them. Added a verifier check and a sweep. | Design review | Caught in review, fixed | No |
| 17 | Oct 26-28 | **Single consumer assumed but not enforced.** Parallel consumers or backfills could conflict. Fixed by an advisory lock. | Design review | Caught in review, fixed | No |
| 18 | Oct 30-31 | **Startup sweeps delayed recovery.** Sweeps ran before streaming, delaying recovery after every crash and risking a blocked startup. Moved to bounded, non-fatal background work. | Design review | Caught in review, fixed | No |

## Design: missing functionality (1)

| # | Date | Issue | Found by | Status | Silent failure |
|---|---|---|---|---|---|
| 19 | Oct 15 | **Pre-existing rows never indexed.** Rows that existed before the replication slot was created were never indexed, since the spike only streamed changes. Fixed by snapshot-then-stream backfill. | Design review, confirmed by the verifier | Shipped, fixed | **Yes: permanent** |

## Silent failures at a glance

| Kind | Issues |
|---|---|
| Permanent | #3 TOAST placeholder, #7 backfill crash plus restart, #19 pre-existing rows never indexed |
| Temporary wrong content | #10 replay rewinds, #12 resurrected deleted articles |

Delete-then-insert (#6) and the empty rebuild window (#15) are counted as issues but not as silent failures: they
caused temporary absence, not wrong content.

## Not counted, and why

- **Naive chunking mode (`naive`).** A deliberate baseline, kept to show what plain upsert does when text gets
  shorter (orphan chunks). It was never the intended behavior.
- **Copy-then-stream backfill (`select-first`).** A negative control in `experiment_backfill.py` that is expected
  to fail: it copies the rows and only then creates the slot, which leaves a gap, and the harness shows the loss.
- **Slot-first backfill (`slot-first`).** A deliberate test variant in `experiment_backfill.py`, not a pipeline
  flaw. It creates the slot and then runs a plain `SELECT`, so it overlaps rather than leaving a gap, and because
  the consumer is idempotent it converges to the correct result. It was the first attempt at a negative control and
  could not fail, which is why `select-first` was built.
- **Ack-first consumer.** A negative control in `experiment_faults.py` (`control --ack-first`), which must show lost
  changes or the harness can't see losses.
- **Production rewind detection.** A measurement improvement (the `trace` lines and `check_rewind`), not a fix for
  a defect. Its restart limitation is counted separately as #14.
- **Harness bugs.** Bugs in the experiment tooling are defects in measurement tools, not in the pipeline.
- **Experiment-design, essay-analysis and time-analysis rows.** Out of scope: they concern how experiments were
  designed, how results were written up, or how time was spent on the project, not how the pipeline behaves.
