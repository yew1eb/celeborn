# [CELEBORN-2465][SPARK] Proactively unregister shuffles on SQL query completion

> Status: Proposal (no code changes) · Jira: [CELEBORN-2465](https://issues.apache.org/jira/browse/CELEBORN-2465) · Baseline: Celeborn 0.7.0-SNAPSHOT · Related: [TODOList.md §2.6](TODOList.md)

## Problem

In long-running Spark applications (ThriftServer, Kyuubi, `spark-sql`), one application runs
many SQL queries, each producing new shuffles. Celeborn currently has no per-query shuffle
cleanup:

- `SparkShuffleManager.unregisterShuffle()` is only triggered by Spark's GC-driven
  `ContextCleaner`, i.e. after the `ShuffleDependency` is garbage-collected on the driver.
  In long sessions it can be retained in the heap indefinitely due to DAG/query-plan
  reference chains (see CELEBORN-2244), so obsolete shuffle data piles up on Celeborn
  worker disks for the whole session lifetime.
- The only fallback is application-level cleanup on driver stop (`ApplicationLost`, or the
  master-side app heartbeat timeout).

Since Celeborn is a long-lived shared service, this accumulation is much more costly than
executor-local shuffle disks.

## Proposal

Add an opt-in config (default `false`):

  celeborn.client.spark.sql.queryEndShuffleCleanup.enabled

When enabled, register a `SparkListener` (via the existing `SparkUtils.addSparkListener`
pattern, as used for `ShuffleFetchFailureReportTaskCleanListener`) that handles
`SparkListenerSQLExecutionEnd` and proactively unregisters all shuffles written by that
query.

Why this is safe without any recovery mechanism: at query completion, all of the query's
stages (including AQE-created ones) have finished and all reads are done; exchange reuse
only happens within a single query; each new SQL statement allocates fresh shuffle IDs.
So there is no "too-early deletion" window — unlike stage-level early deletion
(CELEBORN-2244 / PR #3569), no dependency tracking or auto-recovery is needed.

## Design

1. On `SparkListenerSQLExecutionEnd`: get the `QueryExecution` from the event's `qe`
   field (populated on both success and failure paths), traverse the final executed plan
   (handling AQE), and collect the shuffle IDs of all `ShuffleExchangeExec`s written by
   this query.
2. For each collected shuffle ID (skipping ones not registered in the LifecycleManager),
   trigger the standard Spark cleanup entry point `shuffleDriverComponents.removeShuffle`,
   which flows through `CelebornShuffleDriverComponents` → `RemoveShuffle` broadcast →
   driver-side `SparkShuffleManager.unregisterShuffle` → `LifecycleManager.unregisterAppShuffle`
   → existing delayed-unregister → worker disk deletion. No master/worker/protocol changes.
3. The listener bus is `SparkContext`-level, so one registration covers all ThriftServer
   sessions; no Spark-side configuration needed.

## Known Limitations

- If the same DataFrame reference is executed again (or a cached DataFrame's lineage
  references a cleaned shuffle and the cache is evicted), the shuffle must be regenerated
  via stage retries. Strongly recommend enabling `celeborn.client.stage.rerun.enabled`
  together, and document this clearly. Acceptable for an opt-in feature.
- RDD API jobs are not covered (no `SparkListenerSQLExecutionEnd`); the target scenario
  is long-lived SQL sessions.

## Test Plan

- Multiple SQL queries with shuffles in one session: after each query, its shuffles are
  unregistered and deleted from workers, while later queries are unaffected.
- Config disabled (default): no cleanup.
- Failed query: only actually-registered shuffles are cleaned.
