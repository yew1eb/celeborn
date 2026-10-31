# CIP: Batched Heartbeat Raft Writes

> Status: Draft — Jira: [CELEBORN-2429](https://issues.apache.org/jira/browse/CELEBORN-2429)
> Implementation branch: `CELEBORN-2429`

## Motivation

In HA mode, every worker and application heartbeat RPC reaching the master leader becomes an
individual raft log entry: one proposal, one majority replication + fsync, one apply dispatch —
and the leader replies only after majority commit. Heartbeats are the most frequent metadata
writes: with A running apps and W workers, the raft write rate is ≈ `A/10 + W/30` entries/s
(app HB every 10s, worker HB every 30s).

Two observations from one of our production clusters at peak (900 running apps + 200 workers ≈
97 heartbeat entries/s) show the cost:

1. **Apply lag tracks running apps.** `RatisApplyCompletedIndexDiff` (how far followers lag the
   leader's `lastAppliedIndex`) grows in lockstep with `RunningApplicationCount`, from ~300 to
   1000+ — the lag is driven by raft write volume, i.e. by app count. It is self-reinforcing:
   slow apply delays snapshots and log purging, so segments pile up in heap and young GC slows
   apply further.

   ![RatisApplyCompletedIndexDiff vs RunningApplicationCount](TODO-screenshot-apply-lag.png)

2. **Per-entry pipeline is the hotspot, not apply compute.** Follower flame graph at peak
   (546 samples): `appendEntries` netty processing ~35%, `SegmentedRaftLogWorker` flush+fsync
   ~20%, `RetryCache` ~3%, while the apply thread (`StateMachineUpdater`) is only ~7.7%.
   Cutting the number of entries is the right lever.

   ![Master follower flame graph](TODO-screenshot-flamegraph.png)

Crucially, a heartbeat is the wrong operation to pay a synchronous raft commit for: periodic,
self-healing (re-sent next interval), timeout-tolerant (worker 120s / app 300s), and its reply
is built entirely from leader-local memory (`registeredShuffleKeys`, `expiredShuffleKeys`,
`workerEvent`, `excludedWorkers`, …) — never from the raft result. Synchronous replication buys
no business value and pays the full pipeline cost ~arrival-rate times per second. Unlike
`RequestSlots` (job submission critical path), heartbeats are the only high-frequency raft
writes — and the right target.

## Public Interfaces

**New protobuf messages (internal HA replication protocol):** `Type.BatchHeartbeat = 31` /
`PbMetaRequestType.BatchHeartbeat = 31`, field `batchHeartbeatRequest = 27` — identical numbers
in both protos, per the existing dual-proto convention (leader writes `ResourceRequest`,
followers parse `PbMetaRequest`). Payload `BatchHeartbeatRequest` / `PbBatchHeartbeatRequest`:
`repeated WorkerHeartbeatRequest workerHeartbeats = 1;
 repeated AppHeartbeatRequest appHeartbeats = 2;`
Each child heartbeat keeps its own receive-time `time` field, never rewritten.

**New configurations** (all under `celeborn.master.ha.heartbeat.batch.*`):

| Configuration | Default | Description |
|---|---|---|
| `...batch.enabled` | `false` | Feature switch; enable only after ALL masters run this version. |
| `...batch.interval` | `1s` | Flush window; larger windows only risk losing ≤1 window of heartbeat metadata on failover (negligible vs. 120s/300s timeouts). |

## Proposed Changes

### One flush window = one raft log entry

```
 Worker HB ──┐
 Worker HB ──┤  handleWorkerHeartbeat / handleAppHeartbeat
 App HB    ──┼──► enqueue asynchronously, reply immediately
 App HB    ──┘  (reply content comes from leader local memory, never waits for raft)
                                      │
                              HeartbeatAggregator
                   latest-pending maps, one pending entry
                       per worker/app (short-lock put)
                                      │ flush: every 1s tick
                                      ▼
                 one ResourceRequest { cmdType=BatchHeartbeat,
                     workerHeartbeats[N], appHeartbeats[M] }
                                      │ submitRequest
                   ┌──────────────────┼──────────────────┐
                   ▼                  ▼                  ▼
             Leader apply      Follower apply      Follower apply
        (same single raft entry; MetaHandler expands and applies
         the N+M child heartbeats in order on every node)

 Peak second: ~97 heartbeats → 1 entry/s
   = 1 replication + 1 fsync + 1 apply dispatch
```

### HeartbeatAggregator (new leader-side component)

- Pending heartbeats are kept **per worker/app key** (a `HashMap` put under a short lock)
  instead of in a queue, so the buffer is bounded intrinsically — at most one pending heartbeat
  per worker/app (one heartbeat round; ≈1k entries at production peak, ≈10k at 10000 apps), no
  matter how long a flush stalls on the blocking raft submit. Offer never blocks and never
  fails. Apps cannot newly register while the submit is stalled (registration shares the same
  blocking raft write), so the key set stays close to the live workers/apps.
- The newest heartbeat per key wins (apply is overwrite-semantic). On a duplicate app key —
  only possible when a flush stalls longer than the app heartbeat interval — the older counter
  deltas are dropped; this only skews cluster-level metrics counters, since quota enforcement
  (`userResourceConsumption`) is updated through the leader-local bypass, not raft.
- Purely time-driven: one scheduled flush thread drains the maps under the lock and submits
  one batch per tick outside it, never waiting to fill. Standby flushes discard (uncommittable,
  re-sent next interval).
- Failures: a failed submit (e.g. leadership change) drops and logs the batch — heartbeats
  self-heal next interval; `flushSafely` catches `Throwable` so the scheduled task survives;
  each batch carries a fresh `MasterClient.genRequestId()` for retry-cache compatibility. A
  submit slower than the flush interval logs a warning — the signal that raft is the
  bottleneck and windows are stretching (naturally rate-limited to one per flush cycle).

### Apply side

`MetaHandler` shares the `handleWorkerHeartbeat` / `handleAppHeartbeat` helpers between the
single-entry and `BatchHeartbeat` cases — one source of field mapping, so the paths cannot
drift. Expansion runs serially in the existing apply executor; per-heartbeat semantics and
ordering are unchanged.

### Quantified benefit

With A applications, W workers and flush interval T:

```
entries/s before ≈ A/10 + W/30        entries/s after = 1/T
reduction factor ≈ (A/10 + W/30) × T
```

| Window T | A=1000, W=200 (~107/s) | Reduction |
|---|---|---|
| 100ms | 10 entries/s | ~11x |
| **1s (default)** | **1 entry/s** | **~107x** |
| 2s | 0.5 entries/s | ~214x |

At 10000 apps (≈1000 heartbeats/s) each flush still yields a single entry (~1000x). Note the
`updateXxxMeta` call count is unchanged — only the per-entry pipeline overhead is removed,
which is exactly where the flame-graph hotspots are.

## Compatibility, Deprecation, and Migration Plan

- **Fully additive, off by default** — with `...batch.enabled=false` every path is the
  pre-existing synchronous one.
- **Mixed-version hazard**: an old follower does **not** crash on a `BatchHeartbeat` entry —
  proto3 reads `cmdType=31` as `UNRECOGNIZED`, the error is caught in
  `MetaHandler.handleWriteRequest`, and the entry is skipped while its log index advances.
  Result: **silent metadata divergence** (stale `workersMap`/`appHeartbeatTime` on followers,
  no crash to warn you), dangerous if a stale follower becomes leader.
- **Rollout**: upgrade all masters with the flag `false`, then enable cluster-wide and
  rolling-restart. **Rollback**: flag back to `false` and rolling-restart; written batched
  entries stay parseable. Nothing is deprecated; the per-heartbeat path remains the default.

## Test Plan

- `MasterStateMachineSuiteJ#testBatchHeartbeat`: a batched entry applies correctly (each
  child's own `time` honored) and survives the `ResourceRequest` → `PbMetaRequest` wire
  round-trip.
- `HeartbeatAggregatorSuiteJ` (new, single-node raft, full offer → submit → replicate →
  apply chain): N offers produce far fewer entries; empty windows produce none; duplicate
  keys within one window collapse to the newest heartbeat.
- Regression: existing master suites pass with the feature disabled and enabled; mixed-version
  behavior covered by rollout discipline + manual rolling-upgrade verification.

## Rejected Alternatives

- **Batch but wait for commit before replying** (synchronous group commit): batches entries like
  this proposal, but each heartbeat reply must wait for its window's batch to be committed by
  the raft majority — adding up to ~1 window (~1s) of latency to every reply and parking RPC
  threads — while gaining nothing over async enqueue: heartbeats are re-sent every interval,
  so no individual heartbeat needs a durability guarantee.
- **Soft-state heartbeats** (as in Kafka's quorum controller): another order of magnitude, but
  failover view-recovery (checkpoint + election grace period) needs its own design — future
  evolution, not this CIP.
- **Parallel/sharded apply**: replicas of one state machine must apply in identical order;
  sharding breaks global ordering of cross-worker operations.
- **Bounded queues with rejection**: any fixed capacity is an arbitrary number to defend, and
  overflow either fails heartbeat RPCs (a failure mode workers never see in healthy operation)
  or drops silently; a per-key pending map is bounded intrinsically by the number of live
  workers/apps — no number to tune, no reject path.
