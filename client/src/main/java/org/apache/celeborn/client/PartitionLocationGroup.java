/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

/**
 * A thin wrapper of the writable PartitionLocation(s) of one (shuffleId, partitionId).
 *
 * <p>In the common case (the partition never splits) it only holds a single volatile {@link
 * PartitionLocation} reference; the parallel state (active location list and retired epochs) is
 * inflated lazily on the first SOFT_SPLIT/HARD_SPLIT/push failure, or when a revive response
 * delivers more than one active location. Selection is uniform over the writable subset —
 * non-retired and soft-split locations, which stay writable until they hard-split — via {@code
 * mapId % writableCount}.
 */
public class PartitionLocationGroup {

  /**
   * The only location before inflation; intentionally NOT kept in sync after inflation — the
   * inflated {@link ParallelState#active} list is the source of truth.
   */
  private volatile PartitionLocation single;

  /** null until inflated; all mutating methods go through double-checked inflation. */
  private volatile ParallelState parallel;

  public PartitionLocationGroup(PartitionLocation loc) {
    this.single = loc;
  }

  /** Fast path: returns the single location when not inflated, zero extra cost. */
  public PartitionLocation currentFor(int mapId) {
    return pick(mapId, -1);
  }

  /**
   * A writable location for {@code mapId}; when every known location is locally retired, falls back
   * to the latest (possibly retired) one. This mirrors the baseline path, which never retires
   * locally and keeps pushing the possibly-dead location until the worker rejects it and the reject
   * drives the next revive round.
   */
  public PartitionLocation currentOrLatest(int mapId) {
    PartitionLocation loc = currentFor(mapId);
    return loc == null ? latest() : loc;
  }

  /**
   * Pick a writable location for {@code mapId}, skipping {@code excludeEpoch} (-1 = skip nothing);
   * null when nothing is writable. The active list is snapshotted because a concurrent full-set
   * merge may shrink it.
   */
  private PartitionLocation pick(int mapId, int excludeEpoch) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      return (loc != null && loc.getEpoch() != excludeEpoch) ? loc : null;
    }
    // Snapshot the active list: a concurrent full-set merge may shrink it via removeIf, so
    // size()-then-get() on the live list races. Collect the writable subset in a single pass
    // (two passes read the retired CHM twice and raced with a concurrent retire/eviction).
    PartitionLocation[] snapshot = p.active.toArray(new PartitionLocation[0]);
    List<PartitionLocation> writable = new ArrayList<>();
    for (PartitionLocation loc : snapshot) {
      if (isWritable(p, loc, excludeEpoch)) {
        writable.add(loc);
      }
    }
    if (writable.isEmpty()) {
      return null;
    }
    int start = Math.floorMod(mapId, writable.size());
    return writable.get(start);
  }

  private static boolean isWritable(ParallelState p, PartitionLocation loc, int excludeEpoch) {
    if (loc.getEpoch() == excludeEpoch) {
      return false;
    }
    StatusCode cause = p.retired.get(loc.getEpoch());
    return cause == null || cause == StatusCode.SOFT_SPLIT;
  }

  /** The location with the max epoch, used where a single representative location is needed. */
  public PartitionLocation latest() {
    ParallelState p = parallel;
    if (p == null) {
      return single;
    }
    // Snapshot for the same reason as pick(): a concurrent full-set eviction may shrink the
    // list between the emptiness check and the last-element access.
    PartitionLocation[] snapshot = p.active.toArray(new PartitionLocation[0]);
    return snapshot.length == 0 ? single : snapshot[snapshot.length - 1];
  }

  public int maxEpoch() {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      return loc == null ? -1 : loc.getEpoch();
    }
    return p.maxEpoch;
  }

  /**
   * A retired epoch still present in the active list, i.e. not yet confirmed digested by the
   * LifecycleManager. Attached to the batched revive at send time so the LM's gap-based allocation
   * sees the real active-set size.
   */
  static final class OutstandingRetire {
    final PartitionLocation location;
    final StatusCode cause;

    OutstandingRetire(PartitionLocation location, StatusCode cause) {
      this.location = location;
      this.cause = cause;
    }
  }

  /**
   * Snapshot of {@link OutstandingRetire}s, epoch ascending. Only epochs still in the active list
   * are included — an evicted epoch has already been digested by the LM.
   */
  List<OutstandingRetire> outstandingRetires() {
    ParallelState p = parallel;
    if (p == null) {
      return new ArrayList<>(0);
    }
    List<OutstandingRetire> retires = new ArrayList<>();
    for (PartitionLocation loc : p.active.toArray(new PartitionLocation[0])) {
      StatusCode cause = p.retired.get(loc.getEpoch());
      if (cause != null) {
        retires.add(new OutstandingRetire(loc, cause));
      }
    }
    return retires;
  }

  /**
   * Mark {@code epoch} as retired with {@code cause}; soft-retired locations stay writable, hard
   * ones are skipped by routing. A non-soft cause upgrades a previous SOFT_SPLIT retire and is
   * never downgraded back. Synchronized so the tombstone write cannot interleave with the full-set
   * cleanup in {@link #mergeActiveLocations}.
   *
   * @return true if this is the first time the epoch is retired (dedupe signal for the caller).
   */
  public synchronized boolean retire(int epoch, StatusCode cause) {
    ParallelState p = inflateIfNeeded();
    boolean[] firstRetire = {false};
    p.retired.compute(
        epoch,
        (k, existing) -> {
          if (existing == null) {
            firstRetire[0] = true;
            return cause;
          }
          return existing == StatusCode.SOFT_SPLIT && cause != StatusCode.SOFT_SPLIT
              ? cause
              : existing;
        });
    return firstRetire[0];
  }

  /**
   * Converge to the active set delivered by the LifecycleManager: add locally missing epochs, never
   * re-add retired ones. With {@code fullSet}, retired epochs the LM no longer reports are dropped
   * from both the active list and the retired map. Synchronized for atomic check-then-add across
   * concurrent revive responses.
   */
  public synchronized void mergeActiveLocations(
      List<PartitionLocation> locations, boolean fullSet) {
    if (locations == null || locations.isEmpty()) {
      return;
    }
    if (parallel == null && locations.size() == 1) {
      // Stay in thin-wrapper mode when the LM only knows one active location.
      updateLatest(locations.get(0));
      return;
    }
    ParallelState p = inflateIfNeeded();
    for (PartitionLocation loc : locations) {
      if (loc == null || p.retired.containsKey(loc.getEpoch())) {
        continue;
      }
      insertActive(p, loc);
      if (loc.getEpoch() > p.maxEpoch) {
        p.maxEpoch = loc.getEpoch();
      }
    }
    if (fullSet && !p.retired.isEmpty()) {
      Set<Integer> reported = new HashSet<>();
      for (PartitionLocation loc : locations) {
        if (loc != null) {
          reported.add(loc.getEpoch());
        }
      }
      p.active.removeIf(
          loc -> p.retired.containsKey(loc.getEpoch()) && !reported.contains(loc.getEpoch()));
      p.retired.keySet().removeIf(epoch -> !reported.contains(epoch));
    }
  }

  /**
   * Legacy single-location update (adaptive parallelism disabled or single-location response).
   * Synchronized because revive responses can be applied concurrently by the ReviveManager
   * scheduler thread and by push threads via the blocking revive path, so the non-inflated
   * check-then-set on {@link #single} must be atomic.
   */
  public synchronized void updateLatest(PartitionLocation loc) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation cur = single;
      if (cur == null || loc.getEpoch() >= cur.getEpoch()) {
        single = loc;
      }
    } else {
      List<PartitionLocation> one = new ArrayList<>(1);
      one.add(loc);
      mergeActiveLocations(one, false);
    }
  }

  /** Visible for testing and observability logging. */
  int activeCount() {
    ParallelState p = parallel;
    return p == null ? (single == null ? 0 : 1) : p.active.size();
  }

  /** Epochs of the active list, ascending — for diagnostics in failure messages. */
  List<Integer> activeEpochsSnapshot() {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      List<Integer> epochs = new ArrayList<>(1);
      if (loc != null) {
        epochs.add(loc.getEpoch());
      }
      return epochs;
    }
    List<Integer> epochs = new ArrayList<>(p.active.size());
    for (PartitionLocation loc : p.active) {
      epochs.add(loc.getEpoch());
    }
    return epochs;
  }

  /** Retired epochs and their causes, for diagnostics in failure messages. */
  List<String> retiredEpochsSnapshot() {
    ParallelState p = parallel;
    if (p == null) {
      return new ArrayList<>(0);
    }
    List<String> retires = new ArrayList<>(p.retired.size());
    for (Map.Entry<Integer, StatusCode> entry : p.retired.entrySet()) {
      retires.add(entry.getKey() + "=" + entry.getValue());
    }
    retires.sort(Comparator.comparing(s -> Integer.parseInt(s.substring(0, s.indexOf('=')))));
    return retires;
  }

  private ParallelState inflateIfNeeded() {
    ParallelState p = parallel;
    if (p == null) {
      synchronized (this) {
        p = parallel;
        if (p == null) {
          p = new ParallelState();
          PartitionLocation loc = single;
          if (loc != null) {
            p.active.add(loc);
            p.maxEpoch = loc.getEpoch();
          }
          parallel = p;
        }
      }
    }
    return p;
  }

  private static void insertActive(ParallelState p, PartitionLocation loc) {
    List<PartitionLocation> active = p.active;
    int i = 0;
    while (i < active.size() && active.get(i).getEpoch() < loc.getEpoch()) {
      i++;
    }
    if (i < active.size() && active.get(i).getEpoch() == loc.getEpoch()) {
      // Already present (location objects are immutable per epoch).
      return;
    }
    p.active.add(i, loc);
  }

  /** Parallel state, only exists for partitions that ever split or have multiple locations. */
  private static class ParallelState {
    // Active locations, sorted by epoch ascending. Includes soft-retired ones, which stay
    // writable until they hard-split.
    final CopyOnWriteArrayList<PartitionLocation> active = new CopyOnWriteArrayList<>();
    // epoch -> retire cause
    final ConcurrentHashMap<Integer, StatusCode> retired = new ConcurrentHashMap<>();
    volatile int maxEpoch = -1;
  }
}
