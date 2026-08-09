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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

/**
 * A thin wrapper of the writable PartitionLocation(s) of one (shuffleId, partitionId).
 *
 * <p>In the common case (the partition never splits) it only holds a single volatile {@link
 * PartitionLocation} reference, so the fast path costs exactly one extra object header compared to
 * storing the location directly. The parallel state (active location list and retired epochs) is
 * inflated lazily on the first SOFT_SPLIT/HARD_SPLIT/push failure, or when a revive response
 * delivers more than one active location.
 *
 * <p>Selection policy: {@code mapId % activeCount}, preferring non-retired locations; soft-retired
 * (draining) locations are used as fallback so in-flight writes are never dropped.
 */
public class LocationGroup {

  /**
   * The only location of this partition before inflation. Only read on the non-inflated fast path
   * (and as {@link #latest()}'s fallback when the inflated active list is empty); it is
   * intentionally NOT kept in sync after inflation — the inflated {@link ParallelState#active} list
   * is the source of truth.
   */
  private volatile PartitionLocation single;

  /** null until inflated; all mutating methods go through double-checked inflation. */
  private volatile ParallelState parallel;

  public LocationGroup(PartitionLocation loc) {
    this.single = loc;
  }

  /** Fast path: returns the single location when not inflated, zero extra cost. */
  public PartitionLocation currentFor(int mapId) {
    return pick(mapId, -1);
  }

  /**
   * Pick a usable location for {@code mapId}, skipping {@code excludeEpoch} (-1 = skip nothing).
   * Non-retired locations are preferred; soft-retired (draining) locations are the fallback so
   * in-flight writes are never dropped. Returns null when nothing is usable.
   */
  private PartitionLocation pick(int mapId, int excludeEpoch) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      return (loc != null && loc.getEpoch() != excludeEpoch) ? loc : null;
    }
    List<PartitionLocation> active = p.active;
    int n = active.size();
    if (n == 0) {
      return null;
    }
    int start = Math.floorMod(mapId, n);
    for (int pass = 0; pass < 2; pass++) {
      // pass 0: fully active locations; pass 1: soft-retired (draining) locations.
      for (int i = 0; i < n; i++) {
        PartitionLocation loc = active.get((start + i) % n);
        if (loc.getEpoch() == excludeEpoch) {
          continue;
        }
        StatusCode cause = p.retired.get(loc.getEpoch());
        if (pass == 0 && cause == null) {
          return loc;
        }
        if (pass == 1 && cause == StatusCode.SOFT_SPLIT) {
          return loc;
        }
      }
    }
    return null;
  }

  /** The location with the max epoch, used where a single representative location is needed. */
  public PartitionLocation latest() {
    ParallelState p = parallel;
    if (p == null) {
      return single;
    }
    List<PartitionLocation> active = p.active;
    return active.isEmpty() ? single : active.get(active.size() - 1);
  }

  public int maxEpoch() {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation loc = single;
      return loc == null ? -1 : loc.getEpoch();
    }
    return p.maxEpoch;
  }

  /** Whether at least one location can still accept writes (active or soft-draining). */
  public boolean hasUsable() {
    ParallelState p = parallel;
    if (p == null) {
      return single != null;
    }
    for (PartitionLocation loc : p.active) {
      StatusCode cause = p.retired.get(loc.getEpoch());
      if (cause == null || cause == StatusCode.SOFT_SPLIT) {
        return true;
      }
    }
    return false;
  }

  /**
   * Pick an active location other than {@code excludeEpoch} for {@code mapId}, used to re-push
   * batches without waiting for revive when the partition has more than one active location.
   * Returns null if there is no other usable location.
   */
  public PartitionLocation anotherActiveFor(int mapId, int excludeEpoch) {
    return pick(mapId, excludeEpoch);
  }

  /**
   * Mark {@code epoch} as retired with {@code cause}. Soft-retired locations keep draining;
   * hard-retired ones are skipped by {@link #currentFor(int)}.
   *
   * @return true if this is the first time the epoch is retired (dedupe signal for the caller to
   *     send at most one revive per epoch).
   */
  public boolean retire(int epoch, StatusCode cause) {
    ParallelState p = inflateIfNeeded();
    return p.retired.putIfAbsent(epoch, cause) == null;
  }

  /**
   * Converge to the full active set delivered by the LifecycleManager: add locally missing epochs,
   * never re-add locally retired ones. Synchronized because the check-then-add of {@link
   * #insertActive} is not atomic across concurrent revive responses.
   */
  public synchronized void mergeAll(List<PartitionLocation> locations) {
    if (locations == null || locations.isEmpty()) {
      return;
    }
    if (parallel == null && locations.size() == 1) {
      // Stay in thin-wrapper mode when the LM only knows one active location.
      updateSingle(locations.get(0));
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
  }

  /** Legacy single-location update (parallel write disabled or single-location response). */
  public void updateSingle(PartitionLocation loc) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation cur = single;
      if (cur == null || loc.getEpoch() >= cur.getEpoch()) {
        single = loc;
      }
    } else {
      List<PartitionLocation> one = new ArrayList<>(1);
      one.add(loc);
      mergeAll(one);
    }
  }

  public boolean isInflated() {
    return parallel != null;
  }

  /** Visible for testing. */
  int activeCount() {
    ParallelState p = parallel;
    return p == null ? (single == null ? 0 : 1) : p.active.size();
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
    // Active locations, sorted by epoch ascending. Includes soft-retired (draining) ones.
    final CopyOnWriteArrayList<PartitionLocation> active = new CopyOnWriteArrayList<>();
    // epoch -> retire cause
    final ConcurrentHashMap<Integer, StatusCode> retired = new ConcurrentHashMap<>();
    volatile int maxEpoch = -1;
  }
}
