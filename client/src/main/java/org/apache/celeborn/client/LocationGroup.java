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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

/**
 * A thin wrapper of the writable PartitionLocation(s) of one (shuffleId, partitionId).
 *
 * <p>In the common case (the partition never splits) it only holds a single volatile {@link
 * PartitionLocation} reference, so the fast path costs exactly one extra object header compared to
 * storing the location directly. The parallel state (active location list, retired epochs and the
 * {@link HotTracker}) is inflated lazily on the first SOFT_SPLIT/HARD_SPLIT/push failure, or when a
 * revive response delivers more than one active location.
 *
 * <p>Selection policy: {@code mapId % activeCount}, preferring non-retired locations; soft-retired
 * (draining) locations are used as fallback so in-flight writes are never dropped.
 */
public class LocationGroup {

  private final long hotPartitionWindowMs;
  private final int maxLocations;

  /** The only location of this partition before inflation. */
  private volatile PartitionLocation single;

  /** When this executor learned the location held by {@link #single}. */
  private volatile long singleLearnTimeMs;

  /** null until inflated; all mutating methods go through double-checked inflation. */
  private volatile ParallelState parallel;

  public LocationGroup(PartitionLocation loc, long hotPartitionWindowMs, int maxLocations) {
    this.single = loc;
    this.singleLearnTimeMs = System.currentTimeMillis();
    this.hotPartitionWindowMs = hotPartitionWindowMs;
    this.maxLocations = maxLocations;
  }

  /** Fast path: returns the single location when not inflated, zero extra cost. */
  public PartitionLocation currentFor(int mapId) {
    ParallelState p = parallel;
    if (p == null) {
      return single;
    }
    List<PartitionLocation> active = p.active;
    int n = active.size();
    if (n == 0) {
      return null;
    }
    int start = Math.floorMod(mapId, n);
    for (int i = 0; i < n; i++) {
      PartitionLocation loc = active.get((start + i) % n);
      if (!p.retired.containsKey(loc.getEpoch())) {
        return loc;
      }
    }
    // No fully active location: soft-retired locations still accept draining writes.
    for (int i = 0; i < n; i++) {
      PartitionLocation loc = active.get((start + i) % n);
      if (p.retired.get(loc.getEpoch()) == StatusCode.SOFT_SPLIT) {
        return loc;
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
    for (int i = 0; i < n; i++) {
      PartitionLocation loc = active.get((start + i) % n);
      if (loc.getEpoch() != excludeEpoch && !p.retired.containsKey(loc.getEpoch())) {
        return loc;
      }
    }
    for (int i = 0; i < n; i++) {
      PartitionLocation loc = active.get((start + i) % n);
      if (loc.getEpoch() != excludeEpoch
          && p.retired.get(loc.getEpoch()) == StatusCode.SOFT_SPLIT) {
        return loc;
      }
    }
    return null;
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
    boolean newlyRetired = p.retired.putIfAbsent(epoch, cause) == null;
    p.hot.onRetire(epoch);
    return newlyRetired;
  }

  /**
   * Measure the fill time of {@code epoch} on SOFT_SPLIT and maybe boost the desired location
   * count. Must be called before {@link #retire(int, StatusCode)} for the same epoch.
   *
   * @return the current desired total location count, to be carried by ReviveRequest.
   */
  public int onSoftSplit(int epoch) {
    ParallelState p = inflateIfNeeded();
    return p.hot.onSoftSplit(epoch, System.currentTimeMillis());
  }

  public int desiredLocationCount() {
    ParallelState p = parallel;
    return p == null ? 1 : p.hot.desired();
  }

  /**
   * Converge to the full active set delivered by the LifecycleManager: add locally missing epochs,
   * never re-add locally retired ones, and keep {@link #single} at the max epoch location.
   */
  public void mergeAll(List<PartitionLocation> locations) {
    if (locations == null || locations.isEmpty()) {
      return;
    }
    if (parallel == null && locations.size() == 1) {
      // Stay in thin-wrapper mode when the LM only knows one active location.
      updateSingle(locations.get(0));
      return;
    }
    ParallelState p = inflateIfNeeded();
    long now = System.currentTimeMillis();
    for (PartitionLocation loc : locations) {
      if (loc == null || p.retired.containsKey(loc.getEpoch())) {
        continue;
      }
      insertActive(p, loc);
      p.hot.onEpochLearned(loc.getEpoch(), now);
      if (loc.getEpoch() > p.maxEpoch) {
        p.maxEpoch = loc.getEpoch();
      }
    }
    if (!p.active.isEmpty()) {
      single = p.active.get(p.active.size() - 1);
    }
  }

  /** Legacy single-location update (parallel write disabled or single-location response). */
  public void updateSingle(PartitionLocation loc) {
    ParallelState p = parallel;
    if (p == null) {
      PartitionLocation cur = single;
      if (cur == null || loc.getEpoch() >= cur.getEpoch()) {
        single = loc;
        singleLearnTimeMs = System.currentTimeMillis();
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

  /** Visible for testing. */
  HotTracker hotTracker() {
    return inflateIfNeeded().hot;
  }

  private ParallelState inflateIfNeeded() {
    ParallelState p = parallel;
    if (p == null) {
      synchronized (this) {
        p = parallel;
        if (p == null) {
          p = new ParallelState(hotPartitionWindowMs, maxLocations);
          PartitionLocation loc = single;
          if (loc != null) {
            p.active.add(loc);
            p.maxEpoch = loc.getEpoch();
            // The initial location was learned at registration/wrap time.
            p.hot.onEpochLearned(loc.getEpoch(), singleLearnTimeMs);
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
      // Same epoch, refresh in place (location objects are immutable per epoch).
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
    final HotTracker hot;
    volatile int maxEpoch = -1;

    ParallelState(long hotPartitionWindowMs, int maxLocations) {
      this.hot = new HotTracker(hotPartitionWindowMs, maxLocations);
    }
  }

  /**
   * Decides whether a partition is hot by measuring, per epoch, how long it took to fill up one
   * location (fillTime = SOFT_SPLIT time - the time this executor learned the epoch). Each epoch is
   * measured independently, so out-of-order fills (epoch 10 filled before epoch 5) do not interfere
   * with each other.
   */
  static class HotTracker {
    private final long windowMs;
    private final int maxLocations;
    // epoch -> when this executor learned the epoch
    private final ConcurrentHashMap<Integer, Long> epochLearnTime = new ConcurrentHashMap<>();
    // epochs whose split has been measured (dedupe repeated SOFT_SPLIT notifications)
    private final Set<Integer> splitReported = ConcurrentHashMap.newKeySet();
    private volatile int currentDesired = 1;
    // debounce: boost at most once per window
    private volatile long lastBoostTime = -1;

    HotTracker(long windowMs, int maxLocations) {
      this.windowMs = windowMs;
      this.maxLocations = maxLocations;
    }

    void onEpochLearned(int epoch, long nowMs) {
      epochLearnTime.putIfAbsent(epoch, nowMs);
    }

    int onSoftSplit(int epoch, long nowMs) {
      if (!splitReported.add(epoch)) {
        // Repeated SOFT_SPLIT notification of the same epoch, ignore.
        return currentDesired;
      }
      Long start = epochLearnTime.get(epoch);
      if (start == null) {
        // Unknown start point, conservatively do not boost.
        return currentDesired;
      }
      long fillTime = nowMs - start;
      if (fillTime < windowMs && (lastBoostTime < 0 || nowMs - lastBoostTime >= windowMs)) {
        currentDesired = Math.min(currentDesired + 1, maxLocations);
        lastBoostTime = nowMs;
      }
      return currentDesired;
    }

    void onRetire(int epoch) {
      epochLearnTime.remove(epoch);
      splitReported.remove(epoch);
    }

    int desired() {
      return currentDesired;
    }
  }
}
