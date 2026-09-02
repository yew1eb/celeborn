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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.protocol.message.StatusCode;

/**
 * Writable PartitionLocation(s) of one (shuffleId, partitionId). Stays a thin single-location
 * wrapper until a split, a push failure, or a multi-location revive response inflates the
 * parallel state. Routing is uniform over the writable subset — non-retired plus soft-split
 * locations, which stay writable until they hard-split — via {@code mapId % writableCount}.
 */
public class PartitionLocationGroup {

  /** Source of truth before inflation; intentionally left stale afterwards. */
  private volatile PartitionLocation single;

  /** Null until inflated; mutators inflate via double-checked locking. */
  private volatile ParallelState parallel;

  public PartitionLocationGroup(PartitionLocation loc) {
    this.single = loc;
  }

  /**
   * Writable location for {@code mapId}; when nothing is writable, falls back to the latest
   * (possibly retired) one — mirroring the baseline path, which keeps pushing the possibly-dead
   * location until the worker rejects it.
   */
  public PartitionLocation currentFor(int mapId) {
    PartitionLocation loc = pick(mapId);
    return loc == null ? latest() : loc;
  }

  boolean hasWritableFor(int mapId) {
    return pick(mapId) != null;
  }

  private PartitionLocation pick(int mapId) {
    ParallelState p = parallel;
    if (p == null) {
      return single;
    }
    List<PartitionLocation> writable = new ArrayList<>();
    for (ActiveEntry e : p.active) {
      if (e.writable()) {
        writable.add(e.location);
      }
    }
    if (writable.isEmpty()) {
      return null;
    }
    return writable.get(Math.floorMod(mapId, writable.size()));
  }

  /** The max-epoch location, where a single representative is needed. */
  public PartitionLocation latest() {
    ParallelState p = parallel;
    if (p == null) {
      return single;
    }
    // Iterating a CopyOnWriteArrayList is snapshot-based: no race with a concurrent full-set
    // merge shrinking the list.
    PartitionLocation latest = single;
    for (ActiveEntry e : p.active) {
      latest = e.location;
    }
    return latest;
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
   * A retired epoch still in the active list, attached to the batched revive at send time so the
   * LM's gap-based allocation sees the real active-set size.
   */
  static final class OutstandingRetire {
    final PartitionLocation location;
    final StatusCode cause;

    OutstandingRetire(PartitionLocation location, StatusCode cause) {
      this.location = location;
      this.cause = cause;
    }
  }

  /** Retired epochs still in the active list, epoch ascending. */
  List<OutstandingRetire> outstandingRetires() {
    ParallelState p = parallel;
    if (p == null) {
      return new ArrayList<>(0);
    }
    List<OutstandingRetire> retires = new ArrayList<>();
    for (ActiveEntry e : p.active) {
      if (e.cause != null) {
        retires.add(new OutstandingRetire(e.location, e.cause));
      }
    }
    return retires;
  }

  /**
   * Mark {@code epoch} retired: a soft-split location stays writable, a harder cause upgrades a
   * soft retire and is never downgraded back.
   *
   * @return true on the first retire of the epoch (dedupe signal for the caller)
   */
  public synchronized boolean retire(int epoch, StatusCode cause) {
    ParallelState p = inflateIfNeeded();
    for (int i = 0; i < p.active.size(); i++) {
      ActiveEntry e = p.active.get(i);
      if (e.location.getEpoch() != epoch) {
        continue;
      }
      if (e.cause == null) {
        p.active.set(i, new ActiveEntry(e.location, cause));
        return true;
      }
      if (e.cause == StatusCode.SOFT_SPLIT && cause != StatusCode.SOFT_SPLIT) {
        p.active.set(i, new ActiveEntry(e.location, cause));
      }
      return false;
    }
    // Already evicted by a full-set merge; the LM no longer reports it, nothing to mark.
    return true;
  }

  /**
   * Converge to the LM-delivered active set: add missing epochs, never resurrect retired ones. A
   * full-set reply evicts retired epochs the LM no longer reports — those have been digested by
   * the LM. Synchronized for atomic check-then-add across concurrent revive responses.
   */
  public synchronized void mergeActiveLocations(
      List<PartitionLocation> locations, boolean fullSet) {
    if (locations == null || locations.isEmpty()) {
      return;
    }
    if (parallel == null && locations.size() == 1) {
      updateLatest(locations.get(0));
      return;
    }
    ParallelState p = inflateIfNeeded();
    for (PartitionLocation loc : locations) {
      if (loc == null) {
        continue;
      }
      insertActive(p, loc);
      if (loc.getEpoch() > p.maxEpoch) {
        p.maxEpoch = loc.getEpoch();
      }
    }
    if (fullSet) {
      evictDigestedRetires(p, locations);
    }
  }

  private static void evictDigestedRetires(ParallelState p, List<PartitionLocation> reported) {
    boolean anyRetired = false;
    for (ActiveEntry e : p.active) {
      if (e.cause != null) {
        anyRetired = true;
        break;
      }
    }
    if (!anyRetired) {
      return;
    }
    Set<Integer> reportedEpochs = new HashSet<>();
    for (PartitionLocation loc : reported) {
      if (loc != null) {
        reportedEpochs.add(loc.getEpoch());
      }
    }
    p.active.removeIf(e -> e.cause != null && !reportedEpochs.contains(e.location.getEpoch()));
  }

  /**
   * Single-location update (adaptive disabled or single-location response). Synchronized because
   * revive responses are applied concurrently by the ReviveManager scheduler thread and by push
   * threads via the blocking revive path.
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

  /** Retired epochs with causes, epoch ascending, for failure diagnostics. */
  List<String> retiredEpochsSnapshot() {
    ParallelState p = parallel;
    if (p == null) {
      return new ArrayList<>(0);
    }
    List<String> retires = new ArrayList<>();
    for (ActiveEntry e : p.active) {
      if (e.cause != null) {
        retires.add(e.location.getEpoch() + "=" + e.cause);
      }
    }
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
            p.active.add(new ActiveEntry(loc, null));
            p.maxEpoch = loc.getEpoch();
          }
          parallel = p;
        }
      }
    }
    return p;
  }

  private static void insertActive(ParallelState p, PartitionLocation loc) {
    List<ActiveEntry> active = p.active;
    int i = 0;
    while (i < active.size() && active.get(i).location.getEpoch() < loc.getEpoch()) {
      i++;
    }
    if (i < active.size() && active.get(i).location.getEpoch() == loc.getEpoch()) {
      // Already present; keep the existing entry so its retire cause is never lost.
      return;
    }
    active.add(i, new ActiveEntry(loc, null));
  }

  /** Inflated once the partition ever splits or gets multiple locations. */
  private static class ParallelState {
    /** Epoch ascending. Mutated only under the group lock; iterated lock-free. */
    final CopyOnWriteArrayList<ActiveEntry> active = new CopyOnWriteArrayList<>();
    volatile int maxEpoch = -1;
  }

  private static final class ActiveEntry {
    final PartitionLocation location;
    /** Retire cause; null while the location is routing-eligible. */
    final StatusCode cause;

    ActiveEntry(PartitionLocation location, StatusCode cause) {
      this.location = location;
      this.cause = cause;
    }

    boolean writable() {
      return cause == null || cause == StatusCode.SOFT_SPLIT;
    }
  }
}
