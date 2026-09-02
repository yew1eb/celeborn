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
 * Writable PartitionLocation(s) of one (shuffleId, partitionId), epoch ascending: a single entry in
 * the common (never-split) case, the active set plus not-yet-digested retire tombstones once the
 * partition splits. Routing is uniform over the writable subset — non-retired plus soft-split
 * locations, which stay writable until they hard-split — via {@code mapId % writableCount}.
 * Mutators are synchronized; readers iterate the copy-on-write list lock-free.
 */
public class PartitionLocationGroup {

  final CopyOnWriteArrayList<EpochState> epochs = new CopyOnWriteArrayList<>();
  private volatile int maxEpoch = -1;

  public PartitionLocationGroup(PartitionLocation loc) {
    if (loc != null) {
      epochs.add(new EpochState(loc, null));
      maxEpoch = loc.getEpoch();
    }
  }

  public PartitionLocation currentFor(int mapId) {
    if (epochs.size() == 1) {
      return epochs.get(0).location;
    }

    PartitionLocation loc = pick(mapId);
    return loc == null ? latest() : loc;
  }

  boolean hasWritableFor(int mapId) {
    return pick(mapId) != null;
  }

  private PartitionLocation pick(int mapId) {
    List<PartitionLocation> writable = new ArrayList<>();
    for (EpochState e : epochs) {
      if (e.writable()) {
        writable.add(e.location);
      }
    }
    if (writable.isEmpty()) {
      return null;
    }
    return writable.get(Math.floorMod(mapId, writable.size()));
  }

  public PartitionLocation latest() {
    // Epoch-ascending, so the last entry seen is the max-epoch location; iterating the
    // copy-on-write list is a race-free snapshot read.
    PartitionLocation latest = null;
    for (EpochState e : epochs) {
      latest = e.location;
    }
    return latest;
  }

  public int maxEpoch() {
    return maxEpoch;
  }

  List<EpochState> outstandingRetires() {
    List<EpochState> retires = new ArrayList<>();
    for (EpochState e : epochs) {
      if (e.cause != null) {
        retires.add(e);
      }
    }
    return retires;
  }

  /**
   * Mark {@code epoch} retired: a soft-split location stays writable, a harder cause upgrades a
   * soft retire and is never downgraded back.
   *
   * @return true on the first retire of the epoch
   */
  public synchronized boolean retire(int epoch, StatusCode cause) {
    for (int i = 0; i < epochs.size(); i++) {
      EpochState e = epochs.get(i);
      if (e.location.getEpoch() != epoch) {
        continue;
      }
      if (e.cause == null) {
        epochs.set(i, new EpochState(e.location, cause));
        return true;
      }
      if (e.cause == StatusCode.SOFT_SPLIT && cause != StatusCode.SOFT_SPLIT) {
        epochs.set(i, new EpochState(e.location, cause));
      }
      return false;
    }
    // Already evicted by a merge; the LM no longer reports it, nothing to mark.
    return true;
  }

  /**
   * Singleton revive response — supersede the whole list: routing must track the newest location
   * only, older entries must not keep receiving writes.
   */
  public synchronized void replace(PartitionLocation loc) {
    if (loc.getEpoch() >= maxEpoch) {
      epochs.set(0, new EpochState(loc, null));
      maxEpoch = loc.getEpoch();
    }
  }

  /**
   * Full-set revive response — merge instead of replace, because the response lags local retires:
   * add missing epochs, never resurrect retired ones (a re-reported tombstone is not digested yet;
   * resurrecting routes writes to a dead location), and evict retired epochs the LM no longer
   * reports — digested.
   */
  public synchronized void merge(List<PartitionLocation> reported) {
    if (reported == null || reported.isEmpty()) {
      return;
    }
    Set<Integer> reportedEpochs = new HashSet<>();
    for (PartitionLocation loc : reported) {
      reportedEpochs.add(loc.getEpoch());
      insertIfAbsent(loc);
    }
    epochs.removeIf(e -> e.cause != null && !reportedEpochs.contains(e.location.getEpoch()));
  }

  private void insertIfAbsent(PartitionLocation loc) {
    int i = 0;
    while (i < epochs.size() && epochs.get(i).location.getEpoch() < loc.getEpoch()) {
      i++;
    }
    if (i < epochs.size() && epochs.get(i).location.getEpoch() == loc.getEpoch()) {
      // Already present; keep the existing entry so its retire cause is never lost.
      return;
    }
    epochs.add(i, new EpochState(loc, null));
    if (loc.getEpoch() > maxEpoch) {
      maxEpoch = loc.getEpoch();
    }
  }

  static final class EpochState {
    final PartitionLocation location;
    final StatusCode cause;

    EpochState(PartitionLocation location, StatusCode cause) {
      this.location = location;
      this.cause = cause;
    }

    boolean writable() {
      return cause == null || cause == StatusCode.SOFT_SPLIT;
    }
  }
}
