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

package org.apache.celeborn.client.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.celeborn.common.protocol.PartitionLocation;
import org.apache.celeborn.common.util.JavaUtils;

/**
 * Tracks the {@link PartitionLocation}s used for shuffle write on the client (executor) side.
 *
 * <p>A partition has at least one active write location stored in {@code singleMap} (the legacy
 * single-value view, also consumed by {@code DataPushQueue} for coarse capacity gating and by
 * reader/commit get-max paths). When dynamic write parallelism (1:N) is enabled, a partition that
 * has been upgraded (i.e. the driver returned multiple sibling locations on a revive) additionally
 * has an entry in {@code siblingsMap}; {@link #selectForMapId} hashes {@code mapId} to pick one
 * sibling per mapper so batch order within a mapper is preserved.
 *
 * <p><b>Sparseness</b>: {@code siblingsMap} only holds entries for partitions that actually have
 * multiple active siblings. Small partitions never split and thus never appear in it; reads fall
 * back to {@code singleMap}. This keeps memory bounded even for very large partition counts (e.g.
 * 100k partitions) — {@code seedOnRegister} never allocates an empty list per partition.
 */
public class WriteLocationTracker {
  private static final Logger logger = LoggerFactory.getLogger(WriteLocationTracker.class);

  // shuffleId -> (partitionId -> newest PartitionLocation). Single-value view; source of fallback.
  private final Map<Integer, ConcurrentHashMap<Integer, PartitionLocation>> singleMap =
      JavaUtils.newConcurrentHashMap();

  // shuffleId -> (partitionId -> active sibling locations). Sparse: only partitions with >1
  // active sibling have an entry here.
  private final Map<Integer, ConcurrentHashMap<Integer, List<PartitionLocation>>> siblingsMap =
      JavaUtils.newConcurrentHashMap();

  private final boolean dynamicWriteParallelismEnabled;

  public WriteLocationTracker(boolean dynamicWriteParallelismEnabled) {
    this.dynamicWriteParallelismEnabled = dynamicWriteParallelismEnabled;
  }

  // ---- reads ----

  /** Single-value (newest) location for a partition, or null if absent. */
  public PartitionLocation getSingle(int shuffleId, int partitionId) {
    ConcurrentHashMap<Integer, PartitionLocation> map = singleMap.get(shuffleId);
    return map == null ? null : map.get(partitionId);
  }

  /**
   * The single-value map for a shuffle (partitionId -> newest location). Used by
   * {@code getPartitionLocation} (public API) and {@code DataPushQueue.takePushTasks} (capacity
   * gating). Callers may mutate the returned map (e.g. {@code put} on revive).
   */
  public ConcurrentHashMap<Integer, PartitionLocation> getSingleMap(int shuffleId) {
    return singleMap.computeIfAbsent(shuffleId, id -> JavaUtils.newConcurrentHashMap());
  }

  /**
   * Pick the location to write for (shuffleId, partitionId, mapId). When 1:N is enabled and a
   * sibling set exists for the partition, hash mapId to deterministically select one (same mapId
   * always routes to the same sibling). Otherwise return {@code fallback}.
   */
  public PartitionLocation selectForMapId(
      int shuffleId, int partitionId, int mapId, PartitionLocation fallback) {
    if (dynamicWriteParallelismEnabled) {
      ConcurrentHashMap<Integer, List<PartitionLocation>> perShuffle = siblingsMap.get(shuffleId);
      if (perShuffle != null) {
        List<PartitionLocation> siblings = perShuffle.get(partitionId);
        if (siblings != null) {
          synchronized (siblings) {
            if (!siblings.isEmpty()) {
              return siblings.get(Math.floorMod(mapId, siblings.size()));
            }
          }
        }
      }
    }
    return fallback;
  }

  /**
   * Whether a newer (higher-epoch) location exists for the partition, to skip a redundant revive.
   * When 1:N is enabled, checks the sibling set for any sibling with epoch > the triggering epoch
   * (a concurrent revive may have already added one). When disabled, falls back to single-value
   * epoch comparison.
   */
  public boolean hasNewer(int shuffleId, int partitionId, int epoch) {
    if (dynamicWriteParallelismEnabled) {
      ConcurrentHashMap<Integer, List<PartitionLocation>> perShuffle = siblingsMap.get(shuffleId);
      if (perShuffle != null) {
        List<PartitionLocation> siblings = perShuffle.get(partitionId);
        if (siblings != null) {
          synchronized (siblings) {
            for (PartitionLocation s : siblings) {
              if (s.getEpoch() > epoch) return true;
            }
          }
          return false;
        }
      }
      // No sibling set: fall through to single-value check.
    }
    PartitionLocation current = getSingle(shuffleId, partitionId);
    return current != null && current.getEpoch() > epoch;
  }

  // ---- writes (sparse) ----

  /**
   * Seed locations on register. Only fills {@code singleMap}; never touches {@code siblingsMap}
   * (no sibling exists yet — a partition starts with a single location). This avoids allocating an
   * empty list per partition, which would be pure waste for large partition counts.
   *
   * @return the single-value map (partitionId -> location) for callers that still need the legacy
   *     return shape, e.g. {@code registerShuffleInternal}.
   */
  public ConcurrentHashMap<Integer, PartitionLocation> seedOnRegister(
      int shuffleId, PartitionLocation[] locations) {
    ConcurrentHashMap<Integer, PartitionLocation> result = getSingleMap(shuffleId);
    for (PartitionLocation location : locations) {
      result.put(location.getId(), location);
    }
    return result;
  }

  /**
   * Update on revive: set the single-value to the first (newest) location, and only maintain the
   * sibling set when the driver actually returned multiple siblings. When {@code locs.size() > 1},
   * replace the sibling set with a fresh snapshot; when {@code locs.size() <= 1}, remove any
   * sibling entry so the partition degrades back to single-value (sparse).
   */
  public void updateOnRevive(int shuffleId, int partitionId, List<PartitionLocation> locs) {
    PartitionLocation loc = (locs == null || locs.isEmpty()) ? null : locs.get(0);
    ConcurrentHashMap<Integer, PartitionLocation> map = getSingleMap(shuffleId);
    if (loc != null) {
      map.put(partitionId, loc);
    }
    if (!dynamicWriteParallelismEnabled || locs == null || locs.size() <= 1) {
      // No (or no longer) multiple siblings: drop any stale sibling entry (sparse degradation).
      ConcurrentHashMap<Integer, List<PartitionLocation>> perShuffle = siblingsMap.get(shuffleId);
      if (perShuffle != null) {
        perShuffle.remove(partitionId);
      }
      return;
    }
    siblingsMap
        .computeIfAbsent(shuffleId, id -> JavaUtils.newConcurrentHashMap())
        .put(partitionId, new ArrayList<>(locs));
  }

  /**
   * Mark a sibling (by epoch) as unavailable (e.g. it hit HARD_SPLIT or push failure). Subsequent
   * selections skip it. When the list becomes empty after removal, the entry is dropped so the
   * partition degrades back to single-value. No-op when 1:N is disabled.
   */
  public void excludeSibling(int shuffleId, int partitionId, PartitionLocation failed) {
    if (!dynamicWriteParallelismEnabled || failed == null) return;
    ConcurrentHashMap<Integer, List<PartitionLocation>> perShuffle = siblingsMap.get(shuffleId);
    if (perShuffle == null) return;
    List<PartitionLocation> siblings = perShuffle.get(partitionId);
    if (siblings == null) return;
    // Driver-side state is updated via the subsequent revive (cause=HARD_SPLIT); consistency is
    // restored on the next allocation. Local exclusion just avoids an immediate revive RPC.
    synchronized (siblings) {
      siblings.removeIf(s -> s.getEpoch() == failed.getEpoch());
      if (siblings.isEmpty()) {
        perShuffle.remove(partitionId);
      }
    }
  }

  // ---- lifecycle ----

  /** Remove all location state for a shuffle (on cleanup). */
  public void cleanup(int shuffleId) {
    singleMap.remove(shuffleId);
    siblingsMap.remove(shuffleId);
  }

  // Visible for testing.
  int siblingsEntryCount(int shuffleId) {
    ConcurrentHashMap<Integer, List<PartitionLocation>> perShuffle = siblingsMap.get(shuffleId);
    return perShuffle == null ? 0 : perShuffle.size();
  }
}
