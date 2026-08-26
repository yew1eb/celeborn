# 自适应分区写并行度（adaptivePartitionWriteParallelism）设计

> **状态**：已实现，分支 `parallel-partition-write`。已经历两轮线上问题修复（并行度塌缩、NULL location），根因与教训见 §10。
> 与社区 CIP-20 / PR#3260 的对比见附录 A。

## 1. 问题与动机

- 写侧现状：**单活跃 location**（epoch 递增覆盖）；SOFT_SPLIT 不丢数据只发 revive，HARD_SPLIT 需重推，期间所有 map task 阻塞等新 location。
- **SOFT→HARD 黄金窗口（问题本质）**：SOFT 模式下 split 升级判定为 `fileLength > splitThreshold(默认 1G) 且 < partitionSplitMaximumSize(默认 2G) → SOFT_SPLIT`；`≥ 2G → HARD_SPLIT`（同步阻塞所有写该 partition 的 map task）。多 mapper 并发写同一 partition 时，单 location 的 fileLength 由 N 个 mapper 共同推高（涨速 ×N），从 1G（SOFT）到 2G（HARD）的**窗口宽度 = 1G / 聚合写速**——写得快则窗口只有秒级，revive + 路由切换来不及完成就升 HARD，shuffle write 全局阻塞。**1:N 并行写的价值正在于此：N 个 mapper 散到 P 个 location，单 location 涨速 ÷P，窗口同比例拉宽**。HARD 模式（≥1G 直接 HARD_SPLIT）无 SOFT 预警，更需要并行写直接压低单 location 涨速。
- **读侧天然支持一个 partition 多个文件**（方案的最大利好，零改动）：
  - `reducerFileGroupsMap: shuffleId -> partitionId -> Set[PartitionLocation]`（`commit/CommitHandler.scala`）；
  - reader 经 `GetReducerFileGroup` 拿整个 Set，`CelebornInputStream.nextReadableLocation()` 顺序串流；
  - 去重已有：batch 头 (mapId, attemptId, batchId)，attempt 过滤 + 跨 location (mapId,batchId) 去重；batchId per-mapTask 全局单调，并行写不撞车；
  - worker 文件名 `partitionId-epoch-mode`（`PartitionLocation.java`），多文件无冲突。
- "单活跃 epoch"假设点（改造涉及）：`latestPartitionLocation`、`ChangePartitionManager.getLatestPartition`、`ShuffleClientImpl.newerPartitionLocationExists`、`updateLatestPartitionLocations`。

## 2. 目标与非目标

**目标（已达成）**
- 单 partition 同时写多个 PartitionLocation（默认并行度 1，热点自动升档，上限可配）；
- location 不可用（SOFT_SPLIT/HARD_SPLIT/push 失败）时写不阻塞：立即切换候选 location，后台补充；
- **热点判定集中在 LM（driver），executor 只保留写路径**；
- 读侧、worker、master、Spark/Flink 集成层零改动；
- 双向版本兼容（新老 client × 新老 LM 均不报错，功能自动降级）。

**非目标（Phase 2，见 §12）**
- worker/client 侧速率统计（解决"检测延迟=写满一个 threshold"的根本手段）；
- worker 主动过载上报（SOFT_SPLIT_OVERLOAD）；
- MAP partition 类型（Flink hybrid shuffle）；
- 并行度降档。

## 3. 总体数据流

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ PartitionLocationGroup.currentFor(mapId) ← mapId % 可写数 选可写 location
             │   （可写 = 非退休 + SOFT_SPLIT；soft 文件在 2G 硬分裂前持续可写）
             ├─ 正常 → 走现有 push/merge 路径（PushState 按 host 分桶，天然兼容）
             ├─ SOFT_SPLIT → group.retire(epoch, SOFT_SPLIT)（保持可写继续分摊写，写不阻塞）
             │     └─ 首次退休发原生 ReviveRequest(epoch, SOFT_SPLIT) 上报
             ├─ HARD_SPLIT / push 失败 → retire + 预置 SUCCESS：
             │     有另一可写 location 时重推线程立即换目标（不等 LM 响应）
             └─ 全部不可写 → 现有同步 revive 路径
        ReviveManager：所有退休上报（含本地已满足的）按 (partition, epoch) 去重后
          一律转发 LM——这是 LM 活跃集记账的唯一输入，一条都不能丢（§5.4）
LM (ChangePartitionManager → PartitionHotnessTracker):
  收到带 cause 的 revive → 活跃集维护：SOFT_SPLIT 且 worker 可用 → epoch 保留（仍可写）；
    HARD_SPLIT/失败/worker 不可用 → epoch 移出活跃集（终态，迟到 SOFT 不复活）；
    计量条件 (cause ∈ {SOFT_SPLIT, HARD_SPLIT} 且旧 location 的 worker 仍可用) 满足时
    HotState 判定：fillTime = 首报时刻 - allocTime(epoch)
    < hotWindow ? desired = ceil(window / fillTime)（首报去重、单调递增、上限截断）；
    push 失败类 cause 一律只退休不判定（见 §6.2 统一计量规则）
  补差分配 gap = desired - 活跃数（互不相同 worker、epoch 递增，可为 0；
    SOFT 上报不释放容量，仅 desired 增长或硬性移除后补缺）
  revive 响应一律返回【活跃 location 全集】(newLocs 放 max epoch + additionalLocs 放其余，
    含仍可写的 soft epoch) → 所有 executor 收敛到同一集合
读侧：不变（fileGroups Set + 多 location 串流 + (mapId,attemptId,batchId) 去重）
```

## 4. 协议改动

### 4.1 proto（`common/src/main/proto/TransportMessages.proto`）
唯一的新增字段：
```proto
message PbChangeLocationPartitionInfo {
  int32 partitionId = 1;
  int32 status = 2;
  PbPartitionLocation partition = 3;   // 保持：epoch 最大的 location（老 client 语义）
  bool oldAvailable = 4;
  // The remaining active locations of the partition (full active set delivery).
  repeated PbPartitionLocation additionalPartitions = 5;
}
```
- proto3 新增字段天然向后兼容：新 client→老 LM 拿不到 additionalPartitions，退化为单 location；老 client→新 LM 忽略未知字段。
- **不采用** PR#3260 把 field 3 改 repeated 的做法（多元素对旧 client 有 merge 畸形风险，见附录 A.2）。

### 4.2 Java 层
- `ReviveRequest`：**无新增字段**——executor 上报与上游原生完全一致。
- `ChangeLocationResponse` 增加第 4 个 case class 字段 `additionalLocs: util.Map[Integer, util.List[PartitionLocation]]`（带默认值，兼容既有构造点）；toPb 写入 `additionalPartitions`，fromPb 读回。
- `RequestLocationCallContext.reply` 增加带默认值的可选参数 `additionalLocations`。
- **Revive 消息允许同 partition 多 epoch 条目**：executor 把同一 partition 已退休的每个 epoch 的上报都放进一条 Revive（每个退休 epoch 的 cause 都是 LM 活跃集记账的输入，见 §5.4；并行列表天然支持重复 partitionId）；`handleRevive` 按 **distinct partition 数**构造 `ChangeLocationsCallContext`，同 partition 的重复回复**首条生效、后续忽略**。
- 兼容性说明：Revive/ChangeLocationResponse 是 **executor client ↔ LM（driver）的应用内消息**，executor 与 driver 必然使用同一 celeborn client jar；proto serde 路径另有 4.1 的 wire 兼容兜底。cpp client 的 `PbPartitionSplit` 路径不动；Flink client 直接消费 `PbChangeLocationResponse` proto，新增字段对其无感。

## 5. Executor client 侧实现

### 5.1 内存开销：薄包装 + 懒加载
- `reducePartitionMap` 值类型为 `PartitionLocationGroup`（298 行，其中核心逻辑 ~120 行），初始是**薄包装**：`volatile PartitionLocation single` + `volatile ParallelState parallel = null`（只比现状多一个对象头+一个字段）；
- `ParallelState`（active 列表 / retired 表 / maxEpoch）**只在首次 SOFT_SPLIT/HARD_SPLIT/push 失败或 revive 响应携带多 location 时 inflate**（双重检查锁）；
- `currentFor(mapId)` 快路径：`parallel == null` 直接返回 `single`，与现状开销相同；膨胀后 `single` 不再同步，`active` 列表是唯一事实源；
- 内存账目（5 万 partition/executor）：薄包装增量 ≈ 1MB；ParallelState 仅热点 partition 存在（个位数~几十个）。

### 5.2 PartitionLocationGroup 行为语义
- **选择策略：可写集合均匀取模**（`Math.floorMod`）：`currentFor` 与 `anotherUsableFor` 统一委托私有方法 `pick(mapId, excludeEpoch)`——可写 = 非退休 + SOFT_SPLIT（soft 文件在达到 `partitionSplitMaximumSize` 硬分裂前持续可写），`mapId % 可写数` 在可写子集（epoch 升序）上均匀分派；全部不可写返回 null。**soft location 是一等路由目标而非兜底**：若把 soft 排除出新写路由，稳态下几乎所有槽位都处于 soft 状态，全部 map 会 bump 到最新 1~2 个非退休 location，并行度塌缩成串行 churn（线上实证，见 §10 事故 A）。同一 map task 稳定写同一 location（保住 PushState 按 host 聚合语义）；不同 map task 散到不同 worker；
- **退休语义**：`retire(epoch, cause)` 返回是否首次退休（SOFT 首报据此去重，每 epoch 只上报一次 SOFT）；**cause 可升级不可降级**——已 SOFT_SPLIT 退休的 epoch 之后又 HARD_SPLIT/失败时升级为硬性 cause（退出可写集合），反向不降级；升级后的硬性 cause 由失败路径的 ReviveRequest 上报（ReviveManager 按 (partition,epoch) 去重）；
- **`anotherUsableFor(mapId, excludeEpoch)`**：HARD_SPLIT/push 失败时在其余可写 location 中挑一个立即重推；
- **全集收敛**：`mergeActiveLocations(locations, fullSet)`（`synchronized`，消除并发 revive 响应下"检查重复→插入"的竞态）以 LM 下发的活跃全集为准，按 epoch 有序插入——不同 executor 收敛到**相同顺序**的 active 列表，`mapId % 可写数` 分派一致；跳过本地已退休 epoch；单 location 响应走 `updateLatest` 保持薄包装不膨胀；`fullSet=true` 时**清理已被 LM 消化（全集中不再出现）的退休 epoch**——退休条目规模收敛到"在途退休"量级，路由空间不会被死条目稀释（非全集的 `updateLatest` 单条更新不触发清理；LM 侧 soft epoch 保留在活跃集内，会继续出现在全集中，故 soft location 不会被误清理）；
- 可写数变化时 mapId 映射偏移——不影响正确性（读侧按 (mapId,attemptId,batchId) 去重）。

### 5.3 ShuffleClientImpl 接入
| 位置 | 实现 |
|---|---|
| `reducePartitionMap` | 值类型 `PartitionLocationGroup`；私有 `getPartitionLocationMap()`；**公开 `getPartitionLocation()` 签名不变**，内部投影 `group.latest()` |
| `pushOrMergeData` | 选 location 改为 `group.currentFor(mapId)`；全不可用且开关开启时走同步 revive 后重取 |
| SOFT_SPLIT 回调（pushData 与 mergeData 两处） | 收敛于 `handleSoftSplitRetire`：`newlyRetired = group.retire(epoch, SOFT_SPLIT)`；仅首次退休且 `!mapperEnded` 发原生 ReviveRequest（无任何附加字段）。数据已落 worker，写不阻塞，soft location 保持可写继续分摊路由。**判定逻辑零残留** |
| HARD_SPLIT / push 失败 / mergeData 重提交 | 收敛于 `retireAndPresetIfAnotherUsable`：发 ReviveRequest + `group.retire(epoch, cause)`；若 `anotherUsableFor(mapId, epoch) != null`，**预置 `reviveStatus=SUCCESS`**，重推线程立即从 `currentFor(mapId)` 取另一可写 location 重推，不等 LM 响应；否则走现有 urgent revive+重推 |
| 重推 SUCCESS 分支 | 取 `group.currentFor(mapId)`（硬性退休 epoch 被排除，soft 仍可被选中） |
| `newerPartitionLocationExists` | `group.maxEpoch() > epoch` |
| `reviveBatch` 响应处理 | 开关开启：`group.mergeActiveLocations(partition + additionalLocs, true)` 全集收敛（含退休条目清理）；关闭：`group.updateLatest(loc)`。含 `loc == null` NPE 保护 |
| mergeData 路径、`PushState` | 零改动 |

### 5.4 退休上报：executor 的唯一义务，也是 LM 记账的承重墙
并行写下 executor 即使已切到其他可写 location、不需要新 location，也必须在退休某 epoch 时上报 revive——这是 LM 感知 split/失败、维护活跃集合与做热点判定的唯一通道。**上报一条都不能丢**：
- ReviveManager 批量发送时，所有退休上报——**包括本地已满足的**（已有更新 location / mapper 已结束）——都按 (partition, epoch) 去重（保留最硬 cause：任何非 SOFT_SPLIT cause 覆盖 SOFT_SPLIT）后一律转发；被同 partition 更高 epoch 请求挤掉的等待请求也转入上报列表；
- 一条 Revive 消息可携带同 partition 的多个 epoch 条目（§4.2）；
- 丢弃任何一条的后果：LM 活跃集被死 epoch 撑大 → 补差分配归零 → executor 全集收敛后仍无可写 location（事故 B，见 §10）。

### 5.5 正确性论证
- **去重**：batchId per-mapTask 全局单调，读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序/连续性 → 并行写安全；
- **重推重复**：换 location 重推产生的重复 batch 由读侧去重兜底（CIP-20 评审问答确认这是既有行为，见附录 A.3）；
- **soft-retired 续写**：SOFT_SPLIT 语义下 worker 继续接收该文件的写（直到 2G 硬分裂才拒写），in-flight 不丢，新写也可以继续分摊到 soft location；`partitionSplitMaximumSize` 是硬性兜底上限；
- **预置 SUCCESS 的安全性**：HARD_SPLIT 时 worker 已拒收该 batch，旧 location 里只有之前成功落盘的 batch；重推到另一 location 后读侧两文件合并+去重，等价于现有"revive 后重推"路径；
- **speculation/rerun/stageEnd 后重跑**：既有路径不变。

### 5.6 并发设计（审计结论）

**线程模型**：`PartitionLocationGroup` 被四类线程并发访问——DataPusher push 线程（每 map task 一个）、push 回调线程（split/失败回调）、ReviveManager 单线程调度器（批量 revive 响应应用）、以及 push 线程直连（全不可用分支的同步 `revive()` 也会应用响应）。LM 侧同一 partition 的批处理由条纹锁 + `inBatchPartitions` 去重天然串行。

**无需加锁的点及依据**：
- `pick`/`currentFor` 读路径：CopyOnWriteArrayList + ConcurrentHashMap，toArray 快照迭代安全；
- `retire()` 首报信号：CHM `compute` 按 key 原子——并发退休同一 epoch 恰好一个线程拿到 true（SOFT 首报去重的保证）；
- `retire` 与 `mergeActiveLocations` 交错：退休不丢——merge 跳过 retired；竞态下入 active 但 retired 有标记，硬性退休照样被 pick 跳过（soft 标记不影响可选性）；清理只删"LM 不再报告的 retired"，误删后该 epoch 亦不在 active，不会再被选中；
- `HotState.activeEpochs` 全部变更点（注册/移除/读）均在 `entry.synchronized` 内；热点判定去重的 `splitReported.add` 在 `state.synchronized` 内；
- `reviveStatus` 预置：字段本身 volatile（baseline 机制）。

**修复项**：`updateLatest` 非膨胀路径的 check-then-set 原非原子（同步 revive 使响应应用可被 push 线程并发执行，乱序响应可能旧 epoch 覆盖新 epoch）——已加 `synchronized`（与 `mergeActiveLocations` 同 monitor，重入安全）。baseline 是无保护 `put`（last-writer-wins），修复后严格优于 baseline。

**良性记录在案**：(a) 全不可用场景的同步 revive 羊群——重复 RPC 由 LM 侧 `inBatchPartitions` piggyback 合并，正确性无虞；(b) `removeShuffle` 后迟到上报会重建一个 HotState 条目——生命周期末尾的一次性微泄漏；(c) `latest()` 在 active 为空时回退 stale `single`——只用作 revive oldLoc 与拥塞门控代表，无正确性影响。

## 6. LM 侧实现（热点判定集中地）

### 6.1 HotState（稀疏，per (shuffleId, partitionId)）
热点状态全部收敛在独立组件 **`PartitionHotnessTracker.scala`**（275 行，从 ChangePartitionManager 提取）：`ChangePartitionManager` 持有一个实例，把判定依赖（latestPartitionLocation 查询、worker 可用性查询）以函数注入，所有时间戳由调用方传入（可注入时钟），tracker 可脱离 LM 独立单测。内部维护 `partitionHotStates: shuffleId -> partitionId -> HotState`：
```
activeEpochs      : LinkedHashSet[Integer]   // 可写 epoch（插入序）：soft 保留，hard/失败移除
hardRetiredEpochs : Set[Integer]             // 被硬性移除过的 epoch：迟到的 SOFT 上报不得复活
allocTimeMs       : Map[Int, Long]           // 每个 epoch 的真实分配时间
splitReported     : Set[Integer]             // 已判定过的 epoch（首报去重）
desired           : volatile Int = 1         // 期望活跃 location 总数（单调递增，封顶 max）
```
- **稀疏**：普通 partition 无条目，活跃集合推导为 `{ latestPartitionLocation.epoch }`；任何并行模式下 oldEpoch >= 0 的 revive 都会建条目（onEpochRetired 维护可写集，且新 epoch 的 allocTime 必须记录，供后续判定）；
- **allocTime 来源**（准确计量的关键）：
  - 新 epoch：`allocateGapLocations` 分配成功时记录（真实创建时间）；
  - epoch 0：`LifecycleManager.handleRegisterShuffle` 成功路径调用 `recordInitialAllocTime`（putIfAbsent，重复注册不覆盖）；
  - 未知 allocTime 的 epoch（老数据）：该次 split 保守不升档；
- shuffle 注销时清理（含 `shuffleInitialAllocTimeMs`）。

### 6.2 热点判定（`onEpochRetired`，在 revive 请求到达时触发）
```
onEpochRetired(shuffleId, partitionId, epoch, oldPartition, cause, now):
  // 活跃集维护：soft 保留可写，其余移除；移除是终态——迟到的 SOFT 上报不复活已硬性移除的 epoch
  if (cause == SOFT_SPLIT && workerAvailableByLocation(oldPartition)
      && epoch ∉ hardRetiredEpochs) activeEpochs += epoch
  else { activeEpochs -= epoch; hardRetiredEpochs += epoch }
  measureEligible = (cause ∈ {SOFT_SPLIT, HARD_SPLIT})
                    && workerAvailableByLocation(oldPartition)   // 统一计量规则
  if (!measureEligible) return                     // push 失败类 / 已知不可用 worker / null 旧 location：只退休
  if (splitReported.contains(epoch)) return        // 同 epoch 重复上报去重
  allocTime = allocTimeMs[epoch] ?? (epoch==0 ? shuffleInitialAllocTime : null)
  if (allocTime == null || now - allocTime >= windowMs) { markSplitReported; return }  // 不升档
  if (splitReported.add(epoch)) {
      target = ceil(windowMs / fillTimeMs)          // 比例步进：K 个 location 各分 1/K 流速，
                                                    // 写满周期拉长到 K×fillTime ≥ window 所需的最小 K
      newDesired = min(maxLocations, target)
      if (newDesired > desired) desired = newDesired   // 单调递增 + 上限截断，无需去抖
  }
```
- **比例步进**：fillTime 本身就携带了需要多少并行度的信息——10s 写满（window 60s）意味着需要 6 路才能把单 location 写满周期拉出窗口，一次判定直达（封顶 maxLocations）；30s 写满只需 2 路，不会过度分配。desired 单调递增 + per-epoch 首报去重 + 上限三重约束，去抖/冷却参数都不需要；
- **统一计量规则**：SOFT_SPLIT 与 HARD_SPLIT 统一计量——两者都是"阈值触发的 split"，同样反映快速写满；HARD 模式（`celeborn.client.shuffle.partitionSplit.mode=HARD`）下热点判定由此激活，不再只有 SOFT 模式受益。两个守卫条件：
  - **worker 必须仍可用**（`workerStatusTracker.workerAvailableByLocation`）：HARD_SPLIT 若由 worker 故障/过载触发（worker 已被 exclude），反映的是 worker 问题而非 partition 热点，不计量；
  - **push 失败类 cause 一律不计量**（PUSH_DATA_FAIL_* / CONNECTION_EXCEPTION 等）：网络/连接问题与分区热度无关，这是原则性排除。
- **判定依据**：fillTime = 全局第一个 split 上报到达时刻 - 该 epoch 真实分配时刻 ≈ `threshold / 该 location 聚合写入速度`。`threshold=1G`、`window=60s` ⇒ 热点线 ≈ **17MB/s 单 location 聚合速度**；
- **epoch 乱序免疫**：每个 epoch 独立对照自己的 allocTime——epoch 10 先于 epoch 5 写满互不影响（"上报间隔法"在这里会误判，这也是不用间隔法的原因）；
- **偏差说明（如实）**：epoch 0 的 allocTime 是 registerShuffle 时刻，mapper 可能更晚才开始写 → fillTime 高估 → 首个 epoch 判定**偏保守**（方向安全）；split 首报时刻略晚于真实写满时刻（一次 push 间隔内），fillTime 略偏大 → 同样偏保守；
- **检测延迟不变**：仍由 split 事件驱动（写满一个 threshold 才感知），这是 split 驱动方案的共同局限，根治需要速率统计（§12）。

### 6.3 分配与回复解耦（`handleRequestPartitions`）
1. **补差分配**（`allocateParallelLocations`）：desired 从 HotState 读（无条目=1），截断上限；`surviving = currentActiveEpochs`（请求到达时已完成活跃集维护——soft 保留、hard/失败移除——无需再减 change.epoch；SOFT 上报不释放容量，仅 desired 增长或硬性移除后补缺），`gap = max(0, desired - surviving.size)`；`allocateGapLocations` 逐次分配（epoch 从 max(latest, surviving)+1 递增），每轮从 candidates 排除已选 worker（best-effort 互不相同）。gap 可为 0；
2. **全集回复**（`replySuccessFullSet`）：对每个请求回复该 partition 当前可写全集（`newLocs` 放 max epoch，`additionalLocs` 放其余，**含仍可写的 soft epoch**——客户端全集收敛因此不会清理它们），本轮分配 0 个也回全集——executor 间收敛；
3. 分配成功后登记活跃 epoch 与新 epoch 的 allocTime；失败路径不变；
4. 早返回路径（本地已有更新 location）同样回复 `latestLoc + additionalLocs`；
5. `updateLatestPartitionLocations` 用 `map.merge` 保留 max epoch（非并行路径同样受益）。

### 6.4 退休 location 的 commit：维持现状
SOFT_SPLIT 语义允许 split 后续写，提前增量 commit 会使续写 push 命中已提交文件（worker 按 "already committed" 拒绝），引入 revive 风暴。维持 StageEnd 全量 commit。HARD_SPLIT 既有增量 commit 路径不动。

### 6.5 不动的部分
- Master / SlotsAllocator：不动（K 个 location 用现有 `reserveSlotsWithRetry` 逐次 reserve）；
- worker：不动（SOFT_SPLIT 阈值触发是现成信号）；
- `workerStatusTracker.excludeWorkerFromPartition`：逻辑不变。

## 7. 配置项与上线观测点

配置（`CelebornConf.scala`；`docs/configuration/client.md` 已再生成）：

| 配置 | 默认 | 生效侧 | 说明 |
|---|---|---|---|
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.enabled` | false | client + LM | 总开关 |
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.maxLocations` | 8 | **LM** | 单 partition 活跃 location 上限 |
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.hotWindow` | 60s | **LM** | 单 location 写满耗时的热点判定窗口；升档目标 = ceil(窗口 / 写满耗时) |

开关关闭时所有代码路径与现状等价（PartitionLocationGroup 薄包装快路径、LM 走原有 `replySuccess`、不建 HotState）。仅有的 3 处微观差异（均有意识保留、方向更安全）：`updateLatest` 丢弃低 epoch 的迟到响应（现状是无条件覆盖）；SOFT_SPLIT 上报前增加 `!mapperEnded` 前置过滤（现状靠 ReviveManager 后置过滤，效果等价）；DataPushQueue 拥塞门控取 `group.latest()` 作为 partition 的代表 location（并行写下门控粒度略粗，不影响正确性）。

### 7.1 上线观测点（日志）

**LM/driver 侧**（回答"判定与分配是否生效"）：

| 日志 | 级别 | 位置 | 内容 |
|---|---|---|---|
| 升档判定 | INFO | `PartitionHotnessTracker.onEpochRetired` | partition、fillTime、窗口、boost 后 desired |
| 补差分配 | INFO | `ChangePartitionManager.allocateParallelLocations` | partition、分配数、epochs、worker hosts；候选不足时 WARN |
| 未计量退休 | INFO | `PartitionHotnessTracker.onEpochRetired` | push 失败类 / worker 不可用，附原因 |
| stage-end 摘要 | INFO | `PartitionHotnessTracker.removeShuffle` | 每 shuffle 一行：热点 partition 数、各自 desired 与判定次数 |

**Executor 侧**（回答"写路径是否在用多 location"）：

| 日志 | 级别 | 位置 | 内容 |
|---|---|---|---|
| 并行激活 | INFO | `reviveBatch` 响应处理 | activeCount 增长且 >1：partition、K、`epoch@host` 列表 |
| 立即换路 | INFO | `retireAndPresetIfAnotherUsable` | 旧 `epoch@host`、cause、新目标 `epoch@host`（预置 SUCCESS） |
| SOFT 首报退休 | INFO | `handleSoftSplitRetire` | `epoch@host` soft-split，保持可写继续分摊路由（每 epoch 一条，天然去重） |
| 全不可用阻塞 revive | INFO | `pushOrMergeData` | 进入同步 revive、当时 maxEpoch、等待耗时 |
| 收敛清理 | INFO | `reviveBatch` 响应处理 | 清理掉 LM 不再报告的退休条目数 |

## 8. 兼容性矩阵

| 组合 | 行为 |
|---|---|
| 新 client + 新 LM | 全功能 |
| 新 client + 老 LM | 响应无 additionalPartitions → executor 的 mergeActiveLocations 只收单 location，退化为现状，无异常 |
| 老 client + 新 LM | 响应新字段被老 client 忽略；LM 的 HotState 判定照常（老 client 的原生 revive 就是判定输入），只是老 client 不会使用多 location |
| worker 任意版本 | 无感（协议未动 worker 面） |
| Flink client | 直接消费 `PbChangeLocationResponse` proto，新增字段无感；不启用并行写 |
| cpp client | PbPartitionSplit 路径不动，不启用并行写 |

## 9. 实际改动清单

| 文件 | 内容 | 规模 |
|---|---|---|
| `common/src/main/proto/TransportMessages.proto` | `additionalPartitions` 一个字段 | +2 |
| `common/.../message/ControlMessages.scala` | additionalLocs 字段 + 双向 serde | +25 |
| `common/.../CelebornConf.scala` + `docs/configuration/client.md` | 3 个配置 + 文档再生成 | +43 |
| `client/.../PartitionLocationGroup.java` | 薄包装 + ParallelState + 可写集合统一 pick（无判定逻辑）；retire cause 升级 + 全集收敛清理 | 298 |
| `client/.../ShuffleClientImpl.java`、`ReviveManager.java` | §5.3 接入；退休上报全量转发（含本地已满足的，按 (partition,epoch) 去重保留最硬 cause） | ~+250 |
| `client/.../ChangePartitionManager.scala` | 补差分配 + 全集回复；热点状态全部委托给 tracker（自身 757 行） | ~+340 |
| `client/.../PartitionHotnessTracker.scala` | HotState + 统一计量判定 + 比例步进 + hardRetiredEpochs 防复活 + 依赖注入（latestEpoch / workerAvailable / 时钟） | 275 |
| `client/.../LifecycleManager.scala`、`RequestLocationCallContext.scala` | registerShuffle 记录 allocTime、additionalLocs 透传、Revive 按 distinct partition 计数 + 重复回复忽略 | ~+70 |
| `client/src/test/...`（4 个套件） | PartitionLocationGroupSuiteJ 8 例、ChangePartitionManagerAdaptiveParallelismSuite 10 例、PartitionHotnessTrackerSuite 10 例、RequestLocationCallContextSuite 1 例 | 1077 |

合计约 2300 行（含测试，按上表累加）。注：带 ~ 的新增行数为约数——分支含 write-stats/UI 等并行工作，无法按 merge-base 精确隔离本特性。

## 10. 验证与线上事故教训

### 10.1 测试矩阵

| 套件 | 例数 | 覆盖点 |
|---|---|---|
| `PartitionLocationGroupSuiteJ`（JUnit） | 8 | 薄包装快路径、inflate、可写集合均匀取模（soft 参与路由 / hard 排除 / cause 升级）、全集收敛清理、并发快照 |
| `ChangePartitionManagerAdaptiveParallelismSuite` | 10 | fillTime 升/不升档、首报去重、上限截断、epoch 乱序、allocTime 未知保守不升、补差分配不同 worker、分配 0 仍回全集、并发 revive 收敛 |
| `PartitionHotnessTrackerSuite` | 10 | 统一计量守卫（worker 可用性、push 失败不计量）、soft 保留 / hard 移除、迟到 SOFT 不复活、比例步进直达上限、慢速后续不降级 |
| `RequestLocationCallContextSuite` | 1 | 同 partition 重复回复忽略、按 distinct partition 数完成响应 |

最近一次定向回归全绿（JUnit 8/8 + ScalaTest 21/21，`./dev/reformat` 通过）；开发过程中做过两轮 client 全量回归全绿（~33 分钟，BUILD SUCCESS）。

### 10.2 线上事故与教训

**事故 A：并行度塌缩（soft 被排除出新写路由）**。现象：重度倾斜作业（单热点 partition，69 路上限）下 split 按 epoch 顺序串行发生（间隔 ~300ms、落在不同 worker），fillTime 恒 ~200ms 与路数无关，单 map 对单 worker 的 push 过半数收到 softSplit 响应。根因：`pick()` 把 soft-retired 排除出新写路由（只做兜底），稳态下几乎所有槽位处于 soft 状态，所有 map 的 `mapId % K` 起点落在 retired 槽位后向前 bump 到同一个最新 location，1000+ 并发 map 瞬间灌满它 → 串行 churn 自我维持，有效并行度塌缩为 1~2 路。修复：SOFT_SPLIT location 改为一等可写路由目标，可写集合（非退休 + soft）上均匀取模。**教训：soft location 必须参与新写路由——任何"soft 只做兜底"的设计都会在稳态下塌缩。**

**事故 B：NULL location（退休上报通道被吞 + 死 epoch 复活）**。现象：作业失败，`CelebornIOException: Partition location for shuffle 0 partition 868 is NULL!`；driver 侧同一 epoch 被反复以 PUSH_DATA_FAIL 上报且永不分配新 location。根因三层叠加：(a) ReviveManager 批量发送时把"本地已满足"（已有更新 location / mapper 已结束）的请求整条丢弃——soft-retained epoch 的后续 HARD/失败上报几乎全被吞掉（客户端 maxEpoch 已被全集回复推高）；(b) 同 partition 同批次只保留 max epoch 一条，低 epoch 退休上报被折叠丢弃；(c) 跨 executor 乱序时迟到的 SOFT 上报把已硬性移除的 epoch 加回活跃集。后果链：LM 活跃集被死 epoch 撑大（远超 desired）→ gap 恒为 0 → 不再分配 → executor 把所有 location 硬性退休后全集收敛仍无可写 → NULL。修复：退休上报全量转发（含已满足的，按 (partition,epoch) 去重）+ Revive 按 distinct partition 计数响应 + `hardRetiredEpochs` 使移除成为终态。**教训：退休上报是 LM 活跃集记账的承重墙，一条都不能丢；epoch 的移除必须是终态，迟到上报不得复活。**

### 10.3 待办（生产推广 / 上游化前）
- 集成测试（`tests/spark-it`）：`partitionSplit.threshold=10m`、重倾斜 Spark 作业——(a) reducer 数据与原生 shuffle 对拍一致；(b) 该 partition 最终有 >1 个 committed location 且数据无重复无丢失；(c) 写阶段无 revive 长尾（对比开关前后耗时）；
- 故障注入：写中 kill 一台 worker → 写不中断，数据无丢无重；
- 真实集群灰度：小流量开 `adaptivePartitionWriteParallelism.enabled`，观察 worker slot 占用与 StageEnd commit 时长。

## 11. 风险与开放问题
1. **检测延迟**：split 事件驱动，首次判定要等"写满一个 threshold"（1G 阈值约 60s 边界，10G 阈值部署 ×10）；滞后期间等价于现状（单 worker 写），不会更差。根治：速率统计（§12）；
2. **epoch 0 判定偏保守**：allocTime=registerShuffle 时刻早于实际开写，fillTime 高估 → target 低估（方向安全）；
3. **HARD_SPLIT 统一计量的残余误判**：worker 仍可用但 HARD 并非热度所致（mapper 结束后的迟到写恰好越阈值 / worker 内存紧张触发整体 HARD_SPLIT）——均罕见，且后果有上限（desired 封顶 maxLocations，仅多占少量 slot）。接受该残余误判换取 HARD 模式的热点检测能力；
4. **worker 占用放大**：极热分区一次判定即占满 K 台上限——maxLocations 是唯一刹车，生产上线前按集群规模谨慎设值（默认 8）；
5. **单 partition 磁盘占用上界**：soft location 可写至 2G 硬上限，单 partition 磁盘占用上界 = K × partitionSplitMaximumSize（如 69 路 ≈ 138G 散布集群）。soft 文件持续接收新写但 worker 在 soft 状态下不上报拥塞（PushDataHandler 既有行为），拥塞感知存在缺口，后续可单独处理；
6. **AQE skew read 交互**：`splitSkewedPartitionLocations` 多文件场景理论兼容，必须 IT 回归；
7. **replicate 模式**：K 个 location = 2K 个 slot，需压测 reserve 开销；
8. **StageEnd commit 列表变长**：超大 shuffle 需观察；
9. **LM 单点复杂度**：HotState 稀疏且无滑窗，已提取为独立可单测的 PartitionHotnessTracker——比 CIP-20 的滑窗方案轻一个量级，这是上游化时对"LM 复杂度"质疑的主要论据。

## 12. Phase 2 展望（不在本期）
- worker/client 侧速率统计替代 split 事件驱动（检测延迟 → 10~20s，且与 split 阈值解耦）；
- worker 写入速率监控 + `SOFT_SPLIT_OVERLOAD` 主动上报（CIP-20 Further Optimization 的方向）；
- 并行度降档与热点消散回收；
- LM 滑窗估算（可参考 PR#3260 `PartitionLocationMonitor`，仅当速率统计证明必要）。

---

# 附录 A. 与 CIP-20 / PR#3260 的对比

PR #3260（作者 ErikFang，2025-05 创建，[WIP]，diff ~3800 行）是唯一已知的同类社区尝试：executor 新增 `LocationManager` 维护每 partition 的 location 列表（`mapId % size` 静态 hash 路由 + 跳过不可用），LM 侧每 partition 一个 `PartitionLocationMonitor` + 滑窗（180s/10s bucket）估算速率，双通道上报（紧急 revive + 非紧急 `PbPartitionSplitReport`）。2026-04 被 stale bot 流程性关闭，GitHub 零代码评审。

## A.1 对比总览

| 维度 | CIP-20 / PR#3260 | 本方案（已实现） |
|---|---|---|
| **判定信号** | split 事件计数 × 假定字节数（SOFT=1G / HARD=3G 魔数），非实测 | **fillTime 实测**：首报时刻 − 该 epoch 真实分配时刻，per-epoch 独立对照 |
| **速率估算** | `expectedWorkerSpeed=10MB/s` 静态魔数（评审最大质疑） | **无需估算**：hotWindow 即 SLO（单 location 写满应慢于窗口） |
| **升档公式** | `ceil(pushSpeed/expectedSpeed) − activeCount`，依赖速率估算与窗口积累 | `ceil(window / fillTime)` 比例步进，**一次判定直达**，封顶 maxLocations |
| **信号守卫** | push 失败不计速率 | cause ∈ {SOFT,HARD} + 旧 location worker 可用 双守卫 |
| **决策位置** | LM PartitionLocationMonitor（per-partition） | LM PartitionHotnessTracker / HotState（per-partition，同构） |
| **执行通道** | 双通道：紧急只补 1、非紧急异步预分配（回复可空 + 50ms 轮询收敛） | 单通道原生 revive；**全集回复一次收敛**；HARD_SPLIT/失败预置 SUCCESS 立即换路 |
| **epoch 语义** | 重用为并行度刻度（连锁改 ChangePartitionRequest / inBatch 去重 / proto） | 保持单值 max 语义；新 location epoch 递增分配 |
| **协议兼容** | `partition` 单值→repeated（旧 client 多元素 merge 畸形风险）+ 新 MessageType/StatusCode | proto3 additive `additionalPartitions`（field 5），双向降级安全（§8） |
| **executor 改动** | LocationManager ~400 行 + 重试状态机重写（无单测） | PartitionLocationGroup 298 行薄包装 + 退休上报，重试路径复用 |
| **测试** | Monitor 4 例 + 端到端时间断言 | 29 例（§10.1） |
| **实现规模** | ~3800 行（WIP） | ~2300 行含测试（§9） |
| **上限/回收** | maxActiveLocation 默认 numMappers；只升不降 | maxLocations 默认 8；只升不降（同） |

## A.2 关键差异

**判定信号**：CIP-20 的信号是"事件 × 假定字节数"——SOFT_SPLIT 一律记 1G，无法区分 10s 写满和 10min 写满，只能靠 180s 窗口内的事件频率隐式编码速度；高阈值部署（10G）下事件稀疏，频率估算失准（评审原话）。本方案的 fillTime 是**对每个事件的独立实测**（首报时刻 − 该 epoch 真实分配时刻），一次事件即可判定，无需窗口积累。两者共同的代价：检测延迟都是"写满一个 threshold"，这是 split 驱动方案的固有局限（根治见 §12）。

**并行度决策**：CIP-20 需要 `expectedWorkerSpeed`（静态 10MB/s，评审主战场：它应是集群负载、异构硬件、IO 特征的函数，集群管理员无法给值）。本方案的窗口即 SLO——"单 location 写满应慢于 60s"，`ceil(window / fillTime)` 直接给出把写满周期拉出窗口所需的路数，**不含任何速度假设**。热点定义从"比某个假定的 worker 速度快"变成"比运维设定的 SLO 快"，后者是可理解、可调优的语义。

**executor 间一致性**：CIP-20 非紧急通道回复可为空，executor 靠 50ms 轮询收敛到最新 location 集合；本方案每次 revive 响应携带**活跃全集**（max epoch + additionalPartitions），任何 executor 一次 revive 即与全局收敛，且按 epoch 有序插入保证不同 executor 收敛到**相同顺序**，`mapId % K` 分派一致。

**协议与 epoch 语义**：CIP-20 把 epoch 从"替换序号"重用为"并行度刻度"（连锁改 `ReviveRequest`/`ChangePartitionRequest`/inBatch 去重/proto），并把 `partition` 单值改 repeated——旧 client 收到多元素响应按 merge 语义拼接 message，有产出畸形 PartitionLocation 的风险；新增 MessageType/StatusCode 对旧 LM 不可识别。本方案不动 epoch 语义，只新增 additive 字段 `additionalPartitions`：老 client 忽略、新 client 缺失时退化为单 location。

## A.3 关闭原因与上游化叙事

CIP-20 是**流程性死亡而非技术否决**：GitHub 零代码评审，实质讨论在 CIP Google Doc（主要评论者 Mridul Muralidharan），质疑集中在两点——`expectedWorkerSpeed` 难估、高阈值部署下信号太钝——这两点正是本方案消除的。评审问答同时确认了多 location 写的重复 batch 由既有 (mapId,attemptId,batchId) 去重兜底可接受。大 partition 多 location 并行写这个 idea 在社区没有反对意见，本方案可以作为 CIP-20 的延续上游化。
