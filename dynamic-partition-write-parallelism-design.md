# 动态分区写并行度设计（结合 Uniffle partition split 与 CIP-20）

> **状态**：Phase 1.1 已实现——热点判定已从 executor 迁移到 LifecycleManager（driver）侧，executor 只保留写路径。Commits：Phase 1 `f398d73dc`、Phase 1.1 `5342d934c`（分支 `parallel-partition-write`）。验证结果见 §12。本文档第二部分已按实际代码修订。

# 第一部分：调研结论（精简）

## 1. Uniffle partition split（代码级要点）
- server 端检测：`HugePartitionUtils.hasExceedPartitionSplitLimit` + `rss.server.huge-partition.split.limit`（示例 20G），requireBuffer 响应带 `needSplitPartitionIds`（`ShuffleServerGrpcService.java:697-748`）。
- driver 重分配 + writer 本地 fast-switch；LOAD_BALANCE 模式一次申请固定 N 台（默认 10），按 `taskAttemptId % (serverSize-1)+1` 把不同 map task 散列到不同 server **并行写**；reader 读全部历史 server + bitmap 去重。
- 启示：**固定并行度即可用，不做速率估算；切换不阻塞写**。

## 2. Celeborn 现状（代码级要点）
- 写侧：单活跃 location（epoch 递增覆盖）；SOFT_SPLIT 不丢数据只发 revive，HARD_SPLIT 需重推，期间所有 map task 阻塞等新 location。
- **读侧天然支持一个 partition 多个文件**（方案的最大利好，零改动）：
  - `reducerFileGroupsMap: partitionId -> Set[PartitionLocation]`（`LifecycleManager.scala`）；
  - reader 经 `GetReducerFileGroup` 拿整个 Set，`CelebornInputStream.nextReadableLocation()` 顺序串流；
  - 去重已有：batch 头 (mapId, attemptId, batchId)，attempt 过滤 + 跨 location (mapId,batchId) 去重；batchId per-mapTask 全局单调，并行写不撞车；
  - worker 文件名 `partitionId-epoch-mode`（`PartitionLocation.java`），多文件无冲突。
- "单活跃 epoch"假设点（改造涉及）：`latestPartitionLocation`、`ChangePartitionManager.getLatestPartition`、`ShuffleClientImpl.newerPartitionLocationExists`、`updateLatestPartitionLocations`。

## 3. CIP-20 / PR #3260（未合并，stale 关闭）
- 骨架可用：executor 侧 `LocationManager`（mapId%size 选择、不可用标记、非紧急/紧急双通道 revive）；LM 侧 `PartitionLocationMonitor` 滑窗估算速率动态调并行度；proto 把 Revive 响应 partition 改 repeated。
- 评审弱点：`expectedWorkerSpeed=10MB/s` 难估；生产 split 阈值常 10G 导致 split 频率信号太钝；协议兼容性复杂；WIP 无测试。
- 结论：**沿用其写侧 1:N 骨架，砍掉滑窗动态估算，换成 Uniffle 式"简单触发 + 有界递增并行度"**。

## 3.5 三方方案详细对比（Uniffle vs CIP-20/PR#3260 vs 本方案）

### 3.5.1 总览表

| 维度 | Uniffle partition split | CIP-20 / PR#3260 | 本方案（Phase 1.1，已实现） |
|---|---|---|---|
| **触发主体** | shuffle server（绝对大小阈值，示例 20G） | client 上报 split 事件，LM 侧滑窗估算速率 | **LM（driver）**：复用 executor 原生 SOFT_SPLIT revive 上报，按**单个 location 写满耗时**判定热点 |
| **触发信号质量** | 直接、可靠；但阈值调大后同样稀疏 | split 频率；评审指出生产阈值 10G 时信号太钝 | 同受阈值大小影响（只升档慢、不会升错） |
| **并行度决策** | 固定 N（默认 10），一次到位 | 动态：`ceil(pushSpeed / expectedWorkerSpeed) - active` | 有界递增：每个"写满耗时 < 窗口"的 location 触发 +1，窗口内去抖，上限可配（默认 4）；不估算 worker 速度 |
| **决策位置** | driver（RssShuffleManagerBase） | LM（PartitionLocationMonitor） | **LM（HotState）**：真实分配时间、全局首报、唯一决策者 |
| **写分派 key** | `taskAttemptId % (serverSize-1)+1` | `mapId % size` | `mapId % K`（与二者一致） |
| **split 时写是否阻塞** | 不阻塞（writer 本地 fast-switch） | 不阻塞（切候选 location，非紧急 revive 补充） | 不阻塞（同 CIP-20；HARD_SPLIT/失败直接重推到其他活跃 location） |
| **executor 间一致性** | shuffle handle 序列化下发 split server 全集 | 未明确 | **revive 响应返回活跃 location 全集**，各 executor 收敛到同一 epoch 集合 |
| **executor 改动** | writer 切换逻辑（reassign 框架内） | LocationManager(~390行)+HotTracker | **仅 LocationGroup 写路径（244 行）+ 退休上报**，无判定逻辑 |
| **读侧改动** | 无（bitmap 去重） | 无 | 无 |
| **server/worker 改动** | 有（检测 + 响应携带 needSplitPartitionIds） | 无 | 无 |
| **协议改动** | reassign gRPC + handle 携带 split 状态（较大） | Revive 响应 partition 改 repeated（**wire 破坏性**） | 仅 proto3 新增 `additionalPartitions` 一个字段，**wire 兼容** |
| **兼容性** | 与多副本不兼容（代码直接抛异常） | 评审点名兼容复杂 | 双向降级安全（§10 矩阵）；与 Celeborn 副本机制兼容 |
| **退休数据 commit** | 无此概念 | 未明确 | 维持现状（StageEnd 全量 commit），不引入已提交文件竞态 |
| **实现规模** | 大（框架级，数千行） | ~3800 行（WIP，无测试） | **~1300 行含测试**（Phase 1.1 净改动 +469/-390） |
| **主要局限** | 依赖 reassign；与副本互斥；并行度固定 | 速率估算难（magic number）；兼容复杂；社区未接受 | 热点窗口是经验值；只升不降；高阈值部署升档慢；检测延迟由 split 事件驱动（~写满一个 threshold 的时间） |

### 3.5.2 逐项分析

**触发机制**：Uniffle 的 server 端检测最直接，但它要求 server 维护 partition 级大小统计并改响应协议；Celeborn 的 worker 已经有等价物——`checkDiskFullAndSplit` 在文件超阈值时返回 SOFT_SPLIT，executor 收到后按**原生行为**发 revive 上报——本方案零 worker 改动、executor 零额外判定逻辑，LM 消费这个现成信号。

**并行度决策**：CIP-20 最大争议点是 `expectedWorkerSpeed`（评审：异构集群下无法给出合理值）。Uniffle 干脆不估算（固定 N=10）。本方案取中间态：**有界递增**——比 Uniffle 省资源，比 CIP-20 简单且没有 magic number；代价是几次 split 的爬坡延迟。

**为什么判定放 LM 而不是 executor（Phase 1 → 1.1 的演进）**：Phase 1 把 HotTracker 放在 executor，实测发现 LM 做同样判定的数据质量全面更优——(a) epoch 开始时间是 LM 分配时的**真实创建时间**（executor 只能用收到 revive 响应的时刻，偏小偏激进）；(b) 写满时间用**全局第一个** SOFT_SPLIT 上报（executor 版推送稀疏时会漏判）；(c) **唯一决策者**，不需要"多 executor 各自判定 + LM 取 max + 分布式去抖"。同时 executor 的 proto 字段、判定代码全部删掉，写路径之外的改动归零——决策集中、执行分布，与 Celeborn"LM 管分配、client 管传输"的分层一致。

**executor 间一致性**：Uniffle 靠 shuffle handle 下发 split server 全集；CIP-20 各 executor 各自 revive 只看到"自己那份"。本方案：**revive 响应携带该 partition 当前活跃 location 全集**（LM 本来就掌握全局），任何 executor 一次 revive 即与全局收敛。

**协议与兼容**：PR#3260 把 `PbRevivePartitionInfo.partition` 直接改成 `repeated` 是 wire 破坏性变更。本方案只新增一个 `additionalPartitions` 字段（proto3 additive），老 client 忽略新字段、新 client 缺新字段时退化为单 location。Uniffle 与多副本互斥；本方案下 replica 是每个 location 的 slot 属性，K 个 location 各带 replica，机制不变。

**复杂度**：Uniffle 依赖 reassign 大框架；CIP-20 有滑窗 Monitor。本方案：executor 只有一个 252 行的 LocationGroup（薄包装+懒加载），LM 有一个稀疏 HotState（无滑窗、无速率估算）。是把三方里"已被验证有效的最小部件"组合：Uniffle 的简单决策与全集下发 + CIP-20 的 1:N 写骨架 + Celeborn 既有读侧能力。

---

# 第二部分：技术方案（Phase 1.1，按实际代码修订）

## 4. 目标与非目标

**目标（已达成）**
- 单 partition 同时写多个 PartitionLocation（默认并行度 1，热点自动升档，上限可配）；
- location 不可用（SOFT_SPLIT/HARD_SPLIT/push 失败）时写不阻塞：立即切换候选 location，后台补充；
- **热点判定集中在 LM（driver），executor 只保留写路径**；
- 读侧、worker、master、Spark/Flink 集成层零改动；
- 双向版本兼容（新老 client × 新老 LM 均不报错，功能自动降级）。

**非目标（Phase 2）**
- worker/client 侧速率统计（解决"检测延迟=写满一个 threshold"的根本手段）；
- worker 主动过载上报（SOFT_SPLIT_OVERLOAD）；
- MAP partition 类型（Flink hybrid shuffle）；
- 并行度降档。

## 5. 总体数据流（Phase 1.1 实现版）

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ LocationGroup.currentFor(mapId)          ← mapId % K 选活跃 location
             ├─ 正常 → 走现有 push/merge 路径（PushState 按 host 分桶，天然兼容）
             ├─ SOFT_SPLIT → group.retire(epoch, SOFT_SPLIT)（排水语义，写不阻塞）
             │     └─ 首次退休才发原生 ReviveRequest(epoch, SOFT_SPLIT) 上报 ← 唯一 executor 义务
             ├─ HARD_SPLIT / push 失败 → retire + 预置 SUCCESS：
             │     有另一活跃 location 时重推线程立即换目标（不等 LM 响应）
             └─ 全部不可用 → 现有同步 revive 路径
LM (ChangePartitionManager → PartitionHotnessTracker):
  收到带 cause 的 revive → 退休 epoch 出活跃集合；
    计量条件 (cause ∈ {SOFT_SPLIT, HARD_SPLIT} 且旧 location 的 worker 仍可用) 满足时
    HotState 判定：fillTime = 首报时刻 - allocTime(epoch)
    < hotPartitionWindow ? desired+1（首报去重、窗口去抖、上限截断）；
    push 失败类 cause 一律只退休不判定（见 §8.2 统一计量规则）
  补差分配 gap = desired - 活跃数（互不相同 worker、epoch 递增，可为 0）
  revive 响应一律返回【活跃 location 全集】(newLocs 放 max epoch + additionalLocs 放其余)
    → 所有 executor 收敛到同一集合
读侧：不变（fileGroups Set + 多 location 串流 + (mapId,attemptId,batchId) 去重）
```

## 6. 协议改动

### 6.1 proto（`common/src/main/proto/TransportMessages.proto`，已实施）
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
- Phase 1 曾短暂引入 `PbRevivePartitionInfo.desiredLocationCount`（client 上报期望并行度），**Phase 1.1 判定移到 LM 后已回收**（未发布，删除安全）。
- **不采用** PR#3260 把 field 3 改 repeated 的做法（wire 破坏性变更）。

### 6.2 Java 层（已实施）
- `ReviveRequest`：**无新增字段**（Phase 1.1 已删除 desiredLocationCount/urgent）——executor 上报与上游原生完全一致。
- `ChangeLocationResponse` 增加第 4 个 case class 字段 `additionalLocs: util.Map[Integer, util.List[PartitionLocation]]`（带默认值，兼容既有构造点）；toPb 写入 `additionalPartitions`，fromPb 读回。
- `RequestLocationCallContext.reply` 增加带默认值的可选参数 `additionalLocations`。
- 兼容性说明：Revive/ChangeLocationResponse 是 **executor client ↔ LM（driver）的应用内消息**，executor 与 driver 必然使用同一 celeborn client jar；proto serde 路径另有 6.1 的 wire 兼容兜底。cpp client 的 `PbPartitionSplit` 路径不动；Flink client 直接消费 `PbChangeLocationResponse` proto，新增字段对其无感。

## 7. Executor client 侧实现（Phase 1.1 瘦身版）

### 7.0 内存开销：薄包装 + 懒加载
- `reducePartitionMap` 值类型为 `LocationGroup`（244 行，其中核心逻辑 ~100 行），初始是**薄包装**：`volatile PartitionLocation single` + `volatile ParallelState parallel = null`（只比现状多一个对象头+一个字段）；
- `ParallelState`（active 列表 / retired 表 / maxEpoch）**只在首次 SOFT_SPLIT/HARD_SPLIT/push 失败或 revive 响应携带多 location 时 inflate**（双重检查锁）；
- `currentFor(mapId)` 快路径：`parallel == null` 直接返回 `single`，与现状开销相同；膨胀后 `single` 不再同步，`active` 列表是唯一事实源；
- 内存账目（5 万 partition/executor）：薄包装增量 ≈ 1MB；ParallelState 仅热点 partition 存在（个位数~几十个）。

### 7.1 LocationGroup 行为语义
- **选择策略 `mapId % K`**（`Math.floorMod`）：`currentFor` 与 `anotherActiveFor` 统一委托私有方法 `pick(mapId, excludeEpoch)`——两遍扫描：先选非退休 location，**soft-retired location 兜底**（SOFT 语义允许排水续写）；全部不可用返回 null。同一 map task 稳定写同一 location（保住 PushState 按 host 聚合语义）；不同 map task 散到不同 worker；
- **退休语义**：`retire(epoch, cause)` 返回是否首次退休（调用方据此保证每 (partition,epoch) 只上报一次 revive）；
- **`anotherActiveFor(mapId, excludeEpoch)`**：HARD_SPLIT/push 失败时在其余活跃 location 中挑一个立即重推；
- **全集收敛**：`mergeAll(locations)`（`synchronized`，消除并发 revive 响应下"检查重复→插入"的竞态）以 LM 下发的活跃全集为准，按 epoch 有序插入——不同 executor 收敛到**相同顺序**的 active 列表，`mapId % K` 分派一致；跳过本地已退休 epoch；单 location 响应走 `updateSingle` 保持薄包装不膨胀；
- K 变化时 mapId 映射偏移——不影响正确性（读侧按 (mapId,attemptId,batchId) 去重）。

### 7.2 ShuffleClientImpl 接入（实现版）
| 位置 | 实现 |
|---|---|
| `reducePartitionMap` | 值类型 `LocationGroup`；私有 `getPartitionLocationMap()`；**公开 `getPartitionLocation()` 签名不变**，内部投影 `group.latest()` |
| `pushOrMergeData` | 选 location 改为 `group.currentFor(mapId)`；全不可用且开关开启时走同步 revive 后重取 |
| SOFT_SPLIT 回调（pushData 与 mergeData 两处） | **Phase 1.1 简化**：`newlyRetired = group.retire(epoch, SOFT_SPLIT)`；仅首次退休且 `!mapperEnded` 发原生 ReviveRequest（无任何附加字段）。数据已落 worker，写不阻塞，继续写排水 location。**判定逻辑零残留** |
| HARD_SPLIT / push 失败 / mergeData 重提交 | 发 ReviveRequest + `group.retire(epoch, cause)`；若 `anotherActiveFor(mapId, epoch) != null`，**预置 `reviveStatus=SUCCESS`**，重推线程立即从 `currentFor(mapId)` 取另一活跃 location 重推，不等 LM 响应；否则走现有 urgent revive+重推 |
| 重推 SUCCESS 分支 | 取 `group.currentFor(mapId)`（已退休 epoch 被排除） |
| `newerPartitionLocationExists` | `group.maxEpoch() > epoch` |
| `reviveBatch` 响应处理 | 开关开启：`group.mergeAll(partition + additionalLocs)` 全集收敛；关闭：`group.updateSingle(loc)`。含 `loc == null` NPE 保护 |
| mergeData 路径、`PushState` | 零改动 |

### 7.3 executor 的唯一义务：退休上报
并行写下 executor 即使已切到其他活跃 location、不需要新 location，**也必须在首次退休某 epoch 时上报 revive**——这是 LM 感知 split/失败、维护活跃集合与做热点判定的唯一通道。该上报就是上游原生 SOFT_SPLIT revive 行为本身，不算新增负担。

### 7.4 正确性论证
- **去重**：batchId per-mapTask 全局单调，读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序/连续性 → 并行写安全；
- **重推重复**：换 location 重推产生的重复 batch 由读侧去重兜底（CIP-20 评审问答 [a][b] 确认这是既有行为）；
- **soft-retired 排水**：in-flight 不丢（`PartitionSplitMode.SOFT` 语义）；
- **预置 SUCCESS 的安全性**：HARD_SPLIT 时 worker 已拒收该 batch，旧 location 里只有之前成功落盘的 batch；重推到另一 location 后读侧两文件合并+去重，等价于现有"revive 后重推"路径；
- **speculation/rerun/stageEnd 后重跑**：既有路径不变。

## 8. LM 侧实现（热点判定集中地）

### 8.1 HotState（稀疏，per (shuffleId, partitionId)）
热点状态全部收敛在独立组件 **`PartitionHotnessTracker.scala`**（227 行，R3 从 ChangePartitionManager 提取）：`ChangePartitionManager` 持有一个实例，把判定依赖（latestPartitionLocation 查询、worker 可用性查询）以函数注入，所有时间戳由调用方传入（可注入时钟），tracker 可脱离 LM 独立单测。内部维护 `partitionHotStates: shuffleId -> partitionId -> HotState`：
```
activeEpochs   : LinkedHashSet[Integer]   // 活跃 epoch（插入序）
allocTimeMs    : Map[Int, Long]           // 每个 epoch 的真实分配时间
splitReported  : Set[Integer]             // 已判定过的 epoch（首报去重）
desired        : volatile Int = 1         // 期望活跃 location 总数
lastBoostTimeMs: volatile Long = -1       // 去抖：每窗口最多升一次
```
- **稀疏**：普通 partition 无条目，活跃集合推导为 `{ latestPartitionLocation.epoch }`；任何并行模式下的 revive 分配都会建条目（因为新 epoch 的 allocTime 必须记录，供后续判定）；
- **allocTime 来源**（比 executor 版更准的关键）：
  - 新 epoch：`allocateGapLocations` 分配成功时记录（真实创建时间）；
  - epoch 0：`LifecycleManager.handleRegisterShuffle` 成功路径调用 `recordInitialAllocTime`（putIfAbsent，重复注册不覆盖）；
  - 未知 allocTime 的 epoch（老数据）：该次 split 保守不升档；
- shuffle 注销时清理（含 `shuffleInitialAllocTimeMs`）。

### 8.2 热点判定（`onEpochRetired`，在 revive 请求到达时触发）
```
onEpochRetired(shuffleId, partitionId, epoch, oldPartition, cause, now):
  退休 epoch 出活跃集合
  measureEligible = (cause ∈ {SOFT_SPLIT, HARD_SPLIT})
                    && workerAvailableByLocation(oldPartition)   // 统一计量规则（R1）
  if (!measureEligible) return                     // push 失败类 / 已知不可用 worker / null 旧 location：只退休
  if (splitReported.contains(epoch)) return        // 同 epoch 重复上报去重
  allocTime = allocTimeMs[epoch] ?? (epoch==0 ? shuffleInitialAllocTime : null)
  if (allocTime == null || now - allocTime >= windowMs) { markSplitReported; return }  // 不升档
  if (splitReported.add(epoch)
      && (lastBoostTimeMs < 0 || now - lastBoostTimeMs >= windowMs)                    // 去抖
      && desired < maxLocations) {                                                     // 上限
      desired += 1; lastBoostTimeMs = now
  }
```
- **统一计量规则（R1）**：SOFT_SPLIT 与 HARD_SPLIT 统一计量——两者都是"阈值触发的 split"，同样反映快速写满；HARD 模式（`celeborn.client.partitionSplit.mode=HARD`）下热点判定由此激活，不再只有 SOFT 模式受益。两个守卫条件：
  - **worker 必须仍可用**（`workerStatusTracker.workerAvailableByLocation`）：HARD_SPLIT 若由 worker 故障/过载触发（worker 已被 exclude），反映的是 worker 问题而非 partition 热点，不计量；
  - **push 失败类 cause 一律不计量**（PUSH_DATA_FAIL_* / CONNECTION_EXCEPTION 等）：网络/连接问题与分区热度无关，这是原则性排除。
- **判定依据**：fillTime = 全局第一个 split 上报到达时刻 - 该 epoch 真实分配时刻 ≈ `threshold / 该 location 聚合写入速度`。`threshold=1G`、`window=60s` ⇒ 热点线 ≈ **17MB/s 单 location 聚合速度**；
- **epoch 乱序免疫**：每个 epoch 独立对照自己的 allocTime——epoch 10 先于 epoch 5 写满互不影响（executor "间隔法"在这里会误判，这也是不用间隔法的原因）；
- **偏差说明（如实）**：epoch 0 的 allocTime 是 registerShuffle 时刻，mapper 可能更晚才开始写 → fillTime 高估 → 首个 epoch 判定**偏保守**（方向安全）；split 首报时刻略晚于真实写满时刻（一次 push 间隔内），fillTime 略偏大 → 同样偏保守；
- **检测延迟不变**：仍由 split 事件驱动（写满一个 threshold 才感知），这是 split 驱动方案的共同局限，根治需要速率统计（Phase 2）。

### 8.3 分配与回复解耦（`handleRequestPartitions`）
1. **补差分配**（`allocateParallelLocations`）：desired 从 HotState 读（无条目=1），截断上限；`surviving = currentActiveEpochs - change.epoch`，`gap = max(0, desired - surviving.size)`；`allocateGapLocations` 逐次分配（epoch 从 max(latest, surviving)+1 递增），每轮从 candidates 排除已选 worker（best-effort 互不相同）。gap 可为 0；
2. **全集回复**（`replySuccessFullSet`）：对每个请求回复该 partition 当前活跃全集（`newLocs` 放 max epoch，`additionalLocs` 放其余），本轮分配 0 个也回全集——executor 间收敛；
3. 分配成功后登记活跃 epoch 与新 epoch 的 allocTime；失败路径不变；
4. 早返回路径（本地已有更新 location）同样回复 `latestLoc + additionalLocs`；
5. `updateLatestPartitionLocations` 用 `map.merge` 保留 max epoch（非并行路径同样受益）。

### 8.4 退休 location 的 commit：维持现状
SOFT_SPLIT 语义允许排水续写，提前增量 commit 会使排水 push 命中已提交文件（worker 按 "already committed" 拒绝），引入 revive 风暴。维持 StageEnd 全量 commit。HARD_SPLIT 既有增量 commit 路径不动。

### 8.5 不动的部分
- Master / SlotsAllocator：不动（K 个 location 用现有 `reserveSlotsWithRetry` 逐次 reserve）；
- worker：不动（SOFT_SPLIT 阈值触发是现成信号）；
- `workerStatusTracker.excludeWorkerFromPartition`：逻辑不变。

## 9. 配置项（`CelebornConf.scala`；`docs/configuration/client.md` 已再生成）

| 配置 | 默认 | 生效侧 | 说明 |
|---|---|---|---|
| `celeborn.client.shuffle.parallelWrite.enabled` | false | client + LM | 总开关 |
| `celeborn.client.shuffle.parallelWrite.maxLocationsPerPartition` | 4 | **LM** | 单 partition 活跃 location 上限 |
| `celeborn.client.shuffle.parallelWrite.hotPartitionWindow` | 60s | **LM** | 单 location 写满耗时的热点判定窗口（兼作升档去抖间隔） |

开关关闭时所有代码路径与现状等价（LocationGroup 薄包装快路径、LM 走原有 `replySuccess`、不建 HotState）。

## 10. 兼容性矩阵

| 组合 | 行为 |
|---|---|
| 新 client + 新 LM | 全功能 |
| 新 client + 老 LM | 响应无 additionalPartitions → executor 的 mergeAll 只收单 location，退化为现状，无异常 |
| 老 client + 新 LM | 响应新字段被老 client 忽略；LM 的 HotState 判定照常（老 client 的原生 revive 就是判定输入），只是老 client 不会使用多 location |
| worker 任意版本 | 无感（协议未动 worker 面） |
| Flink client | 直接消费 `PbChangeLocationResponse` proto，新增字段无感；不启用并行写 |
| cpp client | PbPartitionSplit 路径不动，不启用并行写 |

## 11. 实际改动清单

Phase 1（commit `f398d73dc`）+ Phase 1.1（commit `5342d934c`，判定迁移 LM + LocationGroup 简化）+ R1–R3（统一计量规则 + 热点组件提取，待提交）合计：

| 文件 | 内容 | 规模 |
|---|---|---|
| `common/src/main/proto/TransportMessages.proto` | `additionalPartitions` 一个字段 | +2 |
| `common/.../message/ControlMessages.scala` | additionalLocs 字段 + 双向 serde | +25 |
| `common/.../CelebornConf.scala` + `docs/configuration/client.md` | 3 个配置 + 文档再生成 | +43 |
| `client/.../LocationGroup.java` | 薄包装 + ParallelState + 统一 pick 选择（无判定逻辑） | 244 |
| `client/.../ShuffleClientImpl.java`、`ReviveManager.java` | §7.2 接入；R2 提取 `handleSoftSplitRetire` / `retireAndPresetIfAnotherActive` 收敛三处重复 | ~+200 |
| `client/.../ChangePartitionManager.scala` | 补差分配 + 全集回复；热点状态全部委托给 tracker（R3 后自身 734 行） | ~+250 |
| `client/.../PartitionHotnessTracker.scala`（R3 新增） | HotState + 统一计量判定（R1）+ 依赖注入（latestEpoch / workerAvailable / 时钟） | 227 |
| `client/.../LifecycleManager.scala`、`RequestLocationCallContext.scala` | registerShuffle 记录 allocTime、additionalLocs 透传 | ~+60 |
| `client/src/test/.../LocationGroupSuiteJ.java` | 5 例 | 139 |
| `client/src/test/.../ChangePartitionManagerParallelWriteSuite.scala` | 10 例（判定逻辑 + 分配回复；HARD_SPLIT 例适配统一计量规则） | 523 |
| `client/src/test/.../PartitionHotnessTrackerSuite.scala`（R3 新增） | 4 例：HARD+健康升档 / HARD+不可用不升档 / push 失败不升档 / HARD 与 SOFT 等价 | 91 |

## 12. 验证

**Phase 1（executor 判定版，commit f398d73dc）已验证**：
- 目标单测全绿：LocationGroupSuiteJ 5 + HotTrackerSuiteJ 8 + ChangePartitionManagerParallelWriteSuite 4；
- client 全量回归全绿（JUnit 43 + ScalaTest 37，32 分钟）；`./dev/reformat` 通过。

**Phase 1.1（LM 判定版，commit 5342d934c）验证**：
- `ChangePartitionManagerParallelWriteSuite` 重写为 10 例：fillTime 升/不升档、同 epoch 首报去重、窗口去抖、上限截断、epoch 乱序（epoch 10 先于 epoch 5 互不影响）、allocTime 未知保守不升、epoch 0 用注册时间、补差分配不同 worker、分配 0 仍回全集、desired 在失败 revive 后保持；
- `LocationGroupSuiteJ` 适配新构造器后保留 5 例；`HotTrackerSuiteJ` 删除（逻辑迁移）；
- 定向套件全绿（LocationGroupSuiteJ 5/5 + ScalaTest 43/43）；
- LocationGroup 简化（pick 合并、synchronized mergeAll）经行为等价审查。

**R1–R3（统一计量规则 + PartitionHotnessTracker 提取，待提交）验证**：
- R1：计量条件统一为 `(cause ∈ {SOFT_SPLIT, HARD_SPLIT}) && workerAvailable(oldPartition)`，HARD 模式热点判定激活；push 失败类原则性不计量；零 worker / 零 proto 改动（曾评估 worker 侧文件长度上报方案，被否决：不改 worker）；
- R2：ShuffleClientImpl 提取 `handleSoftSplitRetire` / `retireAndPresetIfAnotherActive`，收敛三处 SOFT_SPLIT 重复块与两处退休+预置重复块（+80/−73，纯重构无行为变化）；
- R3：热点状态提取为 `PartitionHotnessTracker.scala`（227 行），ChangePartitionManager 863→734 行纯委托；tracker 依赖注入可独立单测；
- 新增 `PartitionHotnessTrackerSuite` 4 例（HARD+健康 worker 升档、HARD+不可用 worker 不升档、push 失败不升档、HARD 与 SOFT 等价）；`ChangePartitionManagerParallelWriteSuite` 的 HARD_SPLIT 例改为"worker 不可用不升档"语义；
- 定向套件 47/47 全绿；`./dev/reformat` 通过；
- client 全量回归（覆盖 Phase 1.1 + R1–R3）：进行中，结果回填于此。

**待办（上生产前必须做）**：
- 集成测试（`tests/spark-it`）：`partitionSplit.threshold=10m`、重倾斜 Spark 作业——(a) reducer 数据与原生 shuffle 对拍一致；(b) 该 partition 最终有 >1 个 committed location 且数据无重复无丢失；(c) 写阶段无 revive 长尾（对比开关前后耗时）；
- 故障注入：写中 kill 一台 worker → 写不中断，数据无丢无重；
- 真实集群灰度：小流量开 `parallelWrite.enabled`，观察 worker slot 占用与 StageEnd commit 时长。

## 13. 风险与开放问题
1. **检测延迟**：split 事件驱动，首次判定要等"写满一个 threshold"（边界 ~60s，10G 阈值部署 ×10）；滞后期间等价于现状（单 worker 写），不会更差。根治：worker/client 速率统计（Phase 2）；
2. **爬升延迟**：去抖限制每窗口 +1，K=4 最坏 ~3 窗口；如需更快可放宽去抖或按 fillTime 比例步进（待灰度数据决定）;
3. **epoch 0 判定偏保守**：allocTime=registerShuffle 时刻早于实际开写，fillTime 高估（方向安全）；
4. **HARD_SPLIT 统一计量的残余误判（R1 引入，已如实评估）**：worker 仍可用但 HARD_SPLIT 并非热度所致的两个子情形——(a) 该 partition 数据已被 commit（mapper 已结束后的迟到写）；(b) worker 内存紧张触发的整体 HARD_SPLIT。两者都罕见（(a) 需要迟到写恰好越过阈值；(b) 内存紧张通常先走向 exclude worker），且误判后果有上限：desired 最多 +1/窗口、封顶 maxLocations，仅多占少量 slot，冷却后不继续升。接受该残余误判换取 HARD 模式的热点检测能力；
5. **worker 占用放大**：热点 partition 占 K 台 worker；上限+只升热点 partition 控制风险；
6. **AQE skew read 交互**：`splitSkewedPartitionLocations` 多文件场景理论兼容，必须 IT 回归；
7. **replicate 模式**：K 个 location = 2K 个 slot，需压测 reserve 开销；
8. **StageEnd commit 列表变长**：超大 shuffle 需观察；
9. **LM 单点复杂度**：HotState 稀疏且无滑窗，且已提取为独立的 PartitionHotnessTracker（可单测、依赖注入），ChangePartitionManager 本体只增 ~250 行——CIP-20 评审对 LM 复杂度的警惕值得在上游化时预案（本实现比滑窗方案轻一个量级是主要论据）。

## 14. Phase 2 展望（不在本期）
- worker/client 侧速率统计替代 split 事件驱动（检测延迟 → 10~20s，且与 split 阈值解耦）；
- worker 写入速率监控 + `SOFT_SPLIT_OVERLOAD` 主动上报（CIP-20 Further Optimization + Uniffle server 检测思路）；
- 并行度降档与热点消散回收；
- LM 滑窗估算（可参考 PR#3260 `PartitionLocationMonitor`，仅当速率统计证明必要）。
