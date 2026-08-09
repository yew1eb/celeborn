# 动态分区写并行度设计（结合 Uniffle partition split 与 CIP-20）

> **状态**：Phase 1 已实现并通过验证（见 §11-§12）。本文档第二部分已按实际代码修订。

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

## 3.5 三方方案详细对比（Uniffle vs CIP-20/PR#3260 vs 本 Phase 1）

### 3.5.1 总览表

| 维度 | Uniffle partition split | CIP-20 / PR#3260 | 本方案 Phase 1（已实现） |
|---|---|---|---|
| **触发主体** | shuffle server（绝对大小阈值 `rss.server.huge-partition.split.limit`，示例 20G） | client 上报 split 事件，LM 侧滑窗估算速率 | client 本地：复用 worker 既有 SOFT_SPLIT 信号，按**单个 location 写满耗时**判定热点 |
| **触发信号质量** | 直接、可靠（server 掌握真实大小）；但阈值调大后同样稀疏 | split 频率；评审指出生产阈值 10G 时信号太钝 | 同受阈值大小影响（接受此局限，只升档慢、不会升错） |
| **并行度决策** | 固定 N（`partitionSplitLoadBalanceServerNumber`，默认 10），一次到位 | 动态：`ceil(pushSpeed / expectedWorkerSpeed) - active`，上限=mapper 数 | 有界递增：每个"写满耗时 < 窗口"的 location 触发 +1，窗口内去抖，上限可配（默认 4）；不估算 worker 速度 |
| **决策位置** | driver（RssShuffleManagerBase） | LM（PartitionLocationMonitor） | client 提议（desired 总数）+ LM 终审（上限截断、全集下发） |
| **写分派 key** | `taskAttemptId % (serverSize-1)+1` | `mapId % size` | `mapId % K`（与二者一致） |
| **split 时写是否阻塞** | 不阻塞（writer 本地 fast-switch） | 不阻塞（切候选 location，非紧急 revive 补充） | 不阻塞（同 CIP-20；HARD_SPLIT/失败直接重推到其他活跃 location） |
| **executor 间一致性** | shuffle handle 序列化下发 split server 全集 | 未明确（各 executor 各自 revive） | **revive 响应返回活跃 location 全集**，各 executor 收敛到同一 epoch 集合 |
| **读侧改动** | 无（bitmap 去重） | 无（复用多 epoch 串流 + batch 去重） | 无（同 CIP-20） |
| **server/worker 改动** | 有（检测 + 响应携带 needSplitPartitionIds） | 无 | 无 |
| **协议改动** | reassign gRPC + handle 携带 split 状态（较大） | Revive 响应 partition 改 repeated（**wire 破坏性**） | 仅 proto3 新增 optional 字段，**wire 兼容** |
| **兼容性** | 与多副本不兼容（代码直接抛异常） | 评审点名兼容复杂 | 双向降级安全（§10 矩阵）；与 Celeborn 副本机制兼容 |
| **退休数据 commit** | 无此概念 | 未明确 | 维持现状（StageEnd 全量 commit），不引入已提交文件竞态 |
| **新增组件** | ReassignExecutor、MutableShuffleHandleInfo 等（依赖 reassign 大框架） | LocationManager(~390行)+Monitor+滑窗 Hub | LocationGroup(349行，含 HotTracker)，无滑窗无锁状态机 |
| **实现规模** | 大（框架级，数千行） | ~3800 行（WIP，无测试） | **~1450 行含测试**（591 行改动 + LocationGroup 349 行 + 测试 526 行） |
| **主要局限** | 依赖 reassign；与副本互斥；并行度固定 | 速率估算难（magic number）；兼容复杂；社区未接受 | 热点窗口是经验值；只升不降；高阈值部署升档慢 |

### 3.5.2 逐项分析

**触发机制**：Uniffle 的 server 端检测最直接，但它要求 server 维护 partition 级大小统计并改响应协议；Celeborn 的 worker 已经有等价物——`checkDiskFullAndSplit` 在文件超阈值时返回 SOFT_SPLIT，这就是现成的"server 检测"信号，Phase 1 直接消费它，**零 worker 改动**拿到与 Uniffle 等价的触发源。

**并行度决策**：CIP-20 最大争议点是 `expectedWorkerSpeed`——评审指出异构集群/动态负载下无法给出合理值，作者只能回复"我们生产设了 10MB/s"。Uniffle 干脆不估算：固定 N=10 一次给足。Phase 1 取中间态：**有界递增**——比 Uniffle 省资源（不热不升、热也只升到够用），比 CIP-20 简单且没有 magic number；代价是几次 split 的爬坡延迟。

**executor 间一致性**：这是 Uniffle 有而 CIP-20 缺失的一环。Uniffle 靠 shuffle handle 把 split server 全集序列化下发给所有 task；CIP-20 各 executor 各自 revive，只看到"自己那份"新 location，不同 executor 的活跃集合会长期不一致（mapId%K 分派发散，虽不影响正确性但分布不均）。Phase 1 用更轻的手段达到同等效果：**revive 响应携带该 partition 当前活跃 location 全集**（LM 本来就掌握全局），任何 executor 一次 revive 即与全局收敛。

**协议与兼容**：PR#3260 把 `PbRevivePartitionInfo.partition` 直接改成 `repeated` 是 wire 破坏性变更，这也是评审兼容讨论（[f]-[j]）的来源。Phase 1 只用 proto3 新增字段，老 client 忽略新字段、新 client 缺新字段时退化为单 location，不需要协商机制。Uniffle 的兼容性短板是与多副本互斥；Phase 1 下 Celeborn 的 replica 是每个 location 的 slot 属性，K 个 location 各带 replica，机制不变。

**复杂度**：Uniffle 方案建立在 reassign 大框架上，Celeborn 没有也不需要——读侧 `reducerFileGroupsMap` 本来就是 `partitionId -> Set[PartitionLocation]`，不需要 handle 携带 split 状态。CIP-20 的滑窗 Monitor 被 HotTracker（LocationGroup 内部类，~50 行逻辑）取代。Phase 1 是把三方里"已被验证有效的最小部件"组合：Uniffle 的简单决策与全集下发 + CIP-20 的 1:N 写骨架 + Celeborn 既有读侧能力。

---

# 第二部分：Phase 1 技术方案（已实现，按实际代码修订）

## 4. 目标与非目标

**目标（已达成）**
- 单 partition 同时写多个 PartitionLocation（默认并行度 1，热点自动升档，上限可配）；
- location 不可用（SOFT_SPLIT/HARD_SPLIT/push 失败）时写不阻塞：立即切换候选 location，后台补充；
- 读侧、worker、master、Spark/Flink 集成层零改动；
- 双向版本兼容（新老 client × 新老 LM 均不报错，功能自动降级）。

**非目标（Phase 2）**
- LM 集中式速率估算/滑窗（PR#3260 PartitionLocationMonitor）；
- worker 主动过载上报（SOFT_SPLIT_OVERLOAD）；
- MAP partition 类型（Flink hybrid shuffle）。

## 5. 总体数据流（实现版）

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ LocationGroup.currentFor(mapId)          ← mapId % K 选活跃 location
             ├─ 正常 → 走现有 push/merge 路径（PushState 按 host 分桶，天然兼容）
             ├─ SOFT_SPLIT → HotTracker.onSoftSplit 计量该 epoch 写满耗时 → 决定 desired
             │     └─ retire(epoch, SOFT_SPLIT)；首次观测才发 ReviveRequest(desired, urgent=false)
             │        （数据已落 worker，写不阻塞，继续写排水中的 location）
             ├─ HARD_SPLIT / push 失败 → retire(epoch) + presetSuccessIfAnotherActive：
             │     若存在另一活跃 location，预置 reviveStatus=SUCCESS，
             │     重推线程立即把 batch 推到 currentFor/另一活跃 location（不等 revive 响应）
             └─ 全部不可用 → 现有同步 revive 路径
LM: 稀疏活跃 epoch 集合；按 max(desired) 补差分配（不同 worker、epoch 递增）；
    revive 响应一律返回【活跃 location 全集】(newLocs 放 max epoch + additionalLocs 放其余)
    → 所有 executor 收敛到同一集合
读侧：不变（fileGroups Set + 多 location 串流 + (mapId,attemptId,batchId) 去重）
```

## 6. 协议改动（全部 wire 兼容的新增字段）

### 6.1 proto（`common/src/main/proto/TransportMessages.proto`，已实施）
```proto
message PbRevivePartitionInfo {
  int32 partitionId = 1;
  int32 epoch = 2;
  PbPartitionLocation partition = 3;
  int32 status = 4;
  // Desired total number of active locations for the partition, 0/absent means 1.
  int32 desiredLocationCount = 5;
}
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
- **不采用** PR#3260 把 field 3 改 repeated 的做法（wire 破坏性变更）。

### 6.2 Java 层（已实施）
- `ReviveRequest` 新增两个 public 字段：`int desiredLocationCount = 1` 与 `boolean urgent = true`。
  - `desiredLocationCount` 上 wire（toPb/fromPb 全链路，proto 缺省 0 在 fromPb 侧 `Math.max(..., 1)` 兜底）；
  - **`urgent` 不上 wire，是纯 client 侧标记**：当前实现中它仅表达语义（SOFT_SPLIT 的 revive 本来就是 fire-and-forget、没有重推任务等待它），没有消费者；保留作为后续扩展（如 LM 侧优先处理 urgent 请求）的钩子。
- `ChangeLocationResponse` 增加第 4 个 case class 字段 `additionalLocs: util.Map[Integer, util.List[PartitionLocation]]`（带默认值 `new util.HashMap()`，兼容既有构造点），替代设计初稿的"tuple 扩成 4 元组"——对既有 `(status, available, loc)` 解构零影响。toPb 写入 `additionalPartitions`，fromPb 读回。
- `RequestLocationCallContext.reply` 增加带默认值的可选参数 `additionalLocations`，`ChangeLocationsCallContext` 聚合后与 `ChangeLocationResponse` 一起发出。
- 兼容性说明：Revive/ChangeLocationResponse 是 **executor client ↔ LM（driver）的应用内消息**，executor 与 driver 必然使用同一 celeborn client jar（同一作业 classpath），版本错配在实践中不存在；proto serde 路径另有 6.1 的 wire 兼容兜底。cpp client 的 `PbPartitionSplit` 路径不动（cpp 不启用并行写）；Flink client 直接消费 `PbChangeLocationResponse` proto，新增字段对其无感。

## 7. Executor client 侧实现

### 7.0 内存开销：薄包装 + 懒加载（回应"per-partition 实例是否太重"）

**关键观察**：99% 的 partition 一生都不会 split。现状 `reducePartitionMap` 本来就为每个 partition 存一个 `PartitionLocation` 对象，热路径内存基线已存在，目标是"非热点 partition 增量开销 ≈ 0"。

**实现**（`client/src/main/java/org/apache/celeborn/client/LocationGroup.java`，349 行）：
- `reducePartitionMap` 值类型改为 `LocationGroup`，但 LocationGroup 初始是**薄包装**：`volatile PartitionLocation single` + `volatile long singleLearnTimeMs` + `volatile ParallelState parallel = null`（~40B，只比现状多一个对象头+两个字段）；
- `ParallelState`（active 列表 / retired 表 / HotTracker / maxEpoch）**只在首次 SOFT_SPLIT/HARD_SPLIT/push 失败或 revive 响应携带多 location 时 inflate**（双重检查锁）；
- `currentFor(mapId)` 快路径：`parallel == null` 直接返回 `single`，与现状开销相同；
- 内存账目（5 万 partition/executor）：薄包装增量 ≈ 2MB；ParallelState 仅热点 partition 存在（通常个位数~几十个），每个 ~500B。对比 CIP-20：executor 侧每 partition `PartitionLocationList`（含一把 ReentrantReadWriteLock，~300B+）≈ 15MB+，LM 侧还有每 partition 滑窗。

### 7.1 LocationGroup 行为语义（实现版）
- **选择策略 `mapId % K`**（`Math.floorMod`）：与 Uniffle/PR#3260 一致。同一 map task 稳定写同一 location（保住 PushState 按 host 聚合语义）；不同 map task 散到不同 worker；
- **退休语义**：`retire(epoch, cause)` 返回是否首次退休（调用方据此保证每 (partition,epoch) 只发一次 revive）。`currentFor` 优先选未退休 location，**soft-retired（SOFT_SPLIT）location 兜底**（SOFT 语义允许排水续写，in-flight 不丢）；全部不可用返回 null；
- **`anotherActiveFor(mapId, excludeEpoch)`**：HARD_SPLIT/push 失败时在其余活跃 location 中挑一个立即重推；
- **全集收敛**：`mergeAll(locations)` 以 LM 下发的活跃全集为准，补入本地缺失 epoch、跳过本地已退休 epoch，并保持 `single` 为 max epoch location；单 location 响应走 `updateSingle` 保持薄包装不膨胀；
- K 变化时 mapId 映射偏移——不影响正确性（读侧按 (mapId,attemptId,batchId) 去重，与 batch 落在哪个文件无关）。

### 7.2 ShuffleClientImpl 接入（实现版，`client/.../ShuffleClientImpl.java`，+228 行）
| 位置 | 实现 |
|---|---|
| `reducePartitionMap` | 值类型改为 `LocationGroup`；新增私有 `getPartitionLocationMap()` 返回 group map；**公开 `getPartitionLocation()` 签名不变**，内部投影为 `group.latest()`（外部调用方无感） |
| `pushOrMergeData` | 选 location 改为 `group.currentFor(mapId)`；返回 null（全不可用）且开关开启时走同步 revive（`PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY`）后重取 |
| SOFT_SPLIT 回调（pushData 单推与 mergeData 两处） | `desired = group.onSoftSplit(epoch)` → `newlyRetired = group.retire(epoch, SOFT_SPLIT)`；仅首次退休且 mapper 未结束才发 ReviveRequest（`desiredLocationCount=desired`、`urgent=false`）。数据已落 worker，**写不阻塞**，继续写排水 location |
| HARD_SPLIT 回调 / push 失败路径 / mergeData 重提交 | 先发 ReviveRequest，再 `group.retire(epoch, cause)`；若 `group.anotherActiveFor(mapId, epoch) != null`，**预置 `reviveStatus=SUCCESS`**（`presetSuccessIfAnotherActive` / 内联）——重推线程读到 SUCCESS 后从 `currentFor(mapId)` 取另一活跃 location 立即重推，**不等 LM 响应**；无其他活跃 location 时走现有 urgent revive+重推 |
| `submitRetryPushData` / `submitRetryPushMergedData` 的 SUCCESS 分支 | 从 `reducePartitionMap` 直接取 loc 改为 `group.currentFor(mapId)`（已退休 epoch 被排除）；取不到时按失败处理（`remainReviveTimes` 语义不变） |
| `newerPartitionLocationExists` | 改为 `group.maxEpoch() > epoch` |
| `reviveBatch` 响应处理 | 开关开启：`group.mergeAll(newLocs.partition + additionalLocs)` 全集收敛；关闭：`group.updateSingle(loc)`（仅 epoch 更新才替换，等价旧语义）。另修复 `loc == null` 时 `pushExcludedWorkers.remove(loc.hostAndPushPort())` 的 NPE 隐患（加空值保护） |
| mergeData 路径、`PushState`（按 addressPair 分桶、`limitMaxInFlight` perWorker） | 零改动 |

### 7.3 HotTracker（热点判定）——按"单 location 写满耗时"计量，不按事件间隔

**设计演进说明（重要）**：初版用"相邻两次 SOFT_SPLIT 的间隔"估算速度，但该方法在并行化后失效——K>1 时各 location 被**不同 mapper 子集并行填充，epoch 写满顺序不再按编号**（epoch 10 可能先于 epoch 5 写满），跨 epoch 的事件间隔不再代表"写满 1G 的耗时"。因此改为**按 epoch 各自计量写满耗时**，K=1 与 K>1 统一正确。

**先厘清前提（多 mapper 场景）**：
- REDUCE 模式下，**一个 location 对应 worker 上一个文件，该 partition 的所有 mapper（或 mapId%K 命中的子集）共同往里写**。"文件写满 1G"的速度 = 写入该 location 的**聚合速度**，天然是聚合信号，不需要跨 executor 汇总；
- worker 侧文件超阈值后，**之后每个 push 到该 location 的 mapper 都会各自收到 SOFT_SPLIT**（直到 client 完成切换），且 revive 完成前**同一 epoch 会重复通知**——必须按 epoch 去重，否则间隔≈0 会误判热点瞬间拉满并行度。

**实现**（LocationGroup 静态内部类，状态：每个 executor、每个 partition 各一份）：
```
epochLearnTime  : ConcurrentHashMap<Integer, Long>  // 本 executor 获知每个活跃 epoch 的时刻
splitReported   : Set<Integer>                      // 已计量的 epoch（去重）
currentDesired  : volatile int = 1
lastBoostTime   : volatile long = -1                // 去抖：每窗口最多升一次
```

**计量点**：
- `mergeAll()` / inflate 时：`onEpochLearned(epoch, now)`（薄包装的初始 location 用 `singleLearnTimeMs` 回填）；
- SOFT_SPLIT 回调时：
```
onSoftSplit(epoch, now):
  if (!splitReported.add(epoch)) return currentDesired   // 同 epoch 重复通知，忽略
  start = epochLearnTime.get(epoch)
  if (start == null) return currentDesired               // 未知起点，保守不升
  fillTime = now - start
  if (fillTime < windowMs
      && (lastBoostTime < 0 || now - lastBoostTime >= windowMs)) {   // 去抖
      currentDesired = min(currentDesired + 1, maxLocations)         // 上限默认 4
      lastBoostTime = now
  }
  return currentDesired        // 作为 ReviveRequest.desiredLocationCount 发给 LM
```
- `retire(epoch)` 时清理 `epochLearnTime` / `splitReported`（map 大小有界）。

**为什么这样是对的**：
- **每个 location 的 fillTime 独立计量**，与 epoch 编号顺序无关——epoch 10 先写满、epoch 5 后写满，各自对照各自的起点，互不影响；
- fillTime ≈ `threshold / 该 location 聚合写入速度`。`threshold=1G`、`window=60s` ⇒ 热点线 ≈ **17MB/s 单 location 聚合速度**——达到单 worker 单盘舒适 flush 速度（CIP-20 生产经验 10MB/s 量级）即升档，把流量摊开；
- K=1 时退化为"顺序写满"场景，与间隔法等价。

**偏差与边界（如实说明）**：
- `epochLearnTime` 是本 executor 收到 revive 响应的时刻，**略晚于 location 真实创建时间**（revive 传播延迟，亚秒~秒级）⇒ fillTime 略偏小 ⇒ 轻微偏激进，相对 60s 窗口可忽略；
- 推送极稀疏的 executor 可能观测不到某 epoch 的 split → 不升档。漏判只会升得慢，不会升错；推送频繁的 executor 会测得真实短耗时并升档，**LM 对所有 executor 的 desired 取 max**，最热观测者生效；
- 部署把 split 阈值调大到 10G 时，热点线同比变为 ~170MB/s，信号变钝——接受的局限（§13），窗口可按部署配套调整。

**具体例子**（threshold=1G，window=60s，max=4；100 个 mapper 共写 partition 5）：
```
T=0s     注册，1 个 location（epoch 0），各 executor desired=1
T=80s    epoch 0 被全体 mapper 写满 → SOFT_SPLIT(epoch 0)：fillTime=80s > 60s → 不升档
         （正常速度的正常文件滚动），正常 revive 拿 epoch 1
T=130s   epoch 1 写满 → fillTime≈50s < 60s → 热点！desired=2
         ReviveRequest(desired=2) → LM 补 1 个 location（epoch 2）
         此后 mapId%2：epoch 1 排水，mapper 分流写 epoch 1(排水)+2
T=190s   epoch 2 写满 → fillTime≈60s 临界；假设 58s < 60s → desired=3，
         LM 补 epoch 3；分流到 epoch 2(排水)+3 等
T=500s   epoch 3 写满 → fillTime=310s > 60s → 不升档，稳定在 K=2~3
—— 乱序场景：K=2 时 epoch 5（mapper 子集 A 写）与 epoch 10（子集 B 写）并行填充，
   epoch 10 先写满、epoch 5 后写满：各自 fillTime 独立计量，判定互不干扰 ——
   初版"间隔法"在这里会把两个事件的时间差误判为写满耗时，本版无此问题。
```

**关键语义**：
- **第一次 split 通常不升档**（正常文件滚动 fillTime 长）；只有真正高速 partition 的 fillTime 才会短于窗口；
- **desired 是目标总数不是增量**：多 executor 并发上报时 LM 取 max，天然幂等；
- **去抖**：每窗口每 partition 每 executor 最多升一档，LM 上限截断是最终防线；
- **只升不降**：partition 热点是 stage 级现象，降档收益小、抖动复杂度高，Phase 1 不做；
- 判定全在 **executor 本地**，无 RPC、无滑窗、无 `expectedWorkerSpeed` 之类的 magic number。

### 7.4 正确性论证
- **去重**：batchId per-mapTask 全局单调，同一 map task 的不同 batch 写不同 location 后，读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序/连续性 → 并行写安全；
- **重推重复**：HARD_SPLIT/push 失败换 location 重推产生的重复 batch，与现有重推路径语义一致（读侧去重兜底，CIP-20 评审问答 [a][b] 已确认这是既有行为）；
- **soft-retired 排水**：SOFT_SPLIT 的 location 仍接受在途写（`PartitionSplitMode.SOFT` 语义），`currentFor` 优先选非退休 location、无其他选择时仍可用排水中的，in-flight 不丢；
- **预置 SUCCESS 的安全性**：HARD_SPLIT 时 worker 已拒收该 batch，旧 location 里只有之前成功落盘的 batch；重推到另一 location 后读侧两文件合并+去重，等价于现有"revive 后重推"路径；
- **speculation/rerun/stageEnd 后重跑**：`pushOrMergeData` 现有的"map 不含 partitionId → revive"与 `mapperEnded` 检查路径不变。

## 8. LM 侧实现（`ChangePartitionManager.scala` +266 行、`LifecycleManager.scala` +30 行）

### 8.1 活跃 epoch 登记（稀疏）
`ChangePartitionManager` 新增 `partitionActiveEpochs: shuffleId -> partitionId -> LinkedHashSet[epoch]`，**稀疏**：
- **不为普通 partition 建条目**：未升档 partition 的活跃集合推导为 `{ latestPartitionLocation.epoch }`；
- 仅某 partition 首次按 desired>1 分配多个 location 时建条目；之后分配加入、请求到达时按退休 epoch 移除（`removeActiveEpoch`，`oldEpoch >= 0` 时）、shuffle 注销清理；
- 内存 O(升档过的 partition 数 × 平均活跃数)，可忽略。

### 8.2 分配与回复解耦（`handleRequestPartitions`）
开关开启时：
1. **补差分配**（`allocateParallelLocations`）：对该 partition 取队列内所有请求与本请求的 `max(desiredLocationCount)`，截断到配置上限；`surviving = currentActiveEpochs - change.epoch`，`gap = max(0, desired - surviving.size)`；`allocateGapLocations` 用 `allocateFromCandidates(partitionId, newEpoch-1, remaining, slots)` 逐次分配（epoch 从 max(latest, surviving) +1 递增），**每轮从 candidates 排除已选 worker**（primary/replica 均计入，best-effort 互不相同）。gap 可为 0（其他 executor 已触发过分配）。
2. **全集回复**（`replySuccessFullSet`）：不再依赖"新分配 location 与请求一一对应"，对每个请求回复该 partition **当前活跃全集**——`newLocs` 放 max epoch location（老语义），`additionalLocs` 放其余（`currentActiveLocations` 从 workerSnapshots 按活跃 epoch 查出，snapshot 缺失时回退 latest location）。即便本轮分配 0 个，滞后的 executor 也借响应与全局收敛。
3. 分配成功后登记活跃 epoch 集合（`survivingEpochs ++ newEpochs`，仅 desired>1 或已有条目时）；失败路径（`replyFailure`）不变。
- `handleRequestPartitionLocation` 的**早返回路径**（本地已有更新 location 时）同样回复 `latestLoc + additionalLocs`，保持全集语义一致；
- `LifecycleManager.handleRevive` 新增带默认值参数 `desiredLocationCounts`，逐个透传（proto fromPb 已做 0→1 兜底，这里再 `max(...,1)` 防御）；
- `updateLatestPartitionLocations` 改为 `map.merge` 保留 **max epoch**——一次调用携带同 partition 多个 location 时语义正确；对非并行路径也更稳健（旧实现无条件覆盖，可能被旧 epoch 回退）。

### 8.3 退休 location 的 commit：维持现状
初版曾提议把 SOFT_SPLIT 退休 location 纳入 `registerCommitPartitionRequest` 增量 commit，**撤回**：SOFT_SPLIT 语义允许 location 继续排水写入，提前 commit 会使排水 push 命中已提交文件（worker 按 "already committed" 拒绝），引入 revive 风暴。现状（StageEnd 全量 commit 覆盖所有 epoch）在并行写下同样成立。HARD_SPLIT 的既有增量 commit 路径不动。

### 8.4 不动的部分
- Master / SlotsAllocator：不动（K 个 location 用 LM 现有 `reserveSlotsWithRetry` 逐次 reserve）；
- worker：不动（SOFT_SPLIT 阈值触发是现成信号）；
- `workerStatusTracker.excludeWorkerFromPartition`：逻辑不变。

## 9. 配置项（`CelebornConf.scala`，已实施；`docs/configuration/client.md` 已再生成）

| 配置 | 默认 | 访问器 | 说明 |
|---|---|---|---|
| `celeborn.client.shuffle.parallelWrite.enabled` | false | `clientShuffleParallelWriteEnabled` | 总开关（client 与 LM 同源） |
| `celeborn.client.shuffle.parallelWrite.maxLocationsPerPartition` | 4 | `clientShuffleParallelWriteMaxLocationsPerPartition` | 单 partition 活跃 location 上限（LM 终审截断） |
| `celeborn.client.shuffle.parallelWrite.hotPartitionWindow` | 60s | `clientShuffleParallelWriteHotPartitionWindowMs` | 单 location 写满耗时的热点判定窗口（兼作升档去抖间隔） |

开关关闭时所有代码路径与现状等价（LocationGroup 薄包装快路径、desired 恒 1、LM 走原有 `replySuccess`）。

## 10. 兼容性矩阵

| 组合 | 行为 |
|---|---|
| 新 client + 新 LM | 全功能 |
| 新 client + 老 LM | desiredLocationCount 被忽略、响应无 additionalPartitions → 退化为现状，无异常 |
| 老 client + 新 LM | desired=1，LM 按单 location 分配；响应新字段被老 client 忽略 |
| worker 任意版本 | 无感（协议未动 worker 面） |
| Flink client | 直接消费 `PbChangeLocationResponse` proto，新增字段无感；不启用并行写 |
| cpp client | PbPartitionSplit 路径不动，不启用并行写 |

## 11. 实际改动清单（含验证状态）

| 文件 | 改动 | 规模 |
|---|---|---|
| `common/src/main/proto/TransportMessages.proto` | 2 个新字段 | +4 |
| `common/.../protocol/ReviveRequest.java` | desiredLocationCount / urgent 字段 | +4 |
| `common/.../message/ControlMessages.scala` | additionalLocs 字段 + 双向 serde | +28 |
| `common/.../CelebornConf.scala` + `docs/configuration/client.md` | 3 个配置 + 文档再生成 | +43 |
| `client/.../LocationGroup.java`（新） | 薄包装 + ParallelState + HotTracker | 349 |
| `client/.../ShuffleClientImpl.java`、`ReviveManager.java` | §7.2 接入 | +228 / +3 |
| `client/.../ChangePartitionManager.scala`、`LifecycleManager.scala`、`RequestLocationCallContext.scala` | §8 全部 | +266 / +30 / +31 |
| `client/src/test/.../LocationGroupSuiteJ.java`（新） | 5 例 | 143 |
| `client/src/test/.../HotTrackerSuiteJ.java`（新） | 8 例 | 118 |
| `client/src/test/.../ChangePartitionManagerParallelWriteSuite.scala`（新） | 4 例 | 265 |

合计：10 个文件修改（591 insertions / 54 deletions）+ 4 个新文件，约 1450 行（含测试）。

## 12. 验证结果（已完成）与待办

**已完成**：
1. **目标单测全绿**：`LocationGroupSuiteJ`（5：快路径/retire 切换/mergeAll 全集收敛含乱序 epoch/全不可用/anotherActiveFor）、`HotTrackerSuiteJ`（8：升档/不升档/未知起点/同 epoch 去重/去抖/上限/epoch 乱序/retire 清理）、`ChangePartitionManagerParallelWriteSuite`（4：desired=3 补差分配不同 worker 并回全集/活跃已满分配 0 仍回全集/并发 revive 取 max 幂等/上限截断）；
2. **client 全量回归全绿**（32 分钟）：JUnit 43 例（含 `ShuffleClientSuiteJ` 13、`DataPushQueueSuiteJ`、CelebornInputStream 系列）+ ScalaTest 37 例，0 失败——开关关闭时行为与现状等价；
3. `./dev/reformat` 无额外改动，风格一次通过。

**待办（上生产前必须做）**：
4. **集成测试**（`tests/spark-it`）：`partitionSplit.threshold=10m`、单 partition（或重倾斜 key）、多 mapper 的 Spark 作业——断言 (a) reducer 数据与 Spark 原生 shuffle 对拍一致；(b) 该 partition 最终有 >1 个 committed location 且数据无重复无丢失；(c) 写阶段无 revive 长尾（对比开关前后耗时）；
5. **故障注入**：写中 kill 一台 worker → 写不中断（切其他 location），数据无丢无重；
6. **真实集群灰度**：先小流量开 `parallelWrite.enabled`，观察 worker slot 占用与 StageEnd commit 时长。

## 13. 风险与开放问题
1. **热点窗口 60s 是经验值**：阈值调成 10G 的部署信号稀疏（同 CIP-20 评审质疑），Phase 1 接受此局限（升档慢 ≠ 不正确），Phase 2 滑窗解决；
2. **fillTime 起点偏差**：executor 获知 epoch 的时刻略晚于真实创建，fillTime 偏小、判定略偏激进（秒级，相对 60s 窗口可忽略）；
3. **worker 占用放大**：热点 partition 占 K 台 worker，需校验与 `slot.assign.maxWorkers` 的联动；上限+只升热点 partition（非全局）控制风险；
4. **AQE skew read 交互**：`splitSkewedPartitionLocations` 按 chunk offset 切分，多文件场景理论兼容，必须回归（§12.4）；
5. **replicate 模式**：K 个 location = 2K 个 slot，需压测 reserve 开销；
6. **retire 排水竞态**：SOFT_SPLIT 后新旧 location 同时有 in-flight——读侧去重兜底，已论证（§7.4）；
7. **StageEnd commit 列表变长**：退休 location 增多使 StageEnd 全量 commit 变重，超大 shuffle 需观察（不提前 commit 的原因见 §8.3）；
8. **`urgent` 字段当前仅信息性**：无消费者，保留作后续 LM 优先级调度扩展钩子；如评审认为多余可移除。

## 14. Phase 2 展望（不在本期）
- LM 集中滑窗估算（可直接参考 PR#3260 `PartitionLocationMonitor`/`PartitionSplitTimeSlidingHub` 实现）；
- worker 侧写入速率监控 + `SOFT_SPLIT_OVERLOAD` 主动上报（CIP-20 Further Optimization + Uniffle server 检测思路）；
- 并行度降档与热点消散回收；
- `urgent` 标志的 LM 侧优先级调度。
