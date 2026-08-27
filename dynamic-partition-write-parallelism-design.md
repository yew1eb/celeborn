# Adaptive Partition Write Parallelism 设计

Celeborn reduce partition 的写路径是**单活跃 location**:一个 partition 任一时刻只有一个活跃 PartitionLocation(某 worker 上的一个文件),所有 map task 的数据都写它,文件写满后换下一个。本特性允许热点 partition **并行写多个活跃 location**:mapper 按 `mapId % activeCount` 散到各 location,由 LifecycleManager 根据实测写满速度自适应升档。

## 1. 问题定义

单活跃 location 把一个大分区的**全部聚合写压集中到单点**,在倾斜作业上产生两类症状:

- **SOFT→HARD 窗口塌缩**:N 个 mapper 并发写同一文件,涨速 = 聚合写速(×N);从 SOFT 阈值(默认 1G)到 HARD 上限(2G)的窗口 = 1G / 聚合写速。写得快则窗口只有亚秒级,revive + 路由切换来不及完成就升 HARD_SPLIT,写该 partition 的所有 map task 同步阻塞等新 location。
- **单点写瓶颈**:push RTT 升至秒级,per-worker in-flight 饱和,mapper 线程被 push 队列反压顶住。生产实测(单 reduce partition、26090 mapper):per-task shuffle writeTime p50 = 27.5s 而 task 总时长 p50 仅 30.6s——90% 的时间在等写。

N 个 location 并行写时单 location 涨速 ÷N:SOFT→HARD 窗口同比例拉宽、单 worker 写压 ÷N、某 location split 时其余仍可写,写路径不因切换停顿。

## 2. 方案概览与数据流

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ PartitionLocationGroup.currentFor(mapId) ← mapId % 可写数
             │   (可写 = 非退休 + SOFT_SPLIT;soft 文件在 2G 硬分裂前持续可写)
             ├─ 正常 → 现有 push/merge 路径(PushState 按 host 分桶,天然兼容)
             ├─ SOFT_SPLIT → retire(epoch, SOFT)(保持可写,不阻塞),首次退休上报 LM
             ├─ HARD_SPLIT / push 失败 → retire + 预置 SUCCESS:
             │     有另一可写 location 时重推线程立即换目标(不等 LM 响应)
             └─ 全部不可写 → fallback 到 latest()(已知最新 location):拒收重新触发异步
                  revive(携带退休上报),LM 分配新 location 后下一批复位——与基线自愈同形

LM (ChangePartitionManager → PartitionHotnessTracker):
  收到带 cause 的 revive → 活跃集维护:SOFT_SPLIT 且 worker 可用 → epoch 保留;
    其余(HARD_SPLIT / push 失败 / worker 不可用)→ epoch 移出活跃集(终态,迟到 SOFT 不复活)
  热点判定(cause ∈ {SOFT, HARD} 且 worker 可用):
    fillTime = 首报时刻 - allocTime(epoch) < hotWindow ?
    desired = ceil(K × window / fillTime)   (K = 测量时活跃数;单调递增、封顶)
  补差分配 gap = desired - 活跃数(互不相同 worker、epoch 递增)
  revive 响应返回活跃 location 全集(max epoch 为主,其余为 additionals)
    → 所有 executor 收敛到同一 epoch 有序集合

读侧:不变(fileGroups Set + 多 location 串流 + (mapId, attemptId, batchId) 去重)
```

热点判定、活跃集记账、分配全部在 LM(driver);executor 只保留写路径与退休上报。

## 3. 关键设计决策

1. **fillTime 实测,不用速率估算**:fillTime = 某 epoch 的首个 split 上报时刻 − 该 epoch 真实分配时刻,per-epoch 独立对照,一次事件即可判定。对比 CIP-20/PR#3260 的"split 事件计数 × 假定字节数 + `expectedWorkerSpeed=10MB/s` 静态魔数"(社区评审的主要质疑):无需任何速度假设,hotWindow 即 SLO——"单 location 写满应慢于窗口"。
2. **比例步进含 K 因子**:测得的 fillTime 是当前并行度 K 下的单 location 写满时间,聚合写满速率 = K/fillTime,故目标 = `ceil(K × window / fillTime)`。不乘 K 则目标被低估 K 倍,desired 冻结在早期 K≈1 时的一次判定。判定一次直达目标,desired 单调递增 + per-epoch 首报去重 + 上限截断,无需去抖窗口。
3. **全集回复保证 executor 一致性**:每次 revive 响应携带该 partition 的完整活跃集(max epoch 为主回复 + `additionalPartitions`),按 epoch 有序插入,所有 executor 一次 revive 即收敛到**相同顺序**的活跃列表,`mapId % K` 分派全局一致。对比 PR#3260 的 50ms 轮询收敛:无收敛延迟、无中间态路由分歧。
4. **SOFT_SPLIT location 是一等路由目标,退休上报一条不丢**:soft 文件在 2G 硬上限前持续可写,把它排除出新写路由会使稳态下所有槽位都处于 soft 态、写压塌缩到最新的 1~2 个 location(线上实证过)。配套地,LM 的活跃集记账依赖每个 (partition, epoch) 的退休 cause 到达——包括"本地已满足"的上报;丢弃任何一条会让 LM 活跃集被死 epoch 撑大、gap 分配归零,最终 executor 无可写 location(线上 `Partition location ... is NULL!` 事故的根因)。

## 4. 协议改动

`PbChangeLocationPartitionInfo` 新增一个 additive 字段(proto3 向后兼容):

```proto
repeated PbPartitionLocation additionalPartitions = 5;
```

- 新 client → 老 LM:拿不到 additionals,退化为单 location,无异常;
- 老 client → 新 LM:忽略未知字段;LM 热点判定照常(老 client 的原生 revive 就是判定输入),只是老 client 不使用多 location;
- `ReviveRequest` 无新增字段;一条 Revive 消息可携带同 partition 的多个退休 epoch 条目,LM 按 distinct partition 计数完成响应、同 partition 首条回复生效;
- worker / master / Flink / cpp 协议面零改动。

不采用 PR#3260 把 `partition` 单值改 repeated 的做法(多元素对旧 client 有 merge 畸形风险)。

## 5. Executor 侧实现

### PartitionLocationGroup(薄包装,懒膨胀)

`reducePartitionMap` 值类型改为 `PartitionLocationGroup`:未 split 的 partition 只是 `volatile single` + `null` 的 `ParallelState`(比原先多一个对象头),首次 split/失败/多 location 响应才 inflate 出 active 列表 + retired 表。5 万 partition 的 executor 增量内存 ≈ 1MB;ParallelState 仅热点 partition 存在。

- **路由**:`currentFor(mapId)` / `anotherUsableFor` 委托 `pick(mapId, excludeEpoch)`——快照 active 列表单遍收集可写子集(非退休 + soft),`floorMod(mapId, size)` 均匀分派;同一 map task 稳定写同一 location(保住 PushState 按 host 聚合)。
- **退休**:`retire(epoch, cause)` CHM compute 原子,返回是否首次(每 epoch 只上报一次);cause 可升级(SOFT→HARD)不可降级;与全集 merge 同 monitor,防墓碑写与清理交错。
- **全集收敛**:`mergeActiveLocations(locations, fullSet)` 按 epoch 有序插入、跳过本地已退休 epoch;`fullSet=true` 时清理 LM 已消化(全集中不再出现)的退休条目。仅携带 additionals 的响应才视为全集——单元素响应(老 LM / 冷 partition)不当全集,避免误清 soft-retired 条目。
- **诊断视图**:`activeEpochsSnapshot()`/`retiredEpochsSnapshot()` 供失败信息。

### ShuffleClientImpl 接入

| 路径 | 行为 |
|---|---|
| SOFT_SPLIT 回调 | `retire(epoch, SOFT)`(保持可写),首报且 mapper 未结束时上报;数据已落盘,零阻塞 |
| HARD_SPLIT / push 失败 | `retire` + 若有另一可写 location 则预置 `reviveStatus=SUCCESS`,重推线程立即换路不等 LM |
| 全部不可用 | fallback 到 `latest()`(已知最新 location):worker 拒收重新触发异步 revive(ReviveManager 携带退休上报,LM 据此收缩活跃集、分配新 location),后续批次路由到新 epoch;与基线"推旧 location 直到 LM 给新的"自愈同形;HARD_SPLIT 拒收的重推不消耗 revive 预算 |

### 并发要点

Group 被四类线程并发访问(push 线程、push 回调、ReviveManager 调度器、push 线程直连的同步 revive):读路径 COW+CHM 快照无锁;`retire`/`mergeActiveLocations`/`updateLatest` 同 group monitor;`retire` 首报信号靠 CHM compute 原子。LM 侧同一 partition 的批处理由条纹锁 + `inBatchPartitions` 去重串行。

### 正确性

- batchId per-mapTask 全局单调,读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序 → 并行写与重推重复 batch 均安全;
- soft 续写:SOFT_SPLIT 语义下 worker 持续接收该文件写直到 2G,`partitionSplitMaximumSize` 是硬上限;
- speculation / rerun / stageEnd 后重跑:既有路径不变。

## 6. LM 侧实现

### PartitionHotnessTracker(独立可单测)

per (shuffleId, partitionId) 的稀疏 HotState:`activeEpochs`(可写 epoch,soft 保留)、`hardRetiredEpochs`(终态,防迟到 SOFT 复活)、`allocTimeMs`、`splitReported`(首报去重)、`desired`(单调递增)。依赖(latestEpoch / workerAvailability / numMappers)以函数注入,时钟由调用方传入。

`onEpochRetired`(每个退休上报到达时):
- 活跃集维护:SOFT 且 worker 可用 → 保留;其余 → 移除(终态);
- 计量(SOFT/HARD 且 worker 可用):`fillTime = max(1ms, now − allocTime)`,若 < hotWindow 则 `desired = min(cap, ceil(K × window / fillTime))`,K = 报告时活跃数(soft 保留的已计入,被移除的补回 1);
- push 失败类 cause 原则性不计量(与热度无关)。

allocTime 来源:新 epoch 由分配登记;epoch 0 用 registerShuffle 时刻(偏保守,方向安全);未知的保守不升档。

### 分配与回复(`ChangePartitionManager`)

- `desired` 截断于 `cap = maxLocations > 0 ? maxLocations : numMappers`(路由是 `mapId % activeCount`,超过 mapper 数的 location 必有空转,mapper 数是天然上限);
- `gap = max(0, cap 内 desired − 活跃数)`,逐次分配(epoch 递增、best-effort 不同 worker),candidates 耗尽即停;
- 登记用 **reserve 成功后的实际 epoch**(`reserveSlotsWithRetry` 失败重试会换 epoch,事前计划会与实际分叉,泄漏槽位);
- 活跃集中位于不可用 worker 的 epoch 被过滤并终态退休(死 worker 永远等不到退休上报);
- 全集回复:max epoch 为主 + 其余(含 soft)为 additionals;分配 0 个也回全集。

## 7. 配置与可观测性

| 配置 | 默认 | 说明 |
|---|---|---|
| `celeborn.client.shuffle.adaptivePartitionWriteParallelism.enabled` | false | 总开关;关闭时所有路径与现状等价 |
| `...adaptivePartitionWriteParallelism.maxLocations` | -1 | 活跃 location 上限 = min(配置值, 该 shuffle 的 mapper 数);-1 = 仅按 mapper 数(路由 mapId % K,超过 mapper 数必空转,天然上限) |
| `...adaptivePartitionWriteParallelism.hotWindow` | 60s | 热点判定窗口;升档目标 = ceil(K × 窗口 / 写满耗时) |

观测点(均一次性,无重复刷屏):LM 侧升档判定 / 补差分配 / 分配不足(INFO/WARN);executor 侧并行激活 / SOFT 首报退休(含换路去向)/ 全不可用 fallback(INFO);per-batch 重推成功(DEBUG)。

## 8. 测试

| 套件 | 例数 | 覆盖 |
|---|---|---|
| `PartitionLocationGroupSuiteJ` | 9 | 快路径、soft 参与路由/hard 排除、cause 升级、全集收敛与清理、乱序 epoch、并发 pick×merge、epoch 快照视图 |
| `PartitionHotnessTrackerSuite` | 12 | 计量守卫(不可用 worker/push 失败)、K 因子缩放、fillTime 下限与 -1=mapper 数上限、显式上限优先、单调不降、soft 保留/移除、迟到 SOFT 不复活 |
| `ChangePartitionManagerAdaptiveParallelismSuite` | 9 | 升档+补差分配、超窗不升、allocTime 未知保守、首报去重、比例步进、epoch 乱序、gap=0 仍回全集、并发 revive 收敛 |
| `RequestLocationCallContextSuite` | 1 | 同 partition 重复回复忽略、按 distinct 数完成响应 |

## 9. 性能验证(生产)

单 reduce partition 承接全部 26090 mapper 的极端倾斜作业(9.4TB shuffle 写):

| 指标 | 开启前 | 开启后 | 改善 |
|---|---|---|---|
| 写侧 stage 耗时 | 21m35s | 7m00s | 3.1×(数据量还大 59%) |
| per-task writeTime p50 / p90 | 27.5s / 110.8s | 615ms / 1.8s | 45× / 61× |
| total shuffle write time(全集群线程) | 607.5h | 6.0h | 101× |
| 每 GB 写线程耗时 | 361s/GB | 2.3s/GB | 157× |
| shuffle read 聚合吞吐 | 17.6GB/s | 26.3GB/s | +49%(fetchWait/GB 持平) |

## 10. 风险与限制

1. **检测延迟 = 写满一个 threshold**:仍由 split 事件驱动(1G 阈值约 60s 边界);滞后期间等价于现状,不会更差。根治需 worker/client 速率统计(未来工作);
2. **desired 只升不降**:一次误判(如极短 fillTime)在整个 shuffle 生命周期不可回退,后果由上限封顶;epoch 0 判定偏保守(方向安全);
3. **split 事件率与并行度无关**:事件率 = 聚合吞吐 / split 阈值。所需并行度远超集群 worker 数的作业(如单 partition 承接全部 mapper)超出本特性能力范围,应业务侧 salting/repartition;
4. **worker 占用放大**:热点 partition 占 K 台 worker,K × 2 × partitionSplitMaximumSize 磁盘;replicate 模式 slot 翻倍;
5. **AQE skew read / StageEnd commit 变长**:理论兼容,上线前 IT 回归。

## 11. 未来工作

- worker/client 侧速率统计替代 split 事件驱动(检测延迟降到 10~20s,与 split 阈值解耦);
- 并行度降档与热点消散回收;
- worker 过载主动上报(SOFT_SPLIT_OVERLOAD)。
