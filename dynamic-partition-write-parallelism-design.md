# CIP: Adaptive Partition Write Parallelism(自适应分区写并行)

> Status: Draft — Jira: TODO(待申请 CELEBORN-XXXX)
> Implementation branch: `adaptive-parallelism-write`

## Motivation

Celeborn reduce partition 的写路径是**单活跃 location**:一个 partition 任一时刻只有一个活跃 PartitionLocation(某 worker 上的一个文件),所有 map task 的数据都写它,文件写满后换下一个。这把一个大分区的**全部聚合写压集中到单点**,在倾斜作业上产生两类症状:

- **SOFT→HARD 窗口塌缩**:N 个 mapper 并发写同一文件,涨速 = 聚合写速(×N);从 SOFT 阈值(默认 1G)到 HARD 上限(2G)的窗口 = 1G / 聚合写速。写得快则窗口只有亚秒级,revive + 路由切换来不及完成就升 HARD_SPLIT,写该 partition 的所有 map task 同步阻塞等新 location。
- **单点写瓶颈**:push RTT 升至秒级,per-worker in-flight 饱和,mapper 线程被 push 队列反压顶住。在一个单 reduce partition、26090 mapper 的生产倾斜作业上实测:per-task shuffle writeTime p50 = 27.5s 而 task 总时长 p50 仅 30.6s——90% 的 task 时间在等写。

本特性允许热点 partition **并行写多个活跃 location**:mapper 按 `mapId % activeCount` 散到各 location,由 LifecycleManager 根据实测写满速度自适应升档。N 个 location 并行写时单 location 涨速 ÷N:SOFT→HARD 窗口同比例拉宽、单 worker 写压 ÷N、某 location split 时其余仍可写,写路径不因切换停顿。生产效果见 Proposed Changes · 性能验证(生产个例)(写侧 stage 3.1×、per-task writeTime p50 45×)。

## Public Interfaces

**协议(proto3 向后兼容)**:`PbChangeLocationPartitionInfo` 新增一个 additive 字段:

```proto
repeated PbPartitionLocation additionalPartitions = 5;
```

- `ReviveRequest` 无新增字段(退休上报复用既有字段,一条 Revive 可携带多条目的语义见 Proposed Changes);
- worker / master / Flink / cpp 协议面零改动。

**新增配置**(均在 client 侧,`celeborn.client.shuffle.adaptivePartitionWriteParallelism.*`):

| 配置 | 默认 | 说明 |
|---|---|---|
| `...adaptivePartitionWriteParallelism.enabled` | false | 总开关;关闭时所有路径与现状等价 |
| `...adaptivePartitionWriteParallelism.maxLocations` | -1 | 活跃 location 上限 = min(配置值, 该 shuffle 的 mapper 数);-1 = 仅按 mapper 数(路由 mapId % K,超过 mapper 数必空转,天然上限) |
| `...adaptivePartitionWriteParallelism.hotWindow` | 60s | 热点判定窗口;升档目标 = ceil(K × 窗口 / 写满耗时) |

**观测点**(均一次性,无重复刷屏):LM 侧升档判定 / 补差分配 / 分配不足(INFO/WARN);executor 侧并行激活 / SOFT 首报退休(含换路去向)/ 全不可用触发阻塞 revive 及其成功后的新目标(INFO);per-batch 重推成功(DEBUG)。

## Proposed Changes

### 数据流概览

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ PartitionLocationGroup.currentFor(mapId) ← mapId % 可写数
             │   (可写 = 非退休 + SOFT_SPLIT;soft 文件在 2G 硬分裂前持续可写)
             ├─ 正常 → 现有 push/merge 路径(PushState 按 host 分桶,天然兼容)
             ├─ SOFT_SPLIT → retire(epoch, SOFT)(保持可写,不阻塞),首次退休上报 LM
             ├─ HARD_SPLIT / push 失败 → retire + 预置 SUCCESS:
             │     有另一可写 location 时重推线程立即换目标(不等 LM 响应)
             └─ 全部不可写 → 阻塞 revive(ReviveManager 统一入口,per-partition
                  single-flight,有界 3 次):每次尝试携带全部未消化退休上报,
                  LM 消化后一轮补满活跃集;等锁线程复查可写性,通常零 RPC——
                  ReviveManager 的"本地已满足"判定要求当前有可写 location,
                  不可写则异步请求必到 LM,杜绝空转

关键不变量:revive SUCCESS ⟺ 该 partition 当前存在可写 location(同步路径靠
"携带退休上报 + 逐次重查"维持,见决策 5)。

LM (ChangePartitionManager → PartitionHotnessTracker):
  一条 Revive 先按 partition 分组:每组仅 max-epoch 条目走完整请求/分配路径,
    其余条目 = 纯退休上报,只做记账(commit 注册 + onEpochRetired)——
    批量 revive 可携带同 partition 的大量退休条目;逐条走完整路径时
    每条消息的处理量正比于条目数,分组后正比于 distinct partition 数
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

### 关键设计决策

1. **split 事件驱动 + fillTime 实测,不做速度假设**:fillTime = 某 epoch 的首个 split 上报时刻 − 该 epoch 真实分配时刻,per-epoch 独立对照,一次事件即可判定。split 阈值是**字节尺子**(`partitionSplit.threshold` 默认 1G,worker 侧仅有 min 1m / max 2g 硬边界),与磁盘介质无关;fillTime 把它换算成该 location 的**实测**写速,HDD/SSD/NVMe/混插集群各自自校准。与速率统计、PR#3260 静态魔数等备选方案的完整对比见 Rejected Alternatives。
2. **比例步进含 K 因子**:fillTime 是当前并行度 K 下、聚合写压被 K 路分摊后的单 location 写满时间,聚合写满速率 = K/fillTime,故目标 = `ceil(K × window / fillTime)`。目标若不含 K 则被低估 K 倍,升档在 K>1 后即失效。判定一次直达目标,desired 单调递增 + per-epoch 首报去重 + 上限截断,无需去抖窗口。
3. **全集回复保证 executor 一致性**:每次 revive 响应携带该 partition 的完整活跃集(max epoch 为主回复 + `additionalPartitions`),按 epoch 有序插入,所有 executor 一次 revive 即收敛到**相同顺序**的活跃列表,`mapId % K` 分派全局一致。对比 PR#3260 的 50ms 轮询收敛:无收敛延迟、无中间态路由分歧。
4. **SOFT_SPLIT location 是一等路由目标,退休上报一条不丢**:soft 文件在 2G 硬上限前持续可写;若把它排除出新写路由,稳态下所有槽位都会处于 soft 态,写压将塌缩到最新的 1~2 个 location,并行写形同虚设。配套地,LM 的活跃集记账依赖每个 (partition, epoch) 的退休 cause 到达——包括"本地已满足"的上报;丢弃任何一条会让 LM 活跃集被死 epoch 撑大、gap 分配归零,executor 最终因无可写 location 而失败。批量路径的上报**发送时从 `group.outstandingRetires()` 现取**而不是从队列收集:队列在调度器被超时堵住时积压无上界(单条 Revive 可膨胀到上千条),group 视图只含 LM 未消化的退休 epoch——有界(≤ 活跃集大小)、自动去陈旧、RPC 超时丢失的自动重发。
5. **全部不可写时的阻塞 revive:携带退休上报 + 有界重试 + single-flight,三者缺一即破坏活性**。LM 补差分配按 `gap = desired − LM 簿记活跃数`;只发一条 max-epoch 请求时 LM 只消化 1 个退休,gap≈0,响应回几乎全已退休的旧集合,executor 无可写 location 而失败。携带全部未消化退休上报后 LM 一轮补满 K 个 location;否则每次只补 1 个 → 全 executor 的 mapper 投影到唯一可写 location → 秒级再次 HARD_SPLIT → 分配与退休相互加速的正反馈(churn)。single-flight(per-partition 锁 + 拿锁后复查可写性)把 mass-retire 唤醒的 pusher 线程收敛到每 executor 每 partition 至多 1 个在飞 RPC,避免瞬时 RPC 洪峰压垮 LM dispatcher(默认 60s `requestPartition.askTimeout`)。两个更简的变体均不成立:纯异步 fallback(不阻塞,靠 worker 拒收重触发 revive)在 split 密集期路由长期落空,吞吐显著退化;单发同步 revive(不携上报、不重试)则落入上述 gap≈0 陷阱,直接失败。

### Executor 侧实现

#### PartitionLocationGroup(薄包装,懒膨胀)

`reducePartitionMap` 值类型改为 `PartitionLocationGroup`:未 split 的 partition 只是 `volatile single` + `null` 的 `ParallelState`(比原先多一个对象头),首次 split/失败/多 location 响应才 inflate 出 active 列表 + retired 表。5 万 partition 的 executor 增量内存 ≈ 1MB;ParallelState 仅热点 partition 存在。

- **路由**:`currentFor(mapId)` / `anotherUsableFor` 委托 `pick(mapId, excludeEpoch)`——快照 active 列表单遍收集可写子集(非退休 + soft),`floorMod(mapId, size)` 均匀分派;同一 map task 稳定写同一 location(保住 PushState 按 host 聚合)。
- **退休**:`retire(epoch, cause)` CHM compute 原子,返回是否首次(每 epoch 只上报一次);cause 可升级(SOFT→HARD)不可降级;与全集 merge 同 monitor,防墓碑写与清理交错。
- **全集收敛**:`mergeActiveLocations(locations, fullSet)` 按 epoch 有序插入、跳过本地已退休 epoch;`fullSet=true` 时清理 LM 已消化(全集中不再出现)的退休条目。仅携带 additionals 的响应才视为全集——单元素响应(老 LM / 冷 partition)不当全集,避免误清 soft-retired 条目。
- **诊断视图**:`activeEpochsSnapshot()`/`retiredEpochsSnapshot()` 供失败信息;`outstandingRetires()` 供同步 revive 携带未消化退休上报(仅含仍在 active 列表中的退休 epoch,被全集清理的说明 LM 已消化)。

#### ShuffleClientImpl 接入

| 路径 | 行为 |
|---|---|
| SOFT_SPLIT 回调 | `retire(epoch, SOFT)`(保持可写),首报且 mapper 未结束时上报;数据已落盘,零阻塞 |
| HARD_SPLIT / push 失败 | `retire` + 若有另一可写 location 则预置 `reviveStatus=SUCCESS`,重推线程立即换路不等 LM |
| 全部不可用 | 入口与重推路径统一走 `ReviveManager.reviveUntilWritable`:per-partition single-flight 阻塞 revive(有界 3 次),每次尝试携带全部未消化退休上报,LM 消化后一轮补满活跃集;不带上报的单条请求会让 gap 分配归零、回已退休 epoch(机制分析见决策 5)。配套不变量"revive SUCCESS ⟺ 存在可写 location"——异步路径的满足判定要求 `currentFor(mapId) != null`,不可写则请求必到 LM,不会空转 |

#### 并发要点

Group 被四类线程并发访问(push 线程、push 回调、ReviveManager 调度器、push/重推线程经 ReviveManager 的同步 revive):读路径 COW+CHM 快照无锁;`retire`/`mergeActiveLocations`/`updateLatest` 同 group monitor;`retire` 首报信号靠 CHM compute 原子。同步 revive 由 per-partition single-flight 锁串行,拿锁后先复查可写性再决定是否发 RPC。LM 侧同一 partition 的批处理由条纹锁 + `inBatchPartitions` 去重串行。

#### 正确性

- batchId per-mapTask 全局单调,读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序 → 并行写与重推重复 batch 均安全;
- soft 续写:SOFT_SPLIT 语义下 worker 持续接收该文件写直到 2G,`partitionSplitMaximumSize` 是硬上限;
- speculation / rerun / stageEnd 后重跑:既有路径不变。

### LM 侧实现

#### PartitionHotnessTracker(独立可单测)

per (shuffleId, partitionId) 的稀疏 HotState:`activeEpochs`(可写 epoch,soft 保留)、`hardRetiredEpochs`(终态,防迟到 SOFT 复活)、`allocTimeMs`、`splitReported`(首报去重)、`desired`(单调递增)。依赖(latestEpoch / workerAvailability / numMappers)以函数注入,时钟由调用方传入。

`onEpochRetired`(每个退休上报到达时):
- 活跃集维护:SOFT 且 worker 可用 → 保留;其余 → 移除(终态);
- 计量(SOFT/HARD 且 worker 可用):`fillTime = max(1ms, now − allocTime)`,若 < hotWindow 则 `desired = min(cap, ceil(K × window / fillTime))`,K = 报告时活跃数(soft 保留的已计入,被移除的补回 1);
- push 失败类 cause 原则性不计量(与热度无关)。

allocTime 来源:新 epoch 由分配登记;epoch 0 用 registerShuffle 时刻(偏保守,方向安全);未知的保守不升档。

#### 分配与回复(`ChangePartitionManager`)

- `desired` 截断于 `cap = maxLocations > 0 ? maxLocations : numMappers`(路由是 `mapId % activeCount`,超过 mapper 数的 location 必有空转,mapper 数是天然上限);
- `gap = max(0, cap 内 desired − 活跃数)`,逐次分配(epoch 递增、best-effort 不同 worker),candidates 耗尽即停;
- 登记用 **reserve 成功后的实际 epoch**(`reserveSlotsWithRetry` 失败重试会换 epoch,事前计划会与实际分叉,泄漏槽位);
- 活跃集中位于不可用 worker 的 epoch 被过滤并终态退休(死 worker 永远等不到退休上报);
- 全集回复:max epoch 为主 + 其余(含 soft)为 additionals;分配 0 个也回全集。
- 一条 Revive 可携带同 partition 的多个退休 epoch 条目:按 distinct partition 计数完成响应、同 partition 由 max-epoch 条目的首条回复生效(覆盖见 Test Plan · `RequestLocationCallContextSuite`)。

### 性能验证(生产个例)

以下数据来自一个代表性极端负载的生产作业:单 reduce partition 承接全部 26090 mapper(9.4TB shuffle 写),即本特性目标问题(单点聚合写压)的最严苛形态。结论适用于该类倾斜负载,不做跨负载外推:

| 指标 | 开启前 | 开启后 | 改善 |
|---|---|---|---|
| 写侧 stage 耗时 | 21m35s | 7m00s | 3.1×(数据量还大 59%) |
| per-task writeTime p50 / p90 | 27.5s / 110.8s | 615ms / 1.8s | 45× / 61× |
| total shuffle write time(全集群线程) | 607.5h | 6.0h | 101× |
| 每 GB 写线程耗时 | 361s/GB | 2.3s/GB | 157× |
| shuffle read 聚合吞吐 | 17.6GB/s | 26.3GB/s | +49%(fetchWait/GB 持平) |

## Compatibility, Deprecation, and Migration Plan

- **纯 additive,默认关闭**:`enabled=false` 时所有路径与现状等价;proto 仅新增一个 repeated 字段,无字段复用、无语义变更。
- **新 client → 老 LM**:拿不到 additionals,退化为单 location 写,无异常;
- **老 client → 新 LM**:忽略未知字段;LM 热点判定照常(老 client 的原生 revive 就是判定输入),只是老 client 不使用多 location;
- **Rollout**:先升级全部 LM(driver 侧随作业提交,与 executor 同包,天然同版本),再开启开关;LM 滚动升级期间新老 executor 混布即上述两条矩阵,均安全。**Rollback**:开关置回 false 即恢复单 location 写,已产生的多 location 文件读侧天然兼容(fileGroups Set + 串流 + 去重,读路径未改)。
- 无 deprecation;单 location 路径保留为默认。

## Test Plan

| 套件 | 例数 | 覆盖 |
|---|---|---|
| `PartitionLocationGroupSuiteJ` | 10 | 快路径、soft 参与路由/hard 排除、cause 升级、全集收敛与清理、乱序 epoch、并发 pick×merge、epoch 快照视图、outstandingRetires 视图 |
| `ReviveManagerSuiteJ` | 6 | 同步 revive:可写快速路径零 RPC、重试收敛且每轮携带全部退休上报、SUCCESS 无可写/RPC 失败有界放弃(3 次)、single-flight 并发去重(2 线程 1 RPC);批量路径:退休上报发送时从 outstandingRetires 现取(去重、丢弃陈旧 epoch) |
| `PartitionHotnessTrackerSuite` | 12 | 计量守卫(不可用 worker/push 失败)、K 因子缩放、fillTime 下限与 -1=mapper 数上限、显式上限优先、单调不降、soft 保留/移除、迟到 SOFT 不复活 |
| `ChangePartitionManagerAdaptiveParallelismSuite` | 10 | 升档+补差分配、超窗不升、allocTime 未知保守、首报去重、比例步进、epoch 乱序、gap=0 仍回全集、并发 revive 收敛、一条 Revive 的同 partition 多条目分组(上报只记账、max-epoch 驱动请求、commit 注册不丢) |
| `RequestLocationCallContextSuite` | 1 | 同 partition 重复回复忽略、按 distinct 数完成响应 |

回归:特性关闭时既有 client/LM 套件全绿;生产灰度作业(性能验证一节)开启前后对比。

## Rejected Alternatives

- **worker/client 侧速率统计(替代 split 事件驱动)**:client 侧单 mapper 只见自己的流,聚合速率只有 LM 能算,需要新上报通道;且 push 字节/时间含排队、网络、flush 周期,瞬时噪声大,需要平滑窗口,而平滑会重新引入本欲消除的检测延迟。worker 侧按 partition 速率仪表化 + 上报协议 + LM 聚合,补丁面从 1 个 additive proto 字段扩到 worker/protocol/LM 三层;Celeborn 目前没有任何 per-partition 吞吐度量可复用。无论哪侧,判定"热"仍需一个 SLO 阈值(聚合速率 > X),其量纲恰等于 阈值/hotWindow——本方案已内嵌同一 SLO,且零测量基础设施。净收益只有两条:检测延迟可低于"写满一个阈值";与 threshold 配置解耦。收益不足以抵消补丁面扩张,故列为 Future Work 的**信号源替换**(fillTime→目标换算、全集收敛、活跃集记账全部复用)。
- **PR#3260(CIP-20):split 计数 × 静态速率魔数**:用 split 事件频率乘假定字节数,再除以 `expectedWorkerSpeed=10MB/s` 推算所需并行度。10MB/s 对 NVMe 严重低估、对拥塞 HDD 严重高估,静态魔数在异构集群不可能正确——这是其社区评审的主要质疑,本方案的 fillTime 实测正是该质疑的直接答案。其 50ms 轮询收敛存在中间态路由分歧(决策 3)。协议上它把 `partition` 单值改 repeated,多元素对旧 client 有 merge 畸形风险;本方案只加 additive 的 `additionalPartitions`。
- **业务侧 salting / repartition**:把倾斜 key 打散到多个 reduce partition。有效但要求改作业、且读侧/下游语义变化;对"单 partition 承接全部 mapper"的极端场景(所需并行度超过集群 worker 数)仍是唯一根治手段。本特性与它互补:特性解决"检测与并行写"的系统侧自动化,salting 解决超出集群物理上限的倾斜(见 Risks and Limitations 第 3 条)。

## Risks and Limitations

1. **检测延迟 = 写满一个 threshold**:事件驱动的固有限制(默认 1G 阈值下,热 partition 约 60s 边界,越热越快);滞后期间等价于现状,不会更差。进一步降低延迟需换信号源(速率统计,成本收益见 Rejected Alternatives,列入 Future Work);
2. **desired 只升不降**:一次误判(如极短 fillTime)在整个 shuffle 生命周期不可回退,后果由上限封顶;epoch 0 判定偏保守(方向安全);
3. **split 事件率与并行度无关**:事件率 = 聚合吞吐 / split 阈值。所需并行度远超集群 worker 数的作业(如单 partition 承接全部 mapper)超出本特性能力范围,应业务侧 salting/repartition;
4. **worker 占用放大**:热点 partition 占 K 台 worker,K × 2 × partitionSplitMaximumSize 磁盘;replicate 模式 slot 翻倍;
5. **AQE skew read / StageEnd commit 变长**:理论兼容,上线前 IT 回归。

## Future Work

- worker/client 侧速率统计作为**信号源替换**(检测延迟降到 10~20s,与 split 阈值解耦;成本收益账见 Rejected Alternatives,fillTime→目标换算、全集收敛、活跃集记账全部复用);
- 并行度降档与热点消散回收;
- worker 过载主动上报(SOFT_SPLIT_OVERLOAD)。
