# CIP: Adaptive Partition Write Parallelism(自适应分区写并行)

> Status: Draft — Jira: TODO(待申请 CELEBORN-XXXX)
> Implementation branch: `adaptive-parallelism-write`

## Motivation

Celeborn reduce partition 的写路径是**单活跃 location**:一个 partition 任一时刻只有一个活跃 PartitionLocation(某 worker 上的一个文件),所有 map task 的数据都写它,文件写满后换下一个。这把一个大分区的**全部聚合写压集中到单点**,在倾斜作业上产生两类症状:

- **SOFT→HARD 窗口塌缩**:N 个 mapper 并发写同一文件,涨速 = 聚合写速(×N);从 SOFT 阈值(默认 1G)到 HARD 上限(2G)的窗口 = 1G / 聚合写速。写得快则窗口只有亚秒级,revive + 路由切换来不及完成就升 HARD_SPLIT,写该 partition 的所有 map task 同步阻塞等新 location。
- **单点写瓶颈**:push RTT 升至秒级,per-worker in-flight 饱和,mapper 线程被 push 队列反压顶住。在一个单 reduce partition、26090 mapper 的生产倾斜作业上实测:per-task shuffle writeTime p50 = 27.5s 而 task 总时长 p50 仅 30.6s——90% 的 task 时间在等写。

本特性允许热点 partition **并行写多个活跃 location**:mapper 按 `mapId % activeCount` 散到各 location,由 LifecycleManager 根据实测写满速度自适应升档。N 个 location 并行写时单 location 涨速 ÷N:SOFT→HARD 窗口同比例拉宽、单 worker 写压 ÷N、某 location split 时其余仍可写,写路径不因切换停顿。生产效果见 Proposed Changes · 性能验证(生产个例):shuffle 写线程总耗时 431h → 4h12m,作业耗时 40m47s → 6m34s。

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
| `...adaptivePartitionWriteParallelism.minSplitInterval` | 60s | 单 location 两次 split 的最小间隔:写满 threshold 快于该间隔即判热,升档目标 = ceil(K × 间隔 / 写满耗时),稳态下单 location 的 split 间隔收敛到该值。间隔越大并行度越高、单点写压越薄,代价是热点 partition 的并发文件与磁盘占用按 K 倍放大(见 性能验证(生产个例)) |

**观测点**(均一次性,无重复刷屏):LM 侧升档判定 / 补差分配 / 分配不足(INFO/WARN);executor 侧并行激活 / SOFT 首报退休(含换路去向)/ 全不可用触发阻塞 revive 及其成功后的新目标(INFO);per-batch 重推成功(DEBUG)。

## Proposed Changes

### 数据流概览

**术语约定**(对齐 `docs/developers/lifecyclemanager.md` 的 "Revive/PartitionSplit" 词汇):本文的**退休(retire)**指 client 将某个 (partition, epoch) 标记为不再承接新写、并把 cause 随 Revive 上报 LM 的账本动作——即既有 Revive/PartitionSplit 流程中"旧 location 退出"的一侧;cause 为 SOFT_SPLIT/HARD_SPLIT 时对应 partition split 事件,也包括 push 失败与 worker 不可用。**revive** 沿用既有含义:client 向 LM 请求新 location 的 RPC。

```
mapper pushData(partitionId)
   └─ ShuffleClientImpl.pushOrMergeData
        └─ PartitionLocationGroup.currentFor(mapId) ← mapId % 可写数
             │   (可写 = 非退休 + SOFT_SPLIT;soft 文件在 2G 硬分裂前持续可写)
             ├─ 正常 → 现有 push/merge 路径(PushState 按 host 分桶,天然兼容)
             ├─ SOFT_SPLIT → retire(epoch, SOFT)(保持可写,不阻塞),首次退休上报 LM
             ├─ HARD_SPLIT / push 失败 → retire + 预置 SUCCESS:
             │     有另一可写 location 时重推线程立即换目标(不等 LM 响应)
             └─ 全部不可写 → 阻塞等待标准批量 revive(ReviveManager 统一入口,
                  预算 = push.revive.maxRetries):入队 max-epoch 请求并等待,
                  批调度器发送时附全部未消化退休上报,LM 消化后一轮补满活跃集;
                  每 partition 在飞请求由批组批天然去重,本地可满足时零 RPC

关键不变量:阻塞等待的完成谓词就是"该 partition 存在可写 location"——
reviveStatus 只作为本轮失败/超时信号(SUCCESS 是发出时刻的状态,不等于
可写,见决策 5);任何来源让 partition 可写都会唤醒等待线程。

LM (ChangePartitionManager → PartitionHotnessTracker):
  一条 Revive 先按 partition 分组:每组仅 max-epoch 条目走完整请求/分配路径,
    其余条目 = 纯退休上报,只做记账(commit 注册 + onEpochRetired)——
    批量 revive 可携带同 partition 的大量退休条目;逐条走完整路径时
    每条消息的处理量正比于条目数,分组后正比于 distinct partition 数
  收到带 cause 的 revive → 活跃集维护:SOFT_SPLIT 且 worker 可用 → epoch 保留;
    其余(HARD_SPLIT / push 失败 / worker 不可用)→ epoch 移出活跃集(终态,迟到 SOFT 不复活)
  热点判定(cause ∈ {SOFT, HARD} 且 worker 可用):
    fillTime = 首报时刻 - allocTime(epoch) < minSplitInterval ?
    desired = ceil(K × minSplitInterval / fillTime)   (K = 测量时活跃数;单调递增、封顶)
  补差分配 gap = desired - 活跃数(互不相同 worker、epoch 递增)
  revive 响应返回活跃 location 全集(max epoch 为主,其余为 additionals)
    → 所有 executor 收敛到同一 epoch 有序集合

读侧:不变(fileGroups Set + 多 location 串流 + (mapId, attemptId, batchId) 去重)
```

热点判定、活跃集记账、分配全部在 LM(driver);executor 只保留写路径与退休上报。

### 关键设计决策

1. **split 事件驱动 + fillTime 实测,不做速度假设**。不猜磁盘有多快,直接量:split 阈值是固定字节数(默认 1G),在任何磁盘上含义相同;一个 location 从分配到写满 1G 用了多久(fillTime),换算过来就是它的真实写速。HDD/SSD/NVMe/混插集群各自自校准,无需任何先验速度参数。一次 split 事件即可判定,per-epoch 独立对照。与速率统计等信号源的完整对比见 Rejected Alternatives。

2. **升档目标含 K 因子:`target = ceil(K × minSplitInterval / fillTime)`**。一句话:**目标路数 = 当前路数 × 需要放慢的倍数**。数字走一遍:minSplitInterval=60s(一个 location 至少写 60 秒才 split,切换不匆忙);实测当前 K=5 路并行、每路 10 秒写满 1G,即整体每 2 秒就写满一份——太赶。要让每路放慢到 60 秒才满,每路写速需降到 1/6,路数就要 ×6:`5 × 60/10 = 30`。为什么必须带 K:fillTime=10s 这个测量值本身已是 5 路分摊后的结果;公式若不带 K(60/10=6),目标只会从 5 升到 6——把已有的并行度忘了。收敛性质:一次判定直达目标;desired 单调递增、每 epoch 仅首报判定一次、上限截断,无需去抖。

3. **全集回复保证 executor 一致性**。每次 revive 响应携带该 partition 的完整活跃集(max epoch 为主回复 + `additionalPartitions`),按 epoch 有序插入,所有 executor 一次 revive 即收敛到**相同顺序**的活跃列表,`mapId % K` 分派全局一致。否则(PR#3260 的 50ms 轮询收敛)收敛期各 executor 路由不一致,同一 mapId 在不同 executor 写向不同 location。

4. **SOFT_SPLIT location 是一等路由目标,退休上报一条不丢**。两点直觉:(a) soft 文件在 2G 硬上限前持续可写——若把它排除出路由,稳态下所有槽位都处于 soft 态,写压将塌缩回最新的 1~2 个 location,并行写形同虚设;(b) LM 靠每条退休上报维护活跃集——丢一条,死 epoch 就撑大 LM 的活跃集,补差分配 gap 归零,executor 最终无可写 location 而失败。所以上报必须可靠:批量路径在**发送时**现取未消化退休集合,有界、自动去陈旧、超时丢失自动重发(实现见 Executor 侧实现)。

5. **全部不可写时的阻塞 revive:携带退休上报 + 有界重试 + single-flight,缺一不可**。人话版:与其让每个 pusher 线程各自直接去问 LM,不如复用标准批量 revive 通道排队等待——批调度器每 tick 每 partition 只发一条(single-flight 天然成立),发送时把整本"退休账"一起带上,尝试预算用既有的 `celeborn.client.push.revive.maxRetries`。为什么缺一不可:不带上报,LM 簿记里旧 location 还活着,补差 gap≈0,一轮只补 1 个 → 全部 mapper 挤向唯一可写 location → 秒级再次 HARD_SPLIT → 分配与退休互相加速的恶性循环;不去重,mass-retire 同时唤醒的线程会形成 RPC 洪峰压垮 LM(默认 60s `requestPartition.askTimeout`);不阻塞等待,一次调度超时就直接失败。两个更简的变体因此均不成立:纯异步 fallback(靠 worker 拒收重触发)在 split 密集期路由长期落空,吞吐显著退化;单发请求(不携上报)落入上述 gap≈0 陷阱。

### Executor 侧实现

#### PartitionLocationGroup(薄包装,懒膨胀)

`reducePartitionMap` 值类型改为 `PartitionLocationGroup`:未 split 的 partition 只是 `volatile single` + `null` 的 `ParallelState`(比原先多一个对象头),首次 split/失败/多 location 响应才 inflate 出 active 列表 + retired 表。5 万 partition 的 executor 增量内存 ≈ 1MB;ParallelState 仅热点 partition 存在。

- **路由**:`currentFor(mapId)` / `anotherUsableFor` 委托 `pick(mapId, excludeEpoch)`——快照 active 列表单遍收集可写子集(非退休 + soft),`floorMod(mapId, size)` 均匀分派;同一 map task 稳定写同一 location(保住 PushState 按 host 聚合)。
- **退休**:`retire(epoch, cause)` CHM compute 原子,返回是否首次(每 epoch 只上报一次);cause 可升级(SOFT→HARD)不可降级;与全集 merge 同 monitor,防墓碑写与清理交错。
- **全集收敛**:`mergeActiveLocations(locations, fullSet)` 按 epoch 有序插入、跳过本地已退休 epoch;`fullSet=true` 时清理 LM 已消化(全集中不再出现)的退休条目。仅携带 additionals 的响应才视为全集——单元素响应(老 LM / 冷 partition)不当全集,避免误清 soft-retired 条目。
- **诊断视图**:`activeEpochsSnapshot()`/`retiredEpochsSnapshot()` 供失败信息;`outstandingRetires()` 供退休上报(仅含仍在 active 列表中的退休 epoch,被全集清理的说明 LM 已消化)。同步 revive 与批量 revive 都在**发送时**从该视图现取未消化退休集合,而不是从队列收集:队列在调度器被超时堵住时积压无上界,group 视图则有界(≤ 活跃集大小)、自动去陈旧、RPC 超时丢失的条目自动随下一次重发。

#### ShuffleClientImpl 接入

为什么基线没有"全部不可写"这个状态:基线从不在本地退休 location——`reducePartitionMap` 里永远放着一个(可能已死的)location,写线程照写、靠 worker 拒收(HARD_SPLIT)驱动 revive,"location 已死"由 worker 间接告知。本特性把退休变成本地显式状态,入口才第一次看到 `currentFor(mapId) == null`:这不是新情况,是新信息——基线的同局面表现为"每个 batch 白跑一次数据往返再被拒"。既然本地已知必死(mass-retire 时等于全 mapper 风暴式白写),且退休上报本来就必须送达 LM,入口选择阻塞 revive 而不是照写。

| 路径 | 行为 |
|---|---|
| SOFT_SPLIT 回调 | `retire(epoch, SOFT)`(保持可写),首报且 mapper 未结束时上报;数据已落盘,零阻塞 |
| HARD_SPLIT / push 失败 | `retire` + 若有另一可写 location 则预置 `reviveStatus=SUCCESS`,重推线程立即换路不等 LM |
| 全部不可用 | 入口与重推路径统一走 `ReviveManager.reviveUntilWritable`:阻塞等待标准批量 revive(预算 = `push.revive.maxRetries`,重推路径传剩余预算),批调度器发送时携带全部未消化退休上报,LM 消化后一轮补满活跃集;不带上报的单条请求会让 gap 分配归零、回已退休 epoch(机制分析见决策 5)。等待的完成谓词是"可写"而非 reviveStatus——任何来源让 partition 可写都会提前唤醒;`reviveStatus == SUCCESS` 只代表 LM 已处理,不等于可写(簿记竞态),故每轮结束以 `currentFor(mapId)` 重查为准 |

#### 并发要点

Group 被四类线程并发访问(push 线程、push 回调、ReviveManager 调度器、经 ReviveManager 阻塞等待的 push/重推线程):读路径 COW+CHM 快照无锁;`retire`/`mergeActiveLocations`/`updateLatest` 同 group monitor;`retire` 首报信号靠 CHM compute 原子。阻塞等待复用批组批通道,每 partition 在飞请求由调度器天然去重(single-flight),等待线程被满足后重查可写性再返回。LM 侧同一 partition 的批处理由条纹锁 + `inBatchPartitions` 去重串行。

#### 正确性

- batchId per-mapTask 全局单调,读侧 (mapId, attemptId, batchId) 去重不依赖 batch 在文件内的顺序 → 并行写与重推重复 batch 均安全;
- soft 续写:SOFT_SPLIT 语义下 worker 持续接收该文件写直到 2G,`partitionSplitMaximumSize` 是硬上限;
- speculation / rerun / stageEnd 后重跑:既有路径不变。

### LM 侧实现

#### PartitionHotnessTracker(独立可单测)

per (shuffleId, partitionId) 的稀疏 HotState:`activeEpochs`(可写 epoch,soft 保留)、`hardRetiredEpochs`(终态,防迟到 SOFT 复活)、`allocTimeMs`、`splitReported`(首报去重)、`desired`(单调递增)。依赖(latestEpoch / workerAvailability / numMappers)以函数注入,时钟由调用方传入。

`onEpochRetired`(每个退休上报到达时):
- 活跃集维护:SOFT 且 worker 可用 → 保留;其余 → 移除(终态);
- 计量(SOFT/HARD 且 worker 可用):`fillTime = max(1ms, now − allocTime)`,若 < minSplitInterval 则 `desired = min(cap, ceil(K × minSplitInterval / fillTime))`,K = 报告时活跃数(soft 保留的已计入,被移除的补回 1);
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

作业形态:一个简单的 SparkSQL scan + shuffle + write 作业;单 reduce partition 承接全部 26090 mapper,即本特性目标问题(单点聚合写压)的最严苛形态。结论适用于该类倾斜负载,不做跨负载外推。

同一作业四组对照(输入 3.53TB,shuffle 写 9.37TB;作业耗时含读侧与调度噪声,仅供参照,写侧指标是主信号):

| 运行 | 写吞吐 (MB/s) | Shuffle 写线程总耗时 | Executor 运行总耗时 | vcore·h | 作业耗时 |
|---|---|---|---|---|---|
| 未开启 | 6.33 | 431h20m | 546.1h | 1128.7 | 40m47s |
| minSplitInterval=10s | 391.75 | 6h58m | 177.8h | 433.9 | 7m23s |
| minSplitInterval=30s | 552.66 | 4h56m | 170.9h | 504.6 | 8m08s |
| minSplitInterval=60s(默认) | 648.97 | 4h12m | 167.5h | 403.3 | 6m34s |

**minSplitInterval 怎么起作用**:它定义稳态均衡点——升档公式的不动点是"单 location 两次 split 的间隔 = minSplitInterval",即单 location 写速均衡在 threshold/间隔,10s/30s/60s 分别对应约 100/34/17 MB/s。聚合速率固定,间隔越大 → 目标并行度越高,收益来自三处:(1) SOFT→HARD 安全窗 = 1G/单路速率,恰等于该间隔——60s 均衡下几乎总能在 SOFT 态平滑退休,零重推零阻塞;10s 均衡下重推频繁,而重推是重复写,这是 Shuffle 写线程总耗时阶梯(6h58m → 4h56m → 4h12m)的主因;(2) 写压分摊到 shuffle 既有 worker 集合内的更多 location,push RTT 与排队下降;(3) 热点判定要求 fillTime < minSplitInterval,间隔越小,写得稍慢的 partition 越不触发升档,且 fillTime ≥ 间隔后升档冻结——小间隔目标低、封顶早。代价:热点 partition 的并发 location 变多,槽位与磁盘占用按 K 倍放大(K × 2 × partitionSplitMaximumSize),分摊在 shuffle 既有 worker 集合内(补差分配不新增 worker),并由 `maxLocations`/mapper 数上限封顶。

(注:基线写吞吐 6.33 MB/s 反映的是严重反压下 mapper 大部分时间阻塞,而非磁盘上限;30s 组 vcore·h 略高于 10s 组为集群调度波动。)

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
| `ReviveManagerSuiteJ` | 8 | 阻塞等待批量 revive:可写快速路径零 RPC、入队后由批组批满足且发送携带退休上报、预算耗尽/RPC 失败有界放弃、mapperEnded 放弃、并发等待者共享一批(1 RPC)、他源 merge 可写提前唤醒(完成谓词=可写);批量路径:退休上报发送时从 outstandingRetires 现取(去重、丢弃陈旧 epoch) |
| `PartitionHotnessTrackerSuite` | 12 | 计量守卫(不可用 worker/push 失败)、K 因子缩放、fillTime 下限与 -1=mapper 数上限、显式上限优先、单调不降、soft 保留/移除、迟到 SOFT 不复活 |
| `ChangePartitionManagerAdaptiveParallelismSuite` | 10 | 升档+补差分配、超窗不升、allocTime 未知保守、首报去重、比例步进、epoch 乱序、gap=0 仍回全集、并发 revive 收敛、一条 Revive 的同 partition 多条目分组(上报只记账、max-epoch 驱动请求、commit 注册不丢) |
| `RequestLocationCallContextSuite` | 1 | 同 partition 重复回复忽略、按 distinct 数完成响应 |

回归:特性关闭时既有 client/LM 套件全绿;生产灰度作业(性能验证一节)开启前后对比。

## Rejected Alternatives

- **worker/client 侧速率统计(替代 split 事件驱动)**:client 侧单 mapper 只见自己的流,聚合速率只有 LM 能算,需要新上报通道;且 push 字节/时间含排队、网络、flush 周期,瞬时噪声大,需要平滑窗口,而平滑会重新引入本欲消除的检测延迟。worker 侧按 partition 速率仪表化 + 上报协议 + LM 聚合,补丁面从 1 个 additive proto 字段扩到 worker/protocol/LM 三层;Celeborn 目前没有任何 per-partition 吞吐度量可复用。无论哪侧,判定"热"仍需一个 SLO 阈值(聚合速率 > X),其量纲恰等于 阈值/minSplitInterval——本方案已内嵌同一 SLO,且零测量基础设施。净收益只有两条:检测延迟可低于"写满一个阈值";与 threshold 配置解耦。收益不足以抵消补丁面扩张,故列为 Future Work 的**信号源替换**(fillTime→目标换算、全集收敛、活跃集记账全部复用)。
- **PR#3260(CIP-20):split 计数 × 静态速率魔数**:用 split 事件频率乘假定字节数,再除以 `expectedWorkerSpeed=10MB/s` 推算所需并行度。10MB/s 对 NVMe 严重低估、对拥塞 HDD 严重高估,静态魔数在异构集群不可能正确——这是其社区评审的主要质疑,本方案的 fillTime 实测正是该质疑的直接答案。其 50ms 轮询收敛存在中间态路由分歧(决策 3)。协议上它把 `partition` 单值改 repeated,多元素对旧 client 有 merge 畸形风险;本方案只加 additive 的 `additionalPartitions`。
- **业务侧 salting / repartition**:把倾斜 key 打散到多个 reduce partition。有效但要求改作业、且读侧/下游语义变化;对"单 partition 承接全部 mapper"的极端场景(所需并行度超过集群 worker 数)仍是唯一根治手段。本特性与它互补:特性解决"检测与并行写"的系统侧自动化,salting 解决超出集群物理上限的倾斜(见 Risks and Limitations 第 3 条)。

## Risks and Limitations

1. **检测延迟 = 写满一个 threshold**:事件驱动的固有限制——首次判定要等某个 location 写满一个 split 阈值,越热的 partition 触发越快;滞后期间等价于现状,不会更差。进一步降低延迟需换信号源(速率统计,成本收益见 Rejected Alternatives,列入 Future Work);
2. **desired 只升不降**:一次误判(如极短 fillTime)在整个 shuffle 生命周期不可回退,后果由上限封顶;epoch 0 判定偏保守(方向安全);
3. **split 事件率与并行度无关**:事件率 = 聚合吞吐 / split 阈值。所需并行度远超集群 worker 数的作业(如单 partition 承接全部 mapper)超出本特性能力范围,应业务侧 salting/repartition;
4. **资源占用放大**:热点 partition 的并发 location 为 K 个——槽位与磁盘占用按 K 倍放大(K ×(replicate 则 ×2)× partitionSplitMaximumSize),分摊在该 shuffle **既有** worker 集合内:补差分配的候选集来自该 shuffle 已占用的 worker(`workerSnapshots`),不新增 worker,集合大小本身由 `celeborn.client.slot.assign.maxWorkers`(默认 10000)约束;文件数增多也使 commit 体量与读侧文件流相应增加;
5. **AQE skew read / StageEnd commit 变长**:理论兼容,上线前 IT 回归。

## Future Work

- worker/client 侧速率统计作为**信号源替换**(检测延迟降到 10~20s,与 split 阈值解耦;成本收益账见 Rejected Alternatives,fillTime→目标换算、全集收敛、活跃集记账全部复用);
- 并行度降档与热点消散回收;
- worker 过载主动上报(SOFT_SPLIT_OVERLOAD)。
