# Celeborn 动态分区写并行度 — 设计实现方案

> 基于 Uniffle partition split 思路改进,**不改造 Worker 侧代码**,在 Driver/Executor/协议层实现大分区的动态并行写,消除大分区频繁 split 导致的 shuffle write block。
>
> 约束:不加 worker 时间戳、不上报速率、不改 worker 检测逻辑。复用 Celeborn 现有 `SOFT_SPLIT`/`HARD_SPLIT` 信号 + 现有 revive 去重机制。开关默认关闭,向后兼容。

---

## 1. 背景与问题

### 1.1 本质问题:多 mapper 并发击穿 SOFT→HARD 升级窗口

Spark + Celeborn 架构(经 `client-spark/spark-3/.../SparkShuffleManager.java:81,88,158-167` 验证):
- **Driver 端**:1 个 `LifecycleManager`(shuffle 大脑,决策+状态)。
- **Executor 端**:每进程 1 个 `ShuffleClientImpl`(本地 `reducePartitionMap` 缓存、push data、发 revive)。
- **Worker**:多个,存储 partition 文件,判定 split。

多 mapper 并发写同一大分区时,单 location 的 fileLength 由 N 个 mapper 共同推高(涨速 ×N)。split 升级判定(`worker/.../PushDataHandler.scala:1463-1469`):

```
fileLength > splitThreshold(1G) 且 < partitionSplitMaximumSize(2G) → SOFT_SPLIT(软预警,L0 仍可写到 2G)
fileLength ≥ 2G,或 HARD 模式下 ≥ 1G → HARD_SPLIT(同步阻塞,block 所有 MapTask)
```

从 SOFT_SPLIT(1G)到 HARD_SPLIT(2G)的**黄金窗口宽度 = 1G / 写速**。写得快则窗口秒级,revive + 路由切换来不及 → 升 HARD → 全局 block shuffle write。

### 1.2 解决思路

1:N 把 N 个 mapper 分散到 P 个活跃 location(单 location 涨速 ÷ P),护住 SOFT→HARD 升级窗口。**并行度随分区负载动态调整**(revive 频率负反馈),小分区不浪费 slot,大分区够用。

---

## 2. 为什么"不动 Worker"可行

Celeborn worker **现有信号已等价于 Uniffle 的拆分触发**:
- worker `checkDiskFullAndSplit`(`PushDataHandler.scala:1440-1487`)按 fileLength 检测,超 `splitThreshold` 且 SOFT 模式 → 同步返回 `SOFT_SPLIT`(`:1463-1466,404-406`)。
- 等价于 Uniffle server `requireBuffer` 返 `needSplitPartitionIds`(`HugePartitionUtils.java:153-155`,按 `usedPartitionDataSize > splitLimit`)。
- **Celeborn 不需要改 worker 来"获得拆分信号"——SOFT_SPLIT/HARD_SPLIT 就是现成的**。

改造全在 Driver/Executor/协议层。

---

## 3. 三个关键概念(经代码验证,务必区分)

| 概念 | 代码位置 | 语义 |
|---|---|---|
| **SOFT_SPLIT** | `PushDataHandler.scala:1463-1466` | fileLength > 1G 且 < 2G(SOFT 模式)。**预警**,L0 仍可写到 2G,不阻塞。 |
| **HARD_SPLIT** | `PushDataHandler.scala:1468` | fileLength ≥ 2G(SOFT 模式)或 ≥ 1G(HARD 模式)。**真满,同步阻塞**。 |
| **partitionSplitMode** | `CelebornConf.scala:5380-5390` | `SOFT`(默认,1G→SOFT_SPLIT 可写到2G)/ `HARD`(1G→直接 HARD_SPLIT,无 SOFT_SPLIT)。 |
| **revive** | `ChangePartitionManager.scala:189-240` | mapper 收 SOFT/HARD_SPLIT 后主动发 RPC 给 Driver 要新 location。**同步 RPC,Driver 必须立即回复**。 |

→ 动态判据**不能依赖 SOFT_SPLIT**(HARD 模式无此事件),必须用**模式无关的统一信号 = revive 事件**。

---

## 4. 核心算法:动态调整并行度

### 4.1 统一信号:revive 事件(模式无关、场景无关)

无论 SOFT/HARD 模式,无论 sibling 是否均满,sibling 该换时**必然触发 revive**:
- SOFT 模式:sibling 达 1G → SOFT_SPLIT → revive(cause=SOFT_SPLIT)。
- HARD 模式:sibling 达 1G → HARD_SPLIT → revive(cause=HARD_SPLIT)。
- 不均(L0 已 HARD、L1 未满):L0 的 HARD_SPLIT → revive(cause=HARD_SPLIT)。

→ revive 是唯一对两种模式、不均场景都成立的统一信号。Driver 端 `handleRequestPartitionLocation` 收到的 revive cause 区分 SOFT_SPLIT/HARD_SPLIT,但**都计入频率**。

### 4.2 多 mapper 同 L 满了的去重(关键)

多 mapper 写同一 location L,L 满时各自收 split、各自发 revive 给 Driver——**N 个 revive 是同一事件(L 满)的重复上报**。若都计入频率,被放大 N 倍,判据失真。

**Driver 现有三层去重**(`handleRequestPartitionLocation :189-240`):
1. **`requests.containsKey(partitionId)`**(`:216-220`):已有 in-flight 请求 → 后续 revive 注册合并(`:219 add`),return。`replySuccess`(`:289-296`)统一回复。
2. **`getLatestPartition` epoch 短路**(`:222-231`):第一个 revive 已处理完、新 L' 已覆盖 `latestPartitionLocation` → 后续 revive(携 oldEpoch=L)被 `loc.getEpoch > epoch`(`:249`)短路,直接回复 L'。
3. **真正分配**(`:232-234`):只有第一个未短路者 `requests.put` → `handleRequestPartitions` 分配。

→ **同一 L 满了,N 个 revive 经三层去重,只产生 1 次"真正分配"**。

### 4.3 频率统计挂载点(决定性)

- **必须挂在"真正分配成功后"**(第三层,`handleRequestPartitions` 的 `replySuccess` 前),记 1 次时间戳。
- **绝不能挂 `handleRequestPartitionLocation` 入口**(含被短路的 N-1 个,会被放大 N 倍)。
- 如此:频率 = 该 partition **真实更换 location 的速率**,不受 mapper 数污染。

### 4.4 1:N 下的频率语义与无振荡

1:N 后,P 个 sibling 各自独立涨,各自满各自触发"真正分配"(各 sibling 是不同 epoch/location,其"真正分配"互不短路,各自独立计)。**频率 = 各 sibling 满的速率之和**。

- 写速恒定 V、每 sibling 涨速 V/P:单 sibling 满周期 = P×T_split,所有 sibling 轮流满 → 稳态频率 ≈ 1/T_split(与 P 无关)。
- **升 P→2P**:单 sibling 涨速 ÷2,满周期翻倍,频率降为 1/(2T_split)。**频率随 P 升真实下降 → 负反馈收敛,不振荡**。
- K(P) 随 P 缩放:大 P 本就有多 sibling 陆续满(基线频率高),阈值按 P 放大才公平,避免大 P 误升。

### 4.5 动态算法

Driver 为每 partition 维护:
- `activeSiblings`:活跃(未 excluded)sibling 集合。
- `excludedLocations`:满/坏的 location(写侧过滤,不物理移除,见 §6 取舍 A)。
- `reviveTimestamps`:该 partition 近期"真正分配"的时间戳队列(滑动窗口 T,限容)。
- `targetParallelism`:当前目标 P(=`activeSiblings.size`)。

```
revive 到达 handleRequestPartitionLocation(:189):
  复用现有三层去重:
    - in-flight 合并(:216-220) → 注册 context,return(不计频率)
    - epoch 短路(:222-231) → 回复已有 L',return(不计频率)
    - 真正分配(:232-234 → handleRequestPartitions):
       1. 记时间戳到 reviveTimestamps,淘汰窗口外。(只在此时计,不被 mapper 数放大)
       2. 处理本次:排除满/坏 sibling(excluded),补新 sibling(保持当前 P)。同步回复(不阻塞)。
       3. 判升 P:窗口内 revive 次数 ≥ K(P)(K(P)=reviveThresholdRatio × P)
          → P 不够 → 升 P=min(P*2, maxParallelism),补齐新 sibling,清空窗口(避免本次重复计)。
          → 受 cooldownMs 约束:升 P 后冷却期内不再升。
       4. 否则保持 P(本次只换了 sibling)。
```

- **首次升级**:P=1,首次 revive → 窗口 1 次,K(1)=1 → 升 P=2,补 L1。立即同步回复(不阻塞)。L0 涨速 ÷ 2(SOFT 模式护住 1G→2G 余量;HARD 模式 L0 已满被排除,L1 承接)。
- **步长**:倍增 P→2P(避免频繁小步 RPC)。
- **降级**:不做(MVP)。靠 `maxParallelism` 上限 + stage end 统一回收。
- **停止**:达 `maxParallelism`(默认 8)。
- **HARD 模式适配**:HARD 模式每次 sibling 满(1G)即 HARD_SPLIT→revive,cause=HARD_SPLIT 同样计数,判据一致。
- **不均适配**:L0 先 HARD、L1 未满 → L0 的"真正分配"计入频率;频率达 K(P) 升 P,否则只换 L0 保持 P。自然鲁棒。

### 4.6 时序示例(1G/2G,8G 大分区,SOFT 模式)

```
t0: P=1, {L0}
t1: L0 达1G → SOFT_SPLIT → revive → 真正分配 → 频率1次 ≥ K(1)=1 → 升P=2补L1,清窗口
    L0 涨速÷2,护住 1G→2G 窗口(不升 HARD)
t2: mapId hash 分流 L0/L1,各自涨
t3: L0 达1G(本轮)→ 真正分配 → 频率1次 < K(2)=2 → 不升,只换 L0 补 L2,保持P=2
t4: L1 达1G → 真正分配 → 频率2次 ≥ K(2)=2 → 升P=4补L2/L3,清窗口
t5: 4路分流,各 sibling 在 1G→2G 吃余量,不达2G(不升 HARD)
t6: 陆续达1G → 频率4次 ≥ K(4)=4 → 升P=8
t7: 8G 写完,各 sibling <2G,stage end commit 全部
```

### 4.7 与 Uniffle/CIP-20 对比

| 动态维度 | Uniffle | CIP-20 | **本方案** |
|---|---|---|---|
| 信号 | server 数据量阈值 | split 频率推算 pushSpeed | **revive 频率(模式无关)** |
| 模式适配 | — | — | **SOFT/HARD 通用** |
| 不均鲁棒 | — | — | **是(单 sibling HARD 也计数)** |
| 升级触发 | 不升级(一次拆到 N) | 速率超阈 | **窗口 revive 频率 ≥ K(P)** |
| 步长 | — | 速率线性 | **倍增 P→2P** |
| 降级 | — | — | **不做(MVP)** |
| 振荡 | 无(固定) | 可能 | **无(频率随 P 分流真实下降,负反馈)** |
| worker 改动 | 有(server 检测) | 有(上报 fileLength) | **无(revive 现成)** |

---

## 5. 四个命门(实现必须解决)

### 命门 1:协议(回复单 location → 多 location)
- `RequestLocationCallContext.reply`(`:30-36`):`partitionLocationOpt: Option[PartitionLocation]` → `Option[util.List[PartitionLocation]]`。
- `ChangeLocationsCallContext.newLocs`(`:44-46`):Value `Tuple3[.., PartitionLocation]` → `Tuple3[.., util.List[PartitionLocation]]`;`:57-60` put → 累加;`:58` 防重 → 集合内判重。
- `ChangeLocationResponse.newLocs`(`ControlMessages.scala:254-257`):Value → List;proto serde 改(additive,向后兼容)。
- Executor `reducePartitionMap`(`:118`):Value 单值 → List/Set;`reviveBatch`(`:949-976`):962-963 单值 put → addAll。

### 命门 2:回收(`removePrimaryPartitions` 粒度)— 取舍 A 不动 worker
- `ShufflePartitionLocationInfo.removePrimaryPartitions`(`:67-69,84`)按 partitionId 整体 remove → 1:N 下会误删其它活跃 sibling。
- **取舍 A(推荐,严格不动 worker)**:不动 `removePrimaryPartitions`。满 sibling 留活跃集合被标记 `excludedLocations`(写侧过滤),不物理移除;靠 `maxParallelism` 上限控制;stage end `tryFinalCommit` 统一 commit 全部(含满的)。与 Uniffle「追加不删除,excluded 过滤」(`MutableShuffleHandleInfo.java:251-254`)一致。
- 取舍 B(可选):改 `removePrimaryPartitions` 按 uniqueId 精细 remove(改已有方法粒度,非新增检测)。

### 命门 3:epoch 模型(单值判据失效)
- `getLatestPartition`(`ChangePartitionManager.scala:242-254`):epoch 短路 → 活跃集合判据("集合非空且达目标并行度则返回集合,否则补齐")。
- `updateLatestPartitionLocations`(`LifecycleManager.scala:153-159`):单值覆盖 → 追加 sibling。
- `getLatestLocs`(`:700-718`):取 max → 返回集合(AQE 适配)。
- `getAllPrimaryLocationsWithMaxEpoch`(`ShufflePartitionLocationInfo.scala:92-110`)**保留不动**(worker 侧读路径,取 max 给读侧),写侧用新增的"活跃集合"逻辑(Driver 端)。

### 命门 4:快速切换(失败无脑 revive)
- `submitRetryPushData`(`ShuffleClientImpl.java:370`):取单值 → 选未 excluded sibling。
- `onFailure`/HARD_SPLIT(`:1218-1258,1310-1362`):加快速路径——本地集合有未满 sibling → 直接切换重 push,不 revive。借鉴 Uniffle `tryNextServerForSplitPartition`(`TaskAttemptAssignment.java:91-103`)。
- `newerPartitionLocationExists`(`:846-867`):epoch 比对 → "有未 excluded sibling"判据。

---

## 6. 改动清单(取舍 A:零 worker 改动)

### 6.1 配置层 `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala`
新增 5 个配置项(仿照现有 `ConfigEntry` 模式,默认值保证向后兼容):

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `celeborn.client.shuffle.dynamicWriteParallelism.enabled` | false | 总开关 |
| `celeborn.client.shuffle.dynamicWriteParallelism.max` | 8 | 并行度上限 |
| `celeborn.client.shuffle.dynamicWriteParallelism.reviveWindowMs` | 30000 | revive 频率滑动窗口 |
| `celeborn.client.shuffle.dynamicWriteParallelism.reviveThresholdRatio` | 1.0 | K(P)=ratio×P,窗口内真正分配次数达此则升 P |
| `celeborn.client.shuffle.dynamicWriteParallelism.cooldownMs` | 5000 | 升 P 后冷却,避免瞬时连续升级 |

### 6.2 协议层(common,非 worker)
- `client/src/main/scala/org/apache/celeborn/client/RequestLocationCallContext.scala`:`reply`/`newLocs` Value → List。
- `common/src/main/scala/org/apache/celeborn/common/protocol/message/ControlMessages.scala`:`ChangeLocationResponse.newLocs` Value → List。
- `common/src/main/proto/TransportMessages.proto`:`ChangeLocationResponse` 对应 pb 改 Value 结构(additive)。

### 6.3 Driver 端(决策,动态算法核心)
- `client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala`
  - `latestPartitionLocation`(`:102`):单值 → 活跃 sibling 集合(+excluded)。
  - `updateLatestPartitionLocations`(`:153-159`):覆盖 → 追加。
  - `getLatestLocs`(`:700-718`):取 max → 返回集合(AQE)。
- `client/src/main/scala/org/apache/celeborn/client/ChangePartitionManager.scala`
  - 新增 per-partition 状态:`activeSiblings` + `excludedLocations` + `reviveTimestamps`(滑动窗口队列)。
  - `handleRequestPartitionLocation`(`:189-240`):真正分配后(`:232→handleRequestPartitions`)记时间戳 + 排除满 sibling + 补新 sibling(保持 P)→ 判频率 ≥ K(P) 则倍增升 P。**动态算法决策点**。
  - `getLatestPartition`(`:242-254`):epoch 短路 → 活跃集合判据。
  - `handleRequestPartitions`/`replySuccess`(`:256,278-296`):回复多 location。
  - `removeExpiredShuffle`(`:497-503`):清理 per-partition 状态。

### 6.4 Executor 端(路由+快速切换)
- `client/src/main/java/org/apache/celeborn/client/ShuffleClientImpl.java`
  - `reducePartitionMap`(`:118`):Value 单值 → List/Set(开关开启时)。
  - `reviveBatch`(`:949-976`):962-963 → addAll。
  - `:1068` 路由:`map.get` → `selectByMapId(hash(mapId) % size)` 确定性散列,保持 mapper 内 batch 顺序。
  - `submitRetryPushData`(`:370`)、`onFailure`/HARD_SPLIT(`:1218-1258,1310-1362`):加快速切换路径。
  - `newerPartitionLocationExists`(`:846-867`):→ 未 excluded sibling 判据。

### 6.5 Worker 端
- **零改动**(取舍 A)。读侧 `reducerFileGroupsMap`(`CommitHandler.scala:603-606`)已 Set,透明。

---

## 7. 实现顺序(增量、可验证)

1. **配置层**(§6.1):5 个配置项,独立可测。→ 验证:配置加载、默认值。
2. **协议层**(§6.2,命门1):多 location 回复 + serde。→ 验证:`ChangeLocationResponse` 多 location 序列化/反序列化单测。
3. **Driver 状态模型**(§6.3 前半,命门3):单值→集合、追加、活跃集合判据。→ 验证:`LifecycleManagerSuite` 多 sibling 追加/excluded。
4. **Driver 动态算法**(§6.3 后半):revive 频率负反馈决策。→ 验证:`ChangePartitionManagerSuite` LOAD_BALANCE 分配、频率升级、去重挂载点。
5. **Executor 路由**(§6.4 前半,命门4前半+命门1消费侧):reducePartitionMap→List、mapId 散列。→ 验证:路由确定性、batch 顺序。
6. **Executor 快速切换**(§6.4 后半,命门4后半):失败切 sibling 免 RPC。→ 验证:失败注入、快速切换不紧急 revive。
7. **端到端集成测试**(§8):倾斜分区 job、多 mapper 并发、HARD 模式、AQE/speculative。

---

## 8. 验证思路

### 8.1 单元测试(阶段 1,零 worker)
- `ChangeLocationResponse` 多 location serde。
- `ChangePartitionManagerSuite`:LOAD_BALANCE 分配、活跃集合判据、revive 频率升级、K(P) 阈值、cooldown、去重挂载点(多 mapper 同 L 只计 1 次)。
- `LifecycleManagerSuite`:多 sibling 追加/excluded、`getLatestLocs` 返回集合。

### 8.2 端到端(阶段 2)
1. **倾斜分区 job**(1 大 + 多小,SOFT 模式):大 partition SOFT_SPLIT → 并行度升 N;L0 涨速 ÷ N 护住窗口;**HARD_SPLIT 计数下降/归零**(核心指标);写时间下降;读侧 CRC 完整。
2. **多 mapper 并发压测**:N mapper 同时写大分区,验证 LOAD_BALANCE 分散、快速切换、去重正确(频率不被放大)。
3. **HARD 模式 job**:`partitionSplitMode=HARD`,验证无 SOFT_SPLIT 仍能动态升 P(revive cause=HARD_SPLIT 计数)。
4. **不均场景**:构造 L0 先满、L1 未满,验证只换 L0 保持 P(频率未达 K(P));持续则升 P。
5. **关闭开关**:回归单值模型与今天完全一致(向后兼容)。
6. **commit 正确性**:stage end 全 sibling commit(含满的 excluded),无残留。
7. **失败注入**:kill sibling,快速切换不紧急 revive;全满才 revive。
8. **AQE/speculative**:AQE 重算 stage 取集合正确;speculative (mapId,attemptId,batchId) 去重成立。

### 8.3 关注指标
- shuffle 写时间、单 partition 写吞吐、**SOFT/HARD_SPLIT 计数比**(核心,验证 block 消除)、revive 频率轨迹、并行度阶梯轨迹、activeSiblings/excluded 内存、CRC、replication slot。

### 8.4 测试复用
- Uniffle `PartitionSplitOfLoadBalanceModeTest.java` 端到端范式。
- Celeborn 现有 `ChangePartitionManagerSuite`/`LifecycleManagerSuite`/`CommitHandlerSuite` 扩展。

---

## 9. 风险与未决项

| 风险 | 说明 | 缓解 |
|---|---|---|
| **信号滞后(中)** | revive(1G/2G)才触发,非事前速率预判 | 首次升级即倍增 P=2 护住窗口;负反馈持续自适应。接受此滞后换零 worker 改动。 |
| **模式适配** | HARD 模式无 SOFT_SPLIT | revive 频率信号模式无关,cause=HARD_SPLIT 同样计数。已解决。 |
| **不均鲁棒** | L0 先 HARD、L1 未满 | L0"真正分配"计入频率;达 K(P) 升 P,否则只换 L0。已解决。 |
| **去重依赖现有机制** | 频率挂在"真正分配后" | 复用三层去重(`:216-234`);1:N 下各 sibling 不同 epoch,真正分配互不短路,频率语义正确。 |
| **振荡** | 1:N 后多 sibling split | 无。频率随 P 分流真实下降,K(P) 随 P 缩放,cooldown 防瞬时升级。 |
| **升级时延窗口** | 新 sibling 落地有时延 | 首次倍增 P=2;快速切换让已满 sibling 的 mapper 立即切走。 |
| **epoch 模型迁移** | 活跃集合判据替代 epoch 短路 | 取 max 保留给读侧;新增集合给写侧。需保证读侧/commit 正确。 |
| **满 sibling 不物理退出**(取舍A) | 留 excluded,等 commit | `maxParallelism` 上限控资源;stage end 统一 commit。 |
| **AQE `getLatestLocs`** | 改返回集合 | 需确认 Spark AQE reader 消费端兼容。 |
| **协议 additive** | proto 改 Value 结构 | 向后兼容(老 client/worker)。 |
| **HARD_SPLIT 时序** | sibling 各自满各自 HARD | 快速切换靠下次 push 的 HARD 响应(有滞后,免 RPC 缓解)。 |

---

## 10. 关键文件索引(全部已验证)

### 架构
- `client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/SparkShuffleManager.java:81,88,158-167`

### 命门 1 协议
- `client/src/main/scala/org/apache/celeborn/client/RequestLocationCallContext.scala:30-36,44-46,52-66,57-60`
- `common/src/main/scala/org/apache/celeborn/common/protocol/message/ControlMessages.scala:254-257,207-210`
- `client/src/main/java/org/apache/celeborn/client/ShuffleClientImpl.java:118,949-976,962-963`

### 命门 2 回收(worker 已有方法,取舍 A 不动)
- `common/src/main/scala/org/apache/celeborn/common/meta/ShufflePartitionLocationInfo.scala:67-69,84,92-110,112-124`
- `worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Controller.scala:598,782`
- `client/src/main/scala/org/apache/celeborn/client/commit/CommitHandler.scala:460-495,568-607,603-606`

### 命门 3 epoch 模型
- `client/src/main/scala/org/apache/celeborn/client/ChangePartitionManager.scala:242-254,189-240`
- `client/src/main/scala/org/apache/celeborn/client/LifecycleManager.scala:102,153-159,700-718`

### 命门 4 快速切换
- `client/src/main/java/org/apache/celeborn/client/ShuffleClientImpl.java:370,846-867,1068,1218-1258,1310-1362`

### 信号(不动 worker,现成)
- SOFT/HARD_SPLIT 现成检测:`PushDataHandler.scala:262-264,257,1440-1487,1463-1469`
- split 模式配置:`CelebornConf.scala:5380-5390`
- split 阈值:`CelebornConf.scala:5371`(threshold 1G)、`:3305`(max 2G)

### Uniffle 参考
- `~/Workspaces/uniffle-master/client-spark/common/.../handle/MutableShuffleHandleInfo.java:63,162-195,225-278,281-295`(多 server 集合+excluded+散列)
- `~/Workspaces/uniffle-master/client-spark/common/.../shuffle/ReassignExecutor.java:255-390`(快速切换)
- `~/Workspaces/uniffle-master/client-spark/common/.../writer/TaskAttemptAssignment.java:39-116,91-103`(tryNextServerForSplitPartition)
- `~/Workspaces/uniffle-master/server/.../HugePartitionUtils.java:153-155`(split 判定,等价 Celeborn SOFT_SPLIT)
- Uniffle 文档 `~/Workspaces/uniffle-master/docs/client_guide/spark_client_guide.md:172-194`

### CIP-20 参考
- `[CIP-20] Dynamically adjust partition write parallelism .txt`(第 24,28,53-64,82-94,103 行)
