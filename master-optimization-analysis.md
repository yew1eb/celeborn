# Celeborn Master 优化深度调研报告
## 对标 Kafka/etcd/TiKV 的工业实践

> 分析日期：2026-05-20  
> 核心问题：Celeborn HA Master 的 Raft 日志写入瓶颈 & Slot 分配算法优化

---

## 背景：Celeborn Master 的核心矛盾

RSS（Remote Shuffle Service）的 Master 承受的并发压力模型与传统存储系统有本质区别：

- **心跳密度极高**：1000 Worker × 10s 心跳间隔 = 100 req/s，每条携带全量磁盘信息
- **Shuffle 生命周期极短**：单个 Spark Job 内的 shuffle 从 register 到 unregister 通常在分钟级
- **Slot 分配规模极大**：单次 requestSlots 可能涉及百万级 partition，需要毫秒级响应
- **HA 必选**：生产环境不允许 Master 单点，但 Raft 写延迟与心跳频率直接冲突

---

## 第一部分：Raft 日志优化——工业实践对比

### 1.1 现状问题：Worker 心跳是最贵的 Raft 写操作

**Celeborn 当前路径**（`HAMasterMetaManager.java:303`）：

```
Worker 心跳 → handleWorkerHeartbeat()
  → 序列化全量磁盘信息（Protobuf）
  → ratisServer.submitRequest()
  → Ratis: propose → AppendEntries to followers → majority ACK → fsync → apply
  → updateWorkerHeartbeatMeta()（修改内存）
  → return
```

这条路径下，每一次 Worker 心跳都要经历完整的 Raft round-trip（写磁盘 + 网络 RTT × 2）。

### 1.2 Kafka KRaft 的 Epoch 分离机制

**KRaft（KIP-595/KIP-853）的核心洞察**：

并非所有 Broker 心跳携带的信息都需要强一致性。Kafka 将 BrokerHeartbeat 中的信息分为两类：

| 信息类型 | 内容 | 是否走 Raft |
|---------|------|------------|
| **结构性变更** | Broker epoch 变更、ISR 变更、上线/下线 | **走 Raft**，必须持久化 |
| **存活续约** | "我还活着"的时间戳刷新 | **不走 Raft**，仅 Leader 内存更新 |

KIP-853 进一步引入了 Controller Observer 节点，只读请求（获取 Broker 元数据）可路由到 Observer 处理，Leader Controller 专注写入。

**关键设计原则**：心跳超时检测依赖 Leader 内存状态即可——只有 Leader 在做超时检测（`timeoutDeadWorkers()`），Follower 不需要知道最新的 `lastHeartbeat`。

### 1.3 etcd 的 Lease Checkpoint 机制

etcd 面临类似问题：Lease KeepAlive 是高频操作，每次都走 Raft 代价很高。

**etcd 3.5+ 的解决方案（Lease Checkpoint）**：

- KeepAlive 调用在 Leader 内存中更新 Lease 的剩余 TTL
- **每隔 `lease-checkpoint-interval`（默认 5min）才将当前 TTL 写一次 Raft log**
- Leader 切换时，新 Leader 从最后一个 Checkpoint 重算剩余时间，而不是从完整 TTL 重置（避免切换后全量 key 过期风暴）

**从 etcd 借鉴的关键模式**：

```
高频更新（内存）→ 定期 Checkpoint（Raft）→ 节点切换时从 Checkpoint 恢复
```

### 1.4 TiKV 的 Batch Apply 与 Async Apply

TiKV 解决单 Raft Group 吞吐问题的两个核心机制：

**BatchRaftCmdRequest**：Leader 在 100ms 窗口内将多个客户端写请求聚合为一条 Raft Entry，apply 时顺序拆解执行。效果：将 N 次 fsync 降为 1 次。

**Async Apply**：Raft commit（多数派 ACK）和状态机 apply 解耦：
```
propose → [majority ACK] → 立即通知客户端 commit 成功
                         → 异步提交到 apply 线程池（不阻塞后续 propose）
```

**Celeborn 的直接对应**（`StateMachine.java:190`）：

```java
// 当前实现：单线程串行 apply
CompletableFuture.supplyAsync(() -> runCommand(request, trxLogIndex), executorService);
// executorService = newDaemonSingleThreadExecutor(...)
```

所有类型的操作（高频心跳 apply + 低频但敏感的 RequestSlots apply）共享同一个单线程队列，互相阻塞。

### 1.5 Ratis 内置优化：管道化传输

Celeborn 使用 Apache Ratis，其已有 Pipeline 机制，但依赖正确配置：

```java
// HARaftServer.java — 已有配置，控制批量大小
RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, logAppenderQueueNumElements);
RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(logAppenderQueueByteLimit));
```

Ratis gRPC 传输层使用 bidirectional streaming，避免每条 Entry 的连接建立开销。**当前配置的问题是：Raft 写 QPS 过高（Worker 心跳驱动），而不是单条消息太大**，批量化缓冲队列的效果有限。

### 1.6 Leader Local Read（Lease Read）

**三种读取语义的延迟对比**：

| 模式 | 延迟 | 一致性保证 |
|-----|------|---------|
| Linearizable Read | 1 RTT + apply 追赶 | 强一致 |
| Lease Read | ~0（验证本地时钟） | 依赖时钟单调性 |
| Follower/Stale Read | ~0（本地快照） | 弱一致（可能读到旧数据） |

Celeborn Master 的 `handleCheckWorkersAvailable`、slot 分配前的 `workersAvailable()` 等只读操作走了完整的 Raft 路径（通过 `executeWithLeaderChecker` 保证在 Leader 上执行，但实际读的是本地内存），理论上可以利用 Ratis 的 stale read 接口将非关键读绕过 Raft。

---

## 第二部分：Raft 日志优化——Celeborn 具体优化方案

### 方案 A：Worker 心跳分层（最高优先级）

**核心思路**：将心跳的"存活续约"与"状态变更"分离，只有状态真正变化时才写 Raft。

```
心跳到达 Leader
├── 提取变更 delta（磁盘状态变化、highWorkload 变化）
│   ├── delta 非空 → 写 Raft（与现在相同）
│   └── delta 为空 → 直接更新 Leader 内存的 lastHeartbeat，返回
└── 定期（如每 60s）将 lastHeartbeat 做一次 Checkpoint 写入 Raft
    （用于 Leader 切换后恢复存活状态，避免全量重超时）
```

**变更对比检测**（`AbstractMetaManager.updateWorkerHeartbeatMeta` 调用前做）：

```java
// 伪代码
WorkerInfo existing = workersMap.get(key);
boolean diskChanged = !isEqualDiskState(existing.diskInfos(), newDisks);
boolean workloadChanged = existing.isHighWorkLoad() != highWorkload;
boolean statusChanged = !existing.getWorkerStatus().equals(workerStatus);

if (!diskChanged && !workloadChanged && !statusChanged) {
    // 仅更新 lastHeartbeat，不走 Raft
    existing.lastHeartbeat_$eq(System.currentTimeMillis());
    return;
}
// 有变化才走 Raft
ratisServer.submitRequest(...);
```

**预期收益**：在负载稳定的集群中，绝大多数心跳（>95%）是无状态变化的续约，Raft 写 QPS 可降低 10-20x。

### 方案 B：App 心跳统计量本地化

**现状问题**（`HAMasterMetaManager.java:161`）：

App 心跳中的 `totalWritten`、`fileCount`、`shuffleCount` 等统计量也写 Raft，但这些数据：
- 在 `AbstractMetaManager` 中只是追加到 `LongAdder`（累计统计）
- 不影响任何调度决策
- HA 切换后会从 Checkpoint 恢复（计数器归零可接受）

**优化**：仅 `appHeartbeatTime`（超时检测依赖）写 Raft，统计量 Leader 本地维护。

### 方案 C：StateMachine Apply 按 Key 并行化

**现状**（`StateMachine.java:95`）：所有操作共享 1 个 apply 线程。

**改进**：引入 Striped Executor（按 Worker host hash 分片），不同 Worker 的心跳 apply 可并发：

```java
// 同一 Worker 的操作串行（保证 lastHeartbeat 单调递增）
// 不同 Worker 的操作并行（互不影响）
int stripe = Math.abs(host.hashCode()) % STRIPE_COUNT;
stripedExecutors[stripe].submit(() -> runCommand(request, trxLogIndex));
```

**限制**：RequestSlots、AppLost 等跨 Worker 操作仍需串行，需要在 dispatch 时做类型判断。

### 方案 D：Heartbeat 聚合（批量化写 Raft）

参考 TiKV 的 BatchRaftCmdRequest，在 Leader 侧做心跳聚合窗口（50-100ms），将同一窗口内多个 Worker 的心跳合并为一条 Raft Entry：

```
[Heartbeat-W1, Heartbeat-W2, ..., Heartbeat-W50] → 1 条 Raft Entry → 1 次 fsync
```

**与方案 A 的关系**：方案 A 减少写 Raft 的次数，方案 D 减少 fsync 次数，两者正交可叠加。

---

## 第三部分：Slot 分配算法——工业实践对比

### 3.1 Kafka Partition Assignment：Rack-Aware Interleaved Round-Robin

**Kafka 的 `assignReplicasToBrokers` 算法核心**：

给定 B 个 Broker、R 个副本因子、P 个分区：
1. 随机选择起始 Broker index `startIndex`
2. Leader 副本：`broker[(startIndex + i) % B]`（i = partition index）
3. Follower 副本：在 Leader 基础上偏移 `shift`（防止所有 partition 的副本分布完全对齐，增加 skew）
4. Rack-Aware 版本：将 Broker 按 Rack 重排为"交替列表"（interleaved），再对该列表做 round-robin

**与 Celeborn `generateRackAwareWorkers()` 的对比**（`SlotsAllocator.java:437`）：

Celeborn 的实现已经与 Kafka 思路完全一致：按 Rack 分组 → 按 Rack 大小倒序 → 交替取 Worker。两者差异在于：Kafka 是静态预计算一次分配方案，Celeborn 是每次 shuffle 独立计算，没有跨 shuffle 的均衡优化。

### 3.2 HDFS Block Placement：多轮软约束降级

**HDFS 放置策略演进的核心模式**：

```
第 1 轮：满足所有约束（机架感知 + 磁盘空间 + 节点健康）
第 2 轮：放宽磁盘空间限制（仍保证机架隔离）
第 3 轮：放宽机架约束（只保证节点健康）
第 4 轮：任意节点 fallback
```

**Celeborn `locateSlots()` 已有相同模式**（`SlotsAllocator.java:301`）：

```java
// 第 1 轮：满足 slotRestrictions + interruptionAware + rackAware
remain = roundRobin(slots, partitionIds, workersWithoutInterruptions, ...);
// 第 2 轮：放宽 interruptionAware
remain = roundRobin(slots, remain, primaryWorkerCandidates, workersWithEarlyInterruptions, null, ...);
// 第 3 轮：全量 workers，保留 rackAware
remain = roundRobin(slots, remain, workers, workers, null, shouldReplicate, shouldRackAware, ...);
// 第 4 轮：去掉 rackAware
roundRobin(slots, remain, workers, workers, null, shouldReplicate, false, ...);
```

架构上是正确的，但 **HDFS 有一个 Celeborn 缺少的关键机制**：**节点负载的实时感知在放置时已纳入 score**，HDFS DataNode 的 `remainingCapacityPercent` 直接影响节点选择权重。Celeborn 的 `offerSlotsLoadAware` 虽然考虑了磁盘负载，但其核心参数 `taskAllocationRatio` 是**静态初始化一次不再更新**的（`initialized = true` 后不变）。

### 3.3 YARN DRF（Dominant Resource Fairness）的启示

YARN DRF 的核心：每次调度选 **dominant share 最小**的请求（dominant resource = 已分配量/总量 最大的那个维度）。

对 Celeborn 的启示：**当前 `offerSlotsLoadAware` 用的是磁盘分组 + 比例分配，本质上是一个静态权重方案**。更好的做法参考 DRF：每次 partition 分配时，选 `score = max(activeSlotsRatio, flushTimeNorm, fetchTimeNorm)` 最小的磁盘，实现动态 dominant resource 优先的分配。

### 3.4 一致性哈希：Worker 扩缩容时的增量分配

**当前 Celeborn 的问题**：每个 shuffle 的 slot 分配完全独立，无状态，Worker 变化对已有 shuffle 无影响（shuffle 生命周期内 Worker 是固定的）。

但考虑 **ChangePartition**（Reducer 发现 Worker 故障时向 Master 申请新 partition 地址）场景：当前是随机重分配，没有考虑**机架亲和性保持**——原 Worker 和新 Worker 若在同一机架，网络代价更低。

**一致性哈希的适用场景**：`ChangePartition` 时，用一致性哈希在存活 Worker 中选择与原 Worker 最近的节点（同机架优先，同物理机次之），降低数据迁移的网络开销。

---

## 第四部分：综合优化路线图

### 优化优先级与收益矩阵

| 优化方向 | 参考系统 | 当前问题 | 收益估算 | 实现复杂度 |
|---------|---------|---------|---------|---------|
| **Worker 心跳分层（状态变更才写 Raft）** | Kafka KRaft Epoch 分离 | 每次心跳都写 Raft，1000 Worker = 100 Raft writes/s | Raft 写 QPS 降低 10-20x | 中 |
| **App 心跳统计量本地化** | etcd Lease Checkpoint | 统计量不影响调度但走 Raft | Raft 写 QPS 降低约 30% | 低 |
| **Slot 分配移出全局锁** | — | 分配期间阻塞所有心跳 | RequestSlots P99 降低 50%+ | 低 |
| **StateMachine Striped Executor** | TiKV Async Apply | 所有 apply 单线程串行 | Apply 吞吐提升 N 倍（N=条带数） | 中 |
| **心跳 Raft 写聚合（Batching）** | TiKV BatchRaftCmd | 每个心跳独立 fsync | fsync 次数降低 10-50x | 中 |
| **LoadAware 分配比例动态化** | HDFS 实时负载感知 | `taskAllocationRatio` 静态初始化 | 磁盘负载均衡更精准 | 低 |
| **DRF 式动态 Worker 评分** | YARN DRF | 分配权重静态分组 | 热点磁盘减少 | 中 |
| **ChangePartition 一致性哈希** | Cassandra vnodes | 故障重分配无机架亲和 | 降低 ChangePartition 网络开销 | 高 |

### 核心结论

Celeborn Master 优化的**第一性原理**是：

> **区分必须强一致的控制面操作（shuffle 生命周期、Worker 上下线）与只需 Leader 本地感知的监控状态（心跳 liveness、磁盘负载指标），把后者从 Raft 写路径中移除。**

这与 Kafka KRaft 将 "Broker Epoch 变更"和"心跳续约"分离、etcd 将"Lease Grant"和"KeepAlive TTL 更新"分离的哲学完全一致。

对于 Slot 分配，HDFS 和 Kafka 的经验表明：**多轮软约束降级（Celeborn 已有）+ 实时负载感知权重（待完善）+ 机架亲和故障恢复（待补充）** 是工业级分配算法的三要素。

---

## 附录：关键代码位置速查

| 优化点 | 文件 | 行号 |
|------|------|-----|
| Worker 心跳写 Raft 入口 | `HAMasterMetaManager.java` | L303-338 |
| App 心跳写 Raft 入口 | `HAMasterMetaManager.java` | L161-192 |
| StateMachine 单线程 apply | `StateMachine.java` | L95-97 |
| Slot 分配持全局锁 | `Master.scala` | L973-999 |
| Raft 管道化参数配置 | `HARaftServer.java` | L337-362 |
| 静态 taskAllocationRatio | `SlotsAllocator.java` | L59-61, L648-663 |
| Rack-Aware Worker 重排序 | `SlotsAllocator.java` | L437-466 |
| 四轮 fallback 分配 | `SlotsAllocator.java` | L301-422 |
| ResourceConsumption 本地化（正确做法参考） | `HAMasterMetaManager.java` | L333, L370 |
