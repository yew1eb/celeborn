# Celeborn CongestionControl 功能深度调研报告

## Context

本报告对 Apache Celeborn（得物内部版 `celeborn-dewu`）的**拥塞控制（Congestion Control）**功能进行全面深入的分析，涵盖设计目标、架构设计、核心算法、配置参数、客户端集成、监控指标以及演进历史。

目标：产出一份可直接归档的技术 Markdown 文档，供工程师阅读参考。

---

## 一、设计目标与背景

### 1.1 流量控制的整体架构

Celeborn Worker 的流量控制由两层机制组成：

| 机制 | 类型 | 能否禁用 | 作用层面 |
|------|------|----------|----------|
| **Back Pressure（背压）** | 被动触发，基于直接内存用量比 | 不能禁用 | 全局暂停 |
| **Congestion Control（拥塞控制）** | 主动抑制，基于速率和水位线 | 可选开启 | 用户级别差异化限速 |

### 1.2 设计目标

> **防止 Worker OOM，同时不牺牲性能；在保证性能的前提下实现公平性。**

- **Back Pressure** 是最后防线：当直接内存占用达到高阈值时，暂停接收数据并强制刷盘释放内存。
- **Congestion Control** 是主动预防机制：在内存压力出现时，识别并抑制消耗资源最多的用户，让低速用户不受影响，实现资源公平分配。

### 1.3 Back Pressure 三水位线（始终启用）

```
直接内存占用率
  0.95 ─── Pause Replicate：暂停接收 ShuffleClient 数据 + 主 Worker 副本数据，强制刷盘
  0.85 ─── Pause Receive：暂停接收 ShuffleClient 数据，强制刷盘
  0.70 ─── Resume：恢复接收（任一 Pause 触发后，需降至此阈值才恢复）
  0.00 ─── 正常
```

配置项：`celeborn.worker.directMemoryRatio.*`

---

## 二、Congestion Control 核心架构

### 2.1 代码目录结构

```
worker/src/main/java/.../worker/congestcontrol/
├── CongestionController.java        # 主控制器（单例，362行）
├── UserCongestionControlContext.java # 单用户拥塞状态上下文（88行）
├── BufferStatusHub.java              # 时间窗口字节统计（92行）
├── TimeSlidingHub.java               # 通用时间滑动窗口（165行）
└── UserBufferInfo.java               # 单用户缓冲区信息（42行）

worker/src/test/java/.../worker/congestcontrol/
├── TestCongestionController.java     # 主要测试
└── TestTimeSlidingHub.java           # 滑动窗口测试

common/src/main/java/.../write/
└── SlowStartPushStrategy.java        # 客户端慢启动推送策略（181行）
```

### 2.2 核心组件关系图

```
Worker
  └─ CongestionController (singleton)
       ├─ producedBufferStatusHub      ← 全局生产速率（所有用户汇总）
       ├─ consumedBufferStatusHub      ← 全局消费速率（数据刷盘）
       ├─ overHighWatermark: AtomicBoolean
       ├─ workerTrafficQuota           ← Worker 级水位线（来自 CelebornConf/动态配置）
       ├─ defaultUserQuota             ← 默认用户级水位线
       │
       ├─ userBufferStatuses: Map<UserIdentifier, UserBufferInfo>
       │       └─ UserBufferInfo
       │            ├─ timestamp       ← 最后活跃时间
       │            └─ bufferStatusHub ← 该用户的生产速率时间窗口
       │
       └─ userCongestionContextMap: Map<UserIdentifier, UserCongestionControlContext>
               └─ UserCongestionControlContext
                    ├─ congestionControlFlag: volatile boolean
                    ├─ userTrafficQuota       ← 该用户的个人水位线（支持动态配置）
                    ├─ userBufferInfo         ← 指向 userBufferStatuses 中的条目
                    └─ workerBufferStatusHub  ← 引用全局 producedBufferStatusHub
```

### 2.3 数据流向

```
ShuffleClient → PushDataHandler
                    │
                    ├─ fileWriter.getUserCongestionControlContext()
                    │        └─ UserCongestionControlContext
                    │
                    ├─ context.updateProduceBytes(numBytes)
                    │        ├─ 更新 UserBufferInfo（用户维度速率）
                    │        └─ 更新 producedBufferStatusHub（全局生产速率）
                    │
                    └─ isUserCongested(context)
                             └─ 返回 true → 响应状态码 PUSH_DATA_SUCCESS_PRIMARY_CONGESTED
                                           或 PUSH_DATA_SUCCESS_REPLICA_CONGESTED
```

---

## 三、核心算法详解

### 3.1 拥塞判断算法（CongestionController.isUserCongested）

```java
public boolean isUserCongested(UserCongestionControlContext ctx) {
    // 没有任何活跃用户，直接放行
    if (userBufferStatuses.isEmpty()) return false;

    long userProduceSpeed = getUserProduceSpeed(ctx.getUserBufferInfo());
    UserTrafficQuota quota = ctx.getUserTrafficQuota();

    // ① 全局超过高水位线时：抑制生产速率超过"平均潜在速率"的用户
    if (overHighWatermark.get()) {
        long avgSpeed = getPotentialProduceSpeed();  // 全局生产总速率 / 活跃用户数
        if (userProduceSpeed > avgSpeed) {
            return true;  // 直接返回拥塞，不更新 congestionControlFlag
        }
    }

    // ② 用户级水位线检查：独立于全局状态
    if (userProduceSpeed > quota.userProduceSpeedHighWatermark()) {
        ctx.onCongestionControl();   // 设置 congestionControlFlag = true
    } else if (ctx.inCongestionControl()
        && userProduceSpeed < quota.userProduceSpeedLowWatermark()) {
        ctx.offCongestionControl();  // 设置 congestionControlFlag = false（迟滞恢复）
    }

    return ctx.inCongestionControl();
}
```

**迟滞（Hysteresis）特性**：进入拥塞用高水位，退出拥塞用低水位，防止频繁抖动。

**两类触发路径**：

| 触发路径 | 条件 | 特点 |
|----------|------|------|
| 全局路径 | `overHighWatermark=true` 且用户速率 > 全局平均速率 | 即时生效，不修改 `congestionControlFlag` |
| 用户路径 | 用户速率超出用户级高水位线 | 有迟滞，修改 `congestionControlFlag` |

### 3.2 全局水位线检查（定时任务，默认每 10ms）

```java
protected void checkCongestion() {
    long pendingConsume = getTotalPendingBytes();         // MemoryManager.getMemoryUsage()
    long workerProduceSpeed = producedBufferStatusHub.avgBytesPerSec();

    // 同时低于低水位线 → 退出拥塞
    if (pendingConsume < workerTrafficQuota.diskBufferLowWatermark()
     && workerProduceSpeed < workerTrafficQuota.workerProduceSpeedLowWatermark()) {
        overHighWatermark.compareAndSet(true, false);
        return;
    }

    // 任一超过高水位线 → 进入拥塞，触发内存 trim
    if ((pendingConsume > workerTrafficQuota.diskBufferHighWatermark()
      || workerProduceSpeed > workerTrafficQuota.workerProduceSpeedHighWatermark())
     && overHighWatermark.compareAndSet(false, true)) {
        // log
    }

    if (overHighWatermark.get()) {
        trimMemoryUsage();   // MemoryManager.trimAllListeners()
    }
}
```

**注意**：退出拥塞需要**两个条件同时满足**（AND），进入拥塞只需**一个条件满足**（OR），这是非对称设计，确保保守退出。

### 3.3 平均潜在速率计算

```java
// 全局生产总速率 / 活跃用户数 = 每用户"公平份额"
public long getPotentialProduceSpeed() {
    if (userBufferStatuses.size() == 0) return 0;
    return producedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
}

// 全局消费总速率 / 活跃用户数（用于 Gauge 指标上报）
public long getPotentialConsumeSpeed() {
    if (userBufferStatuses.size() == 0) return 0;
    return consumedBufferStatusHub.avgBytesPerSec() / userBufferStatuses.size();
}
```

### 3.4 时间滑动窗口（TimeSlidingHub / BufferStatusHub）

**结构**：
- 内部维护 `LinkedBlockingDeque<Pair<Long, N>>`（时间戳 → 节点）
- 每个节点代表 **1 秒**的聚合数据（`intervalPerBucketInMills = 1000ms`）
- 窗口大小默认 10 秒，最多保留 10 个桶

**avgBytesPerSec 计算**：
```java
// totalBytes * 1000 / (bucketCount * 1000ms)
// = totalBytes / bucketCount    (bytes/sec)
return currentNumBytes * 1000 / ((long) sumInfo.getRight() * intervalPerBucketInMills);
```

**线程安全**：`sum()` 和 `add()` 均为 `synchronized` 方法；字节计数使用 `LongAdder`。

---

## 四、客户端慢启动策略（SlowStartPushStrategy）

客户端接收到拥塞控制信号后，通过类 TCP 的拥塞控制算法调整推送速率。

### 4.1 状态机

```
                     ┌─────────────────────────────┐
                     │         慢启动阶段            │
                     │  currentMaxReqsInFlight < 阈值│
                     │  每次成功 +1（指数增长，每RTT翻倍）│
                     └──────────┬──────────────────┘
                                │ 达到 reqsInFlightBlockThreshold
                                ▼
                     ┌─────────────────────────────┐
                     │        拥塞避免阶段           │
                     │  每 RTT +1（线性增长）         │
                     └──────────┬──────────────────┘
                                │ 收到拥塞控制信号
                                ▼
                     ┌─────────────────────────────┐
                     │          拥塞响应             │
                     │  currentMaxReqsInFlight /= 2 │
                     │  重设阈值为当前值              │
                     │  若已为1，continueCongestedNum++ │
                     └──────────┬──────────────────┘
                                │ 恢复推送
                                ▼
                           回到慢启动阶段
```

### 4.2 Sleep 退避策略

```java
protected long getSleepTime(CongestControlContext context) {
    int currentMaxReqs = context.getCurrentMaxReqsInFlight();

    // 已达最大并发，无需等待
    if (currentMaxReqs >= maxInFlightPerWorker) return 0;

    long sleepInterval = initialSleepMills - 60L * currentMaxReqs;

    // 当 currentMaxReqs = 1（极度拥塞）时，每次持续拥塞额外加 1 秒
    if (currentMaxReqs == 1) {
        return Math.min(sleepInterval + continueCongestedNumber * 1000L, maxSleepMills);
    }

    return Math.max(sleepInterval, 0);
}
```

默认参数：
- `initialSleepMills`：从 `celeborn.client.push.slowStart.initialSleepTime` 读取
- `maxSleepMills`：从 `celeborn.client.push.slowStart.maxSleepTime` 读取
- `maxInFlightPerWorker`：从 `celeborn.client.push.maxReqsInFlight` 读取

---

## 五、配置参数全览

### 5.1 Worker 侧配置

| 配置项 | 默认值 | 说明 | 引入版本 |
|--------|--------|------|----------|
| `celeborn.worker.congestionControl.enabled` | `false` | 是否启用拥塞控制 | 0.3.0 |
| `celeborn.worker.congestionControl.check.interval` | `10ms` | 全局水位线检查间隔 | 0.3.2 |
| `celeborn.worker.congestionControl.sample.time.window` | `10s` | 速率统计时间窗口 | 0.3.0 |
| `celeborn.worker.congestionControl.diskBuffer.high.watermark` | `Long.MAX_VALUE` | 磁盘缓冲区高水位（Bytes） | 0.3.0 |
| `celeborn.worker.congestionControl.diskBuffer.low.watermark` | `Long.MAX_VALUE` | 磁盘缓冲区低水位（Bytes） | 0.3.0 |
| `celeborn.worker.congestionControl.userProduceSpeed.high.watermark` | `Long.MAX_VALUE` | 用户生产速率高水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.userProduceSpeed.low.watermark` | `Long.MAX_VALUE` | 用户生产速率低水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.workerProduceSpeed.high.watermark` | `Long.MAX_VALUE` | Worker 生产速率高水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.workerProduceSpeed.low.watermark` | `Long.MAX_VALUE` | Worker 生产速率低水位（Bytes/sec） | 0.6.0 |
| `celeborn.worker.congestionControl.user.inactive.interval` | `10min` | 用户不活跃超时（超时后清理） | 0.3.0 |

> **注意**：所有水位线默认值为 `Long.MAX_VALUE`，即默认不触发对应的拥塞条件。至少需要配置磁盘缓冲水位线才能使拥塞控制生效。

**废弃配置映射**（0.3.0 → 0.6.0 重命名）：

| 旧配置（已废弃） | 新配置 |
|-----------------|--------|
| `celeborn.worker.congestionControl.low.watermark` | `diskBuffer.low.watermark` |
| `celeborn.worker.congestionControl.high.watermark` | `diskBuffer.high.watermark` |

### 5.2 客户端配置

| 配置项 | 说明 |
|--------|------|
| `celeborn.client.push.limit.strategy` | 推送策略：`SIMPLE`（默认）或 `SLOWSTART` |
| `celeborn.client.push.maxReqsInFlight` | 单 Worker 最大并发请求数 |
| `celeborn.client.push.slowStart.initialSleepTime` | 慢启动初始睡眠时间 |
| `celeborn.client.push.slowStart.maxSleepTime` | 最大睡眠时间上限 |

### 5.3 动态配置支持

Worker 级配置（`workerTrafficQuota`）和用户级配置（`userTrafficQuota`）均支持通过 `ConfigService` 动态更新，无需重启：

```java
// 监听配置变化
configService.registerListenerOnConfigUpdate(this::updateQuota);

// updateQuota 实现
private void updateQuota() {
    workerTrafficQuota = configService.getSystemConfigFromCache().getWorkerTrafficQuota();
    for (UserCongestionControlContext ctx : userCongestionContextMap.values()) {
        ctx.updateUserTrafficQuota(
            configService.getTenantUserConfigFromCache(user.tenantId(), user.name())
                .getUserTrafficQuota());
    }
}
```

**配置优先级（高→低）**：租户用户级 → 租户级 → 系统级 → 静态配置

---

## 六、监控指标

### 6.1 Worker 侧 Metrics（WorkerSource）

| Metric 名称 | 类型 | 含义 |
|------------|------|------|
| `PotentialConsumeSpeed` | Gauge | 全局消费速率 / 活跃用户数（bytes/sec） |
| `WorkerConsumeSpeed` | Gauge | Worker 全局消费速率（bytes/sec） |
| `UserProduceSpeed` | Gauge（带用户标签） | 各用户的生产速率（bytes/sec） |
| `IsHighWorkload` | Gauge（0/1） | Worker 是否处于高负载状态 |
| `PausePushDataStatus` | Gauge | 是否处于 Pause Receive 状态 |
| `PausePushDataAndReplicateStatus` | Gauge | 是否处于 Pause Replicate 状态 |

### 6.2 IsHighWorkload 判断逻辑（CELEBORN-2118，2025-08 引入）

```scala
// Worker.scala
highWorkload = (
    CongestionController.instance().isOverHighWatermark()  ||
    memoryManager.servingState == PUSH_AND_REPLICATE_PAUSED ||
    memoryManager.servingState == PUSH_PAUSED               ||
    activeConnectionCount >= activeConnectionMax
)
```

该指标暴露为 Prometheus 指标 `metrics_IsHighWorkload_Value`，用于 Grafana 大盘展示。当 Master 感知到 Worker `IsHighWorkload=1` 时（CELEBORN-2066），在排除 Worker 数量超限时优先排除高负载 Worker。

### 6.3 状态码（PushDataHandler 返回给客户端）

| 状态码 | 值 | 含义 |
|--------|----|------|
| `PUSH_DATA_SUCCESS_PRIMARY_CONGESTED` | 31 | 数据写入成功，但主节点被拥塞控制 |
| `PUSH_DATA_SUCCESS_REPLICA_CONGESTED` | 32 | 数据写入成功，但副本被拥塞控制 |
| `SOFT_SPLIT` | 13 | 触发软分裂 |
| `HARD_SPLIT` | 11 | 触发硬分裂 |

客户端收到 `PRIMARY_CONGESTED` 或 `REPLICA_CONGESTED` 后，调用 `SlowStartPushStrategy.onCongestControl()` 执行速率回退。

---

## 七、用户生命周期管理

### 7.1 用户创建

首次推送数据时，`getUserBuffer()` 和 `getUserCongestionContext()` 通过 `ConcurrentHashMap.computeIfAbsent()` 懒创建用户条目，并注册 `UserProduceSpeed` Gauge 指标。

### 7.2 用户清理（防内存泄漏）

```java
// 定时任务，间隔 = userInactiveTimeMills（默认 10min）
private void removeInactiveUsers() {
    long now = System.currentTimeMillis();
    for (UserBufferInfo info : userBufferStatuses) {
        if (now - info.getTimestamp() >= userInactiveTimeMills) {
            userBufferStatuses.remove(user);
            userCongestionContextMap.remove(user);
            workerSource.removeGauge(USER_PRODUCE_SPEED, user.toMap());  // 清理 Metric
        }
    }
}
```

### 7.3 状态流转

```
用户首次 Push
    └─→ 创建 UserBufferInfo + UserCongestionControlContext
              │
              ▼
    每次 Push 调用 updateProduceBytes()
              │
              ├─ 速率 > 高水位 → onCongestionControl() → flag=true
              ├─ 全局超水位 + 速率超均值 → 直接返回 true（不修改 flag）
              └─ 速率 < 低水位 + flag=true → offCongestionControl() → flag=false
              │
    超过 userInactiveTimeMills 无 Push
              └─→ removeInactiveUsers() 清理条目 + Metric
```

---

## 八、与 Master 的联动

### 8.1 Worker 黑名单（CELEBORN-1782）

当 Worker 进入拥塞控制（`overHighWatermark=true`）时，该 Worker 会被加入 Master 的 blacklist，避免新 Shuffle 任务继续分配到该 Worker，从根本上减少压力。

### 8.2 高负载 Worker 排除策略（CELEBORN-2066）

Master 在需要排除 Worker 时，优先排除 `IsHighWorkload=1` 的 Worker，以保护整体集群稳定性。

---

## 九、关键 Issue 与演进历史

| Issue | Commit | 描述 |
|-------|--------|------|
| CELEBORN-61/62 | `c924a4ff0` | 客户端慢启动、拥塞避免、拥塞控制的首次实现 |
| CELEBORN-63 | `bb5a4d218` | 新增 CONGESTED 相关状态码 |
| CELEBORN-207 | `f88f5fcf5` | 支持网络拥塞控制；修复开启副本时 Master 可能漏掉拥塞状态的问题 |
| CELEBORN-227 | `bff6e91e0` | 支持多种 Push 策略（SIMPLE/SLOWSTART） |
| CELEBORN-342 | `798ff90bb` | 修复拥塞控制中平均生产字节数计算错误 |
| CELEBORN-524 | `181c1bfcd` | 修复 CongestionControl 过于频繁调用 ChannelsLimiter.onTrim 导致 CPU 卡顿问题 |
| CELEBORN-777 | `52dcd3b5d` | 修复 getPotentialConsumeSpeed 除零错误 |
| CELEBORN-1089 | `320714bf2` | 将 overHighWatermark 检查抽离为独立定时线程 |
| CELEBORN-1472 | `d362d9f75` | 减少 userBufferStatuses 调用次数，降低锁竞争 |
| CELEBORN-1487 Phase1 | `1d44e5fbf` | 新增用户级和 Worker 级生产速率水位线控制 |
| CELEBORN-1487 Phase2 | `7c9a008a1` | CongestionController 支持动态配置更新 |
| CELEBORN-1654 | `fdff49453` | 修复 TestCongestionController#testUserMetrics 偶发失败 |
| CELEBORN-1782 | `cec88b2de` | 拥塞中的 Worker 加入黑名单 |
| CELEBORN-1794 | `406ceb64c` | 修复 TestCongestionController 偶发失败 |
| CELEBORN-1930 | `15ea5d366` | 修复 HARD_SPLIT 在 PushMergedData 中的拥塞控制 NPE 问题 |
| CELEBORN-2066 | `8effb753f` | 排除 Worker 时优先排除高负载 Worker |
| CELEBORN-2082 | `92edae22d` | 新增高负载 Worker 排除日志 |
| CELEBORN-2118 | `a9490d6e2` | 引入 `IsHighWorkload` 指标，统一监控 Worker 过载状态 |

---

## 十、测试覆盖

### TestCongestionController.java 核心测试场景

| 测试方法 | 验证内容 |
|----------|----------|
| `testSingleUser` | 单用户：pending bytes 超阈值时触发拥塞，低于低水位时退出 |
| `testMultipleUsers` | 多用户公平：高速用户被抑制，低速用户不受影响 |
| `testUserMetrics` | 用户 Gauge 指标的创建与清理生命周期 |
| `testUserLevelTrafficQuota` | 用户级独立水位线生效（与全局水位线解耦） |
| `testWorkerLevelTrafficQuota` | Worker 级生产速率水位线触发全局拥塞 |
| `testDynamicConfiguration` | 动态更新配置后拥塞行为变化 |
| `testUpdateProduceBytes` | 字节数更新同步写入用户级和全局 BufferStatusHub |

### 典型配置（测试中使用）

```java
diskBuffer.high.watermark  = 1000 bytes
diskBuffer.low.watermark   = 500  bytes
userProduceSpeed.high      = 20000 bytes/sec
userProduceSpeed.low       = 10000 bytes/sec
workerProduceSpeed.high    = 20000 bytes/sec
workerProduceSpeed.low     = 10000 bytes/sec
```

---

## 十一、关键文件速查

| 文件路径 | 说明 |
|----------|------|
| `worker/src/.../congestcontrol/CongestionController.java` | 主控制器，362 行 |
| `worker/src/.../congestcontrol/UserCongestionControlContext.java` | 用户拥塞状态，88 行 |
| `worker/src/.../congestcontrol/BufferStatusHub.java` | 字节速率统计，92 行 |
| `worker/src/.../congestcontrol/TimeSlidingHub.java` | 滑动窗口基类，165 行 |
| `worker/src/.../congestcontrol/UserBufferInfo.java` | 用户缓冲信息，42 行 |
| `common/src/.../write/SlowStartPushStrategy.java` | 客户端慢启动策略，181 行 |
| `worker/src/.../worker/WorkerSource.scala` | 指标定义（PotentialConsumeSpeed 等） |
| `worker/src/.../worker/Worker.scala` | IsHighWorkload 判断逻辑 |
| `worker/src/.../worker/PushDataHandler.scala` | 拥塞状态码返回 |
| `common/src/main/scala/.../CelebornConf.scala` | 全部配置项定义 |
| `docs/developers/trafficcontrol.md` | 官方流量控制设计文档 |

---

## 十二、总结

Celeborn 的 Congestion Control 通过以下几个关键设计实现了**高效、公平、可观测**的流量控制：

1. **双层触发**：全局水位线（磁盘缓冲/Worker 生产速率）+ 用户级水位线，互相独立，灵活组合。
2. **迟滞状态机**：高水位进入，低水位退出，防止拥塞状态频繁震荡。
3. **公平抑制算法**：全局超水位时，以"平均速率"为基准，仅抑制超额用户，低速用户不受影响。
4. **时间滑动窗口**：1 秒粒度的精确速率统计，窗口大小可配置（默认 10s）。
5. **TCP-like 客户端策略**：慢启动 → 拥塞避免 → 指数退避，精准控制并发推送量。
6. **动态配置**：Worker 级和用户级水位线均可通过 ConfigService 热更新，无需重启。
7. **完善监控**：`IsHighWorkload`、`PotentialConsumeSpeed`、`UserProduceSpeed` 等指标覆盖全链路状态。
