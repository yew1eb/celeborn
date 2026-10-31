# Celeborn TODO 全量分析 — 社区贡献机会评估

> 分析基线:仓库当前分支 `ai-1`,主源码(Java/Scala)共 **32 处 TODO**,另 tests/ 与 web/ 各 1 处,合计 34 处。
> 本文为纯静态分析:逐一精读 TODO 所在代码上下文,评估修复难度、影响面与贡献可行性,并给出推荐项的具体修复方案。

---

## 1. 概述

### 1.1 模块分布

| 模块 | TODO 数 | 涉及文件 |
|------|---------|----------|
| common | 10 | SASL 认证目录(4 个文件)、`CelebornConf`、`ThreadUtils`、`ControlMessages` |
| client | 8 | 读路径(2)、commit(4)、`ChangePartitionManager`、`LifecycleManager` |
| worker | 7 | `PushDataHandler`(3)、`FlushTask`(2)、`PartitionMetaHandler`(2) |
| client-flink | 5 | `FlinkShuffleClientImpl`(2)、tiered/common 各 1 |
| master | 1 | `TagsManager` |
| service | 1 | `SpnegoAuthenticationHandler` |
| tests / web | 2 | `DeploySuite`、`vite.config.ts`(不纳入贡献评估) |

### 1.2 主题聚类

| 主题 | 数量 | 特征 |
|------|------|------|
| A. SASL/安全 | 6 | 机制可配置化 3 处(其一挂 CELEBORN-1158)、注册握手校验缺失 2 处、Kerberos name rules 1 处 |
| B. Flink hybrid shuffle 读写并发 | 9 | 均围绕"下游 task 早于上游启动"与"边写边读"两大设计级缺口,相互纠缠 |
| C. 协议序列化 | 2 | `ControlMessages` 的类型收窄,波及全部 RPC 消息 |
| D. 拥塞控制 | 3 | soft-split 不返回 congest(2 处重复)+ 直存水位线逻辑精化 |
| E. 性能/运维微优化 | 2 | 线程栈 dump 增强、Int→Int 原生 map |
| F. 杂项 | 8 | TagsManager、流关闭、checkpoint、cppClient、日志、HDFS 锁等 |

### 1.3 关键发现

精读过程中发现两处"TODO 与代码现状不符/隐藏真 bug"的情况,是性价比最高的贡献点:

1. **`RegistrationClientBootstrap.java:165` 的 TODO 实际已实现** —— 同文件 `validateServerResponse()`(第 239~257 行)已经完整校验了版本、CLIENT_AUTH 含 ANONYMOUS、CONNECTION_AUTH 含 DIGEST-MD5,即"client validates required/supported mechanism is present"已被覆盖,注释已过时。
2. **`ThreadUtils.scala:468` 存在真 bug** —— `blockedByThreadId.foreach(id => s" Id=$id")` 的 lambda 结果被直接丢弃,`blockedByThreadId` 永远不会输出到线程栈字符串中。这不是"要不要增强"的问题,而是现有输出就有缺陷。

---

## 2. 评估方法

对每个 TODO 按三个维度定级:

- **难度**:低(局部改动、逻辑自包含)/ 中(需跨类理解、加配置项、补测试)/ 高(设计级、涉协议或行为语义变更)
- **影响/价值**:正确性 / 安全 / 运维排障 / 性能
- **适合贡献度**:⭐⭐⭐(优先推荐) / ⭐⭐(可做) / ⭐(不建议,需设计讨论或已有归属)

**"容易贡献"的判定标准**:难度低~中 + 不改变现有行为语义(或变更可配置化、默认关闭)+ 可写单测验证 + 未挂 Jira / 不涉安全协议演进。社区评审成本与代码量同样重要:动 RPC 协议、动拥塞语义的改动即使代码量小,评审周期也长。

---

## 3. 推荐贡献清单

### 3.1 ⭐⭐⭐ 修复 `ThreadStackTrace.toString` 丢弃 `blockedByThreadId` 的 bug(顺手实现 TODO)

**位置**:`common/src/main/scala/org/apache/celeborn/common/util/ThreadUtils.scala:461-481`

**现状摘录**:

```scala
override def toString: String = {
  val sb = new StringBuilder(
    s""""$threadName" Id=$threadId $threadState""")
  lockName.foreach(lock => sb.append(s" on $lock"))
  lockOwnerName.foreach {
    owner => sb.append(s"""owned by "$owner"""")
  }
  blockedByThreadId.foreach(id => s" Id=$id")   // ← bug:结果被丢弃
  if (suspended) sb.append(" (suspended)")
  ...
}
```

**问题分析**:`foreach` 传入的 lambda 返回 String 但没人消费,等价于空操作。被 `blockByThreadId` 阻塞的线程在 dump 输出中看不到阻塞者 Id,直接损害这个类存在的意义——排障时定位线程互相阻塞。该类源自 Spark v3.5.1(`ThreadUtils.scala:432` 注释),Spark 侧对应 SPARK-44896 的讨论中同样指出应补充 `os_prio、cpu、elapsed、tid、nid` 等 jstack 工具信息。

**修复方案**:
1. 最小修复(必做):`blockedByThreadId.foreach(id => sb.append(s" owner Id=$id"))`
2. 可选增强(兑现 TODO):在 `ThreadStackTrace` 的工厂方法里从 `java.lang.management.ThreadInfo` 补充 `waitedTime`、`waitedCount`、`blockedTime`、`blockedCount` 等可从 JVM 拿到的字段(`os_prio/tid/nid` 等仅 jstack 原生工具可见,JMX 拿不到,可不做)。

**测试**:`common` 模块单测,构造含 `blockedByThreadId` 的 `ThreadStackTrace` 断言 toString 输出包含该 Id。可新增 `ThreadUtilsSuite` 用例或复用现有 suite(先 `grep -rn "ThreadStackTrace" common/src/test` 确认)。

**工作量**:最小修复 < 0.5 天;含单测 1 天。**贡献建议**:直接开 Jira 提 PR,标题建议 `ThreadStackTrace.toString drops blockedByThreadId`。修复 bug 部分社区通常快速合入。

---

### 3.2 ⭐⭐⭐ `TagsManager.getTagStore` 每次调用都重建 ConcurrentHashMap

**位置**:`master/src/main/scala/org/apache/celeborn/service/deploy/master/tags/TagsManager.scala:43-51`

**现状摘录**:

```scala
private def getTagStore: ConcurrentHashMap[String, JSet[String]] = {
  configService match {
    case Some(cs) =>
      // TODO: Make configStore.getTags return ConcurrentMap
      JavaUtils.newConcurrentHashMap(cs.getSystemConfigFromCache.getTags)
    case _ =>
      defaultTagStore
  }
}
```

**问题分析**:
1. **性能**:`getTaggedWorkers()` 对 tags 表达式里**每个 tag** 都调用一次 `getTagStore`(第 77 行在 `tags.foreach` 循环内),每次都把整个 `Map<String, Set<String>>` 拷贝进新建的 ConcurrentHashMap;且内层 `Set<String>` 是普通 Set,并发访问下无保障。
2. **一致性隐患**:`addTagToWorker`/`removeTagFromWorker` 只写 `defaultTagStore`,当 `configService` 存在(动态配置启用)时,运行期通过 REST 接口加的 tag 永远不会出现在 `getTagStore` 返回的副本里——读写两个 store。

**修复方案**(二选一,推荐前者):
- **方案 A(最小、无行为变更)**:在 `TagsManager` 内缓存 `getTagStore` 的结果,依赖 `configService` 的缓存刷新语义重新构建。需理解 `FsConfigServiceImpl` 的刷新机制(`FsConfigServiceImpl.java:56` 的 `setTags(getTags(configMap))`),在刷新回调或按版本号失效时重建。
- **方案 B(兑现 TODO 原意)**:把 `DynamicConfig.getTags()`(`DynamicConfig.java:256`,返回 `Map<String, Set<String>>`)的存储字段改为 `ConcurrentHashMap` + `ConcurrentHashMap.newKeySet`,即修改 `FsConfigServiceImpl.java:117` 的 `getTags(configMap)` 构造方式,`TagsManager` 直接消费,消除拷贝。同时把 `getTaggedWorkers` 循环内的 `getTagStore` 提到循环外(只调用一次)。

**测试**:`master` 模块单测(`./build/mvn test -pl master`),现有 `TagsManagerSuite` 若存在可直接扩展断言 tag 增删后 `getTaggedWorkers` 结果正确。

**工作量**:1~2 天。**贡献建议**:先在 dev@celeborn.apache.org 或 Jira 上简述方案 A/B 的取舍(涉及动态配置热更新语义,选错方案评审会打回),再提 PR。

---

### 3.3 ⭐⭐⭐ 服务端校验 `AuthenticationInitiationRequest`(安全加固)

**位置**:`common/src/main/java/org/apache/celeborn/common/network/sasl/registration/RegistrationRpcHandler.java:150-157`

**现状摘录**:

```java
case AUTHENTICATION_INITIATION_REQUEST_VALUE:
  // TODO: not validating the auth init request. Should we?
  PbAuthenticationInitiationRequest authInitRequest = pbMsg.getParsedPayload();
  checkRequestAllowed(RegistrationState.NONE);
  respondToAuthInitialization(callback);
```

**问题分析**:客户端在发起认证前会先发 `AUTHENTICATION_INITIATION_REQUEST`,带上自己的版本号、`authEnabled` 与支持的机制列表(`RegistrationClientBootstrap.java:134-140`),但服务端解析后完全不看:不校验版本是否兼容、不校验 `authEnabled` 是否与服务端一致、不校验客户端机制列表与本地 `SASL_MECHANISMS` 是否有交集,直接回响应。版本不兼容的客户端要等到后续 SASL/注册阶段才会以更含糊的方式失败。校验缺失本身也是安全实现上的观测点(当前无实际利用面,因为响应内容不依赖请求内容)。

**修复方案**:
在 `processRpcMessage` 的 `AUTHENTICATION_INITIATION_REQUEST_VALUE` 分支中补充:
1. `authInitRequest.getVersion()` 不等于 `VERSION`("1.0")时抛 `IOException`/`SecurityException`(与 client 侧 `validateServerResponse` 的版本对等校验对称,`RegistrationClientBootstrap.java:240` 已有先例);
2. `authInitRequest.getAuthEnabled() != conf.authEnabled()` 时告警或报错(倾向报错,双端语义不一致无法完成后续认证);
3. (可选)校验客户端机制列表与本地 `SASL_MECHANISMS` 在 CLIENT_AUTH/CONNECTION_AUTH 上均有交集。

注意保持响应失败路径与 `receive()` 的异常处理一致(第 121~130 行已有 RpcFailure 回写逻辑,抛异常即可复用)。

**测试**:`common` 模块 SASL 相关单测,可参考 `common/src/test` 下 sasl 目录的既有测试形态,新增"版本不匹配的 auth init 请求被拒"用例。`./build/mvn test -pl common -Dtest=<SaslSuite>`。

**工作量**:1~2 天。**贡献建议**:开 Jira(安全加固类,如 `RegistrationRpcHandler should validate AuthenticationInitiationRequest`)。改动内聚于一个 switch 分支,评审面小。

---

### 3.4 ⭐⭐ 清理 `RegistrationClientBootstrap` 中已过时的 TODO

**位置**:`common/src/main/java/org/apache/celeborn/common/network/sasl/registration/RegistrationClientBootstrap.java:165`

**现状摘录**:

```java
if (!validateServerResponse(authInitResponse)) { ... throw new CelebornException(exMsg); }
// TODO: client validates required/supported mechanism is present
```

**问题分析**:TODO 描述的"client 校验必需/支持的机制存在"已由 `validateServerResponse()`(同文件第 239~257 行)完整实现——校验版本、CLIENT_AUTH 含 ANONYMOUS、CONNECTION_AUTH 含 DIGEST-MD5。该注释是历史遗留,会误导后续贡献者重复实现。

**修复方案**:删除该 TODO 行;若要与 3.3 的服务端校验呼应,可在 `validateServerResponse` 上补一行 javadoc 说明校验内容。可与 3.3 合并成一个 PR(同一交互链路的客户端/服务端两侧),提高评审通过率。

**工作量**:0.5 天(含跑相关测试)。**贡献建议**:并入 3.3 的 PR,或单独的 trivial cleanup PR。

---

### 3.5 ⭐⭐ Kerberos name rules 可配置化

**位置**:`service/src/main/scala/org/apache/celeborn/server/common/http/authentication/SpnegoAuthenticationHandler.scala:66-71`

**现状摘录**:

```scala
// TODO: support to config kerberos.name.rules and kerberos.rule.mechanism
// set default rules if no rules set, otherwise it will throw exception
// when parse the kerberos name
if (!KerberosName.hasRulesBeenSet) {
  KerberosName.setRules("DEFAULT")
}
```

**问题分析**:SPNEGO 认证后需把 Kerberos principal 映射为系统用户名,规则由 Hadoop `KerberosName` 的静态规则决定。当前硬编码 fallback 为 `"DEFAULT"`,多 principal 映射场景(如 `rule:...`)无法配置,`HADOOP_SECURITY_AUTH_TO_LOCAL` 环境变量也未必被 Celeborn 进程继承。

**修复方案**:
1. 在 `CelebornConf` 加两个配置项(遵循 builder pattern,见 AGENTS.md「Adding a Config Entry」):
   - `celeborn.http.kerberos.authToLocalRules`(默认 `DEFAULT`)
   - `celeborn.http.kerberos.ruleMechanism`(`hadoop` 风格的规则机制开关,若确有必要)
2. `init()` 中改为:`if (!KerberosName.hasRulesBeenSet) KerberosName.setRules(conf.kerberosAuthToLocalRules)`,无配置时保持 `"DEFAULT"` 不变(向后兼容);
3. 执行 `UPDATE=1 build/mvn test -pl common -Dtest=ConfigurationSuite` 重新生成配置文档。

**测试**:service 模块单测,断言配置了自定义 rules 时 `KerberosName.getRules()` 生效、未配置时仍为 DEFAULT。

**工作量**:1 天。**贡献建议**:标准配置项 PR 流程,注意配置文档再生成与 `./dev/reformat`。唯一不确定点是 `ruleMechanism` 的语义(注释里自带的问句),可在 Jira 描述里问 maintainer,或首版只交付 rules 可配置。

---

### 3.6 ⭐⭐ `shuffleToCompletedMappers` 换原生 Int→Int 结构(备选)

**位置**:`client/src/main/scala/org/apache/celeborn/client/commit/ReducePartitionCommitHandler.scala:80-81`

**现状摘录**:

```scala
// TODO: Move this to native Int -> Int Map
private val shuffleToCompletedMappers = JavaUtils.newConcurrentHashMap[Int, Int]()
```

**问题分析**:装箱 Int 键值对在高频 stage-end 递增路径上有分配开销。TODO 本意是换成无装箱的并发结构。

**修复方案**:Java 侧无现成并发 Int2IntMap;可选(1)自旋 + `AtomicInteger` 值;(2)`ConcurrentHashMap[Int, AtomicInteger]`(仍有键装箱);(3)引入 fastutil 的 `Int2IntOpenHashMap` + 分片锁(引入新依赖,需按 AGENTS.md 流程改 root pom + LICENSE-binary,成本骤增)。除非有压测数据证明收益,否则性价比一般。

**工作量**:1~3 天(视方案)。**贡献建议**:低优先级备选;若做,建议先在 Jira 里附 JMH 数据说明收益,否则容易被标 won't fix。

---

## 4. 其余 TODO 一览(不推荐现在动手,附理由)

| # | 位置 | 主题 | 不推荐理由 |
|---|------|------|-----------|
| 1 | `CelebornSaslClient.java:46` | SASL 机制硬编码 DIGEST-MD5 | **已有 Jira: CELEBORN-1158**。安全协议演进,涉及双端协商,归属明确,不宜抢跑 |
| 2 | `SaslClientBootstrap.java:72` | 同上 | 同 CELEBORN-1158 |
| 3 | `RegistrationRpcHandler.java:62` | 服务端 SASL 机制可配置 | 与 1/2 同主题,依赖机制可配置的整体设计 |
| 4 | `RegistrationClientBootstrap.java:74` | 客户端 SASL 机制可配置 | 同上 |
| 5 | `CelebornConf.scala:1469` | 直存三水位线逻辑精化 | 直存拥塞控制整体重构议题,行为语义变更,需设计文档先行 |
| 6 | `ControlMessages.scala:536` | `toTransportMessage` 参数类型收窄 | 全量 RPC 消息类型层级变更,巨型重构 |
| 7 | `ControlMessages.scala:1071` | `fromTransportMessage` 返回类型收窄 | 同上,且是破坏性 API 变更 |
| 8 | `PushDataHandler.scala:315` | soft-split 时不返回 congest | 拥塞行为语义变更,依赖"客户端会快速停止推送"的假设,需数据验证 |
| 9 | `PushDataHandler.scala:394` | 同上(replica 路径) | 同上,两处为同一决策 |
| 10 | `PushDataHandler.scala:1543` | ended attempt 仅 info 日志 | 排查时反而是 warning 有用;改成可配置日志噪音更大,价值存疑 |
| 11 | `FlushTask.scala:77` | 读写并发场景 force flush | **Flink hybrid shuffle 主题**(边写边读),依赖下游读可见性的整体设计 |
| 12 | `FlushTask.scala:111` | HDFS 流并发写去锁 | 取决于 Hadoop `FSDataOutputStream` 的并发语义,非 Celeborn 单方可控 |
| 13 | `PartitionMetaHandler.scala:151` | region 结束 flush 数据 | Flink hybrid shuffle 主题 |
| 14 | `PartitionMetaHandler.scala:228` | force flush index 文件 | Flink hybrid shuffle 主题 |
| 15 | `ChangePartitionManager.scala:397` | 新分片位置同步给下游 | Flink hybrid shuffle 主题(下游早启动) |
| 16 | `LifecycleManager.scala:426` | 消息不支持 cppClient | 依赖 cpp client 生态进度,Java 侧无法独立闭环 |
| 17 | `CommitHandler.scala:233` | segment 粒度可见时处理 pending 请求 | Flink hybrid shuffle 主题 |
| 18 | `CommitHandler.scala:600` | 提前记录 partition locations | Flink hybrid shuffle 主题 |
| 19 | `MapPartitionCommitHandler.scala:364` | 等待上游注册再回 GetReducerFileGroup | Flink hybrid shuffle 主题 |
| 20 | `CelebornInputStream.java:945` | "consider closing the stream" | 疑似资源泄漏,但读流关闭时序与 checkpoint/异常路径强耦合,改动需全链路推演;作者本人(gaurav)都标注"consider",盲改风险大 |
| 21 | `LocalPartitionReader.java:308` | checkpoint metadata 未实现 | 功能补齐(对照 `WorkerPartitionReader.java:270` 已有实现),中等工作量但收益仅限 local read 场景,优先级由社区定 |
| 22 | `CelebornTierConsumerAgent.java:185` | open reader 失败应重试 | Flink tiered + 下游早启动主题 |
| 23 | `RemoteShuffleInputGate.java:208` | 方法仅 1.19 使用 | Flink 版本兼容杂项,随 Flink 适配自然解决 |
| 24 | `ShuffleResourceTracker.java:102` | map partition 副本 untrack 重构 | Flink 副本语义设计变更 |
| 25 | `FlinkShuffleClientImpl.java:242` | 分片位置为空时应重试 | Flink hybrid shuffle 主题 |
| 26 | `FlinkShuffleClientImpl.java:482` | 握手失败需带原因 revive | 小改进但依赖 revive 协议扩展错误码,涉协议变更 |
| 27 | `DeploySuite.scala:28` | k8s IT 补 shuffle client 测试 | 集成测试工程量大且依赖 CI 环境 |
| 28 | `web/vite.config.ts:28` | 前端代理地址 | 非源码 TODO,无贡献价值 |

注:上表 8~14 项中"同一决策/同一主题"的重复 TODO 实际对应 6~7 个独立议题;Flink hybrid shuffle 主题合计 9 处,是 Celeborn 支持该特性的已知整体缺口,适合以设计提案(而非逐个 TODO)的方式参与。

---

## 5. 总结:建议的提交顺序

1. **第一发**:`ThreadUtils.scala` 的 `blockedByThreadId` bug 修复(3.1 最小修复部分)——真 bug、十几行、单测直白,几乎零评审风险。
2. **第二发**:服务端 auth init request 校验 + 客户端过时 TODO 清理(3.3 + 3.4 合并一个 PR)——安全加固 + 顺手 cleanup,链路自洽。
3. **第三发**:Kerberos name rules 可配置(3.5)——标准配置项流程,注意重新生成配置文档。
4. **第四发**:`TagsManager` 拷贝问题(3.2)——动手前先在邮件列表确认方案 A/B。
5. **备选**:3.6(需 JMH 数据)、`LocalPartitionReader` checkpoint(21,需社区确认优先级)。

每个 PR 遵循社区 Checklist:先开 CELEBORN-XXXX Jira、`./dev/reformat`、相关模块单测通过、配置变更时用 `UPDATE=1` 重跑 `ConfigurationSuite`。
