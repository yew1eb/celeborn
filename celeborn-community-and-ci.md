# Celeborn 社区贡献与 CI 机会（合并篇）

> 基线：上游 `main` @ `626a07cfc`（Metrics / Auth 行号）｜本地 `ai-1`（TODO / CI 行号）｜整理：2026-10-31
> 本文由 5 份调研合并精简而来（代码 TODO、Metrics/Grafana、Auth/SASL、CI 日志台账、CI Spark 4.2 分析），源文件已删除，历史见 git。
> 证据标记：**✅** 已回读源码确认；**⚠️** 来自子 agent 报告，动工前需按基线复核行号。

## 1. 总览

| 方向 | 可提交点 | 首推 | 体量 |
|---|---|---|---|
| 代码 TODO | 6 项推荐 / 28 项暂缓 | `ThreadStackTrace.toString` 真 bug | <0.5 天 |
| Metrics / Grafana | 20（P0 5 / P1 9 / P2 6） | P0 中 3 个代码 bug | 1~10 行 |
| Auth / SASL | 11 个 PR 候选 | replicate 端口补 SASL → checkAuth | 小~中 |
| CI | 11 个问题 + 5 项提案 | 测试泄漏修复、Maven 本体缓存 | 数十行 |

**跨方向第一批 PR**：T1（真 bug）→ Metrics PR-1（3 个计数 bug）→ Auth PR-2（replicate SASL，必须先于 PR-1）→ CI C2（测试泄漏修复）。

---

## 2. 代码 TODO：34 处，6 项推荐

### 2.1 分布

主源码 32 处 + tests/web 各 1 处：

| 模块 | 数量 | 主要位置 |
|---|---|---|
| common | 10 | SASL 认证目录(4 文件)、`CelebornConf`、`ThreadUtils`、`ControlMessages` |
| client | 8 | 读路径(2)、commit(4)、`ChangePartitionManager`、`LifecycleManager` |
| worker | 7 | `PushDataHandler`(3)、`FlushTask`(2)、`PartitionMetaHandler`(2) |
| client-flink | 5 | `FlinkShuffleClientImpl`(2)、tiered/common 各 1 |
| master / service | 2 | `TagsManager`、`SpnegoAuthenticationHandler` |
| tests / web | 2 | `DeploySuite`、`vite.config.ts`（不纳入贡献评估） |

主题上，**Flink hybrid shuffle 读写并发**独占 9 处（设计级缺口，适合提设计提案而非逐个 TODO），SASL/安全 6 处，协议序列化 2 处，拥塞控制 3 处。

### 2.2 推荐清单

| # | 位置 | 问题 | 修复 / 注意 |
|---|---|---|---|
| **T1** ⭐⭐⭐ | `common/.../util/ThreadUtils.scala:461-481` | `blockedByThreadId.foreach(id => s" Id=$id")` 的 lambda 结果被丢弃，线程 dump 永远看不到阻塞者 Id——**现有输出就是错的**，不只是"待增强" | 改 `sb.append(s" owner Id=$id")`；顺带可由 `ThreadInfo` 补 `waitedTime/blockedCount` 等 JMX 可见字段。含单测 1 天，社区通常快速合入 |
| **T2** ⭐⭐⭐ | `master/.../tags/TagsManager.scala:43-51` | `getTagStore` 每次调用都把整个 tag 表拷贝进新 `ConcurrentHashMap`，且 `getTaggedWorkers` 在 `tags.foreach` 循环内逐个 tag 调用；`addTagToWorker` 只写 `defaultTagStore`，动态配置开启时出现**读写两个 store** | 方案 A：缓存结果 + 按配置版本失效；方案 B：`DynamicConfig.getTags()` 改并发容器并去掉循环内重复调用。涉及热更新语义，**先邮件列表确认 A/B 再提 PR** |
| **T3** ⭐⭐⭐ | `common/.../sasl/registration/RegistrationRpcHandler.java:150-157` | 服务端收到 `AUTHENTICATION_INITIATION_REQUEST` 后完全不看版本 / `authEnabled` / 机制列表，直接回响应 | 补三项校验（版本、authEnabled 一致性、机制交集），失败抛异常复用既有 `RpcFailure` 回写路径。1~2 天，改动内聚在一个 switch 分支 |
| **T4** ⭐⭐ | `.../RegistrationClientBootstrap.java:165` | TODO 描述的"client 校验机制存在"**已由** `validateServerResponse()`(239-257) 实现，注释过时，会误导后续贡献者重复实现 | 删 TODO 行，可与 T3 合成一个 PR（同一交互链路两侧） |
| **T5** ⭐⭐ | `service/.../http/authentication/SpnegoAuthenticationHandler.scala:66-71` | Kerberos name rules 硬编码 fallback `"DEFAULT"`，多 principal 映射无法配置 | 新增 `celeborn.http.kerberos.authToLocalRules`（默认 `DEFAULT`，向后兼容），按 `CelebornConf` builder 模式；改完 `UPDATE=1` 重跑 `ConfigurationSuite` 生成配置文档 |
| **T6** ⭐⭐ | `client/.../commit/ReducePartitionCommitHandler.scala:80-81` | `ConcurrentHashMap[Int, Int]` 装箱开销；TODO 想要无装箱 Int→Int | Java 无现成并发 Int2IntMap，引入 fastutil 成本高。**先给 JMH 数据**，否则易被 won't fix |

### 2.3 其余 28 项：不推荐现在动手

| 类别 | 数量 | 代表位置 | 理由 |
|---|---|---|---|
| Flink hybrid shuffle 设计缺口 | 9 | `PushDataHandler.scala:315/394`、`FlushTask.scala:77`、`PartitionMetaHandler.scala:151/228`、`ChangePartitionManager.scala:397`、`CommitHandler.scala:233/600`、`MapPartitionCommitHandler.scala:364` | 围绕"下游早启动 / 边写边读"的整体设计，逐个改不闭环，宜提设计提案 |
| SASL 机制可配置 | 4 | `CelebornSaslClient.java:46`、`SaslClientBootstrap.java:72`、`Registration*Bootstrap/Handler` | 已归属 **CELEBORN-1158**，不要抢跑 |
| 协议 / 语义变更 | 4 | `ControlMessages.scala:536/1071`（类型收窄）、`CelebornConf.scala:1469`（直存水位线） | 动 RPC 类型层级或拥塞语义，评审周期长 |
| 资源 / 功能补齐 | 4 | `CelebornInputStream.java:945`（consider closing）、`LocalPartitionReader.java:308`（checkpoint）、`LifecycleManager.scala:426`（cppClient） | 读流关闭与异常路径强耦合，盲改风险大；其余依赖生态或收益有限 |
| 其余 | 7 | Flink tiered / 版本兼容 / `DeploySuite` k8s IT / `vite.config.ts` | 集成测试成本高或非源码 TODO |

### 2.4 提交顺序

T1 → T3+T4（同一 PR）→ T5 → T2（先确认方案）→ 备选 T6 / checkpoint 补齐。每个 PR：先开 CELEBORN-XXXX Jira、`./dev/reformat`、相关模块单测、配置变更重跑 `ConfigurationSuite`。

---

## 3. Metrics / Grafana：20 个可提交点

### 3.1 P0：确定性 bug（1~10 行，第一批）

| # | 问题 | 证据 | 修复 |
|---|---|---|---|
| M1 | `addHistogram` 算出带 label 的 key，却用**裸 name** 注册进 Dropwizard → 所有 label 变体共享同一 Histogram；`removeMetric` 用带 label 的 key 删除，永远删不掉 | ✅ `AbstractSource.scala:208-215`（对比 `addTimer:184` / `addCounter:199` / `addGauge:121` 均传 `metricNameWithLabel`） | 1 行：`metricRegistry.histogram(metricNameWithLabel, ...)` |
| M2 | `RpcMetricsTracker.close()` 注册时带 `NAME_LABEL`、移除时不带 → key 不匹配，**移除是空操作**，dispatcher/endpoint 每次生命周期泄漏 gauge/timer | ✅ `RpcMetricsTracker.scala:64-66` vs `:71-73` | 三处 `removeGauge/removeTimer` 补 `NAME_LABEL` |
| M3 | `getAllMetricsNum` 漏统计 histogram，容量告警数值偏低（`getMetrics` 却会输出 histogram） | ✅ `AbstractSource.scala:638-645` vs `:649` | 补 `namedHistogram.size()` |
| M4 | Grafana 面板引用不存在的 `metrics_LocalFetchChunkTime_Max`，该面板永久 No data（真实常量 `FetchLocalChunkTime`） | ✅ `celeborn-dashboard.json:6071,6076`；`WorkerSource.scala:176` | 改为 `metrics_FetchLocalChunkTime_Max` |
| M5 | `docs/monitoring.md:124` 记录幽灵指标 `RatisApplyCompletedIndexDiff`（代码只有 `RatisApplyCompletedIndex`），Grafana 还配了空面板 | ✅ 全仓 grep 无该指标；`MasterSource.scala:82` | 删文档行 + 删面板；或真正实现 diff 指标 |

> M1 的后果被掩盖，是因为仅有的两个 histogram（`FetchChunkTransferSize` / `PartitionFileSize`）都没带 label；加 `disk` label 后每块盘会读到同一份聚合数据。

### 3.2 P1：质量与一致性（9 项）

- **M6 杠杆最高**：`dev/lint_grafana.py` 只查 panel id 重复（73 行脚本的唯一逻辑），对单位/描述/重复标题/gridPos/`time.from-to` 全不校验 → 下列缺陷可以随意再次混入。扩展它等于把整类问题永久挡在 CI 外。顺带把 `grafana.yml:52` `git clone` 的 `dashboard-linter` **pin 版本**。
- **M7** 3 组复制粘贴重复面板（`OssFlushSize_Count` id 247/248、`S3FlushSize_Count` 249/250、`RegionStartFailCount_Count` 154/155），而 `RegionFinishFailCount` / `OssFlushCount` / `S3FlushCount` 完全没面板。
- **M8** RPC timer 面板单位写 `ns`，实际经 `reportNanosAsMills` 导出为毫秒（`AbstractSource.scala:584,590`，面板 229/230/232/233）→ 显示差 10⁶。
- **M9** 172/194 面板无 `role` 过滤，`RegisteredShuffleCount` 等 master/worker 同名指标混画，大盘也没 `role` 模板变量。
- **M10** 194 面板全是 timeseries，92 个缺 `unit`、164 个缺 `description`，`graphTooltip:0`，JVM 大盘连 `refresh` 都没有。
- **M11** `docs/monitoring.md:412` 称 DirectArena 指标"未进 Grafana"，实际面板 158/164/173/171/201-216/254-259 已画 → 文档过时。
- **M12** 命名三种风格并存：`ThreadPoolSource` 12 个 snake_case 字面量无常量（每个名字写两遍）、`ResourceConsumptionSource` lowerCamel、其余 UpperCamel。
- **M13** `MessageWithHeader.java:134,138,139` 用裸字符串而非 `WorkerSource` 常量 → 改名不报错、指标静默失效。
- **M14** `ObservedDevice.scala:87` 把设备名/状态拼进指标名（每台机一个指标名），同仓已有 `device`/`mountpoint` label 的正确范式。

### 3.3 P2：设计级（6 项，先发 dev@ 讨论）

- **M15 最值得发邮件**：`getMetrics` 是**破坏性读取**（clear timer 队列 + 每个 timer `reservoir.reset()`），默认同时挂 Prometheus/Json/Logger 三个消费者 → 谁先抓谁拿走、分位数无意义。修法需改非破坏性快照 + 短 TTL，影响 Prometheus 语义。
- **M16** `namedTimers` 的 in-flight startTime map 无上界，仅 `size>5000` 且每 10 分钟清一次，key 是文件路径/UUID。
- **M17** 每个 Source 一个 `metricsCleaner` 单线程池，只在 `destroy()` 关闭，而 `destroy()` 全仓仅 `NettyRpcEnv.scala:364` 一处调用 → 线程泄漏；线程名还硬编码 `worker-metrics-cleaner`。
- **M18** label 值未转义就拼进 Prometheus 文本，含 `"`/`\` 会产生非法输出使整次 scrape 失败。
- **M19** 客户端（LifecycleManager）完全没有 MetricsSystem，`Role.CLIENT` 已定义未使用。
- **M20** Quota 无指标；`ResourceConsumption` 只有 disk/hdfs，缺 S3/OSS，而 `WorkerSource.scala:73-76` 已在统计 S3/OSS flush → 配额核算对对象存储是瞎的。

### 3.4 PR 切分

| PR | 内容 | 说明 |
|---|---|---|
| PR-1 | M1+M2+M3 | 纯代码 bug + 单测，标题参考 `[CELEBORN-XXXX] Fix histogram registration ignoring labels and metric removal key mismatch` |
| PR-2 | M4+M5+M7 | 删/改幽灵与重复面板，补 `RegionFinishFailCount` / `OssFlushCount` / `S3FlushCount` |
| PR-3 | M6 | 扩展 lint 脚本 + pin `dashboard-linter` 版本 |
| PR-4 | M11 + 文档语义更正（`docs/monitoring.md:238` 的 "last hour" 实为进程启动以来累计） | 文档漂移 |
| PR-5 | M15 | 需先 `dev@` 讨论 |

PR-1 必须补两条单测：不同 label 的 histogram 取值互相独立（`CelebornSourceSuite.scala:32` 只测了无 label 版本，所以 M1 一直没被发现）；`RpcMetricsTracker.close()` 后 gauge/timer 真正被移除（现有测试未覆盖 `close()`）。

---

## 4. Auth / SASL：20+ 发现，11 个 PR 候选

从 Spark `org.apache.spark.network.sasl` 移植后基本停在"能跑通 happy path"的状态，留下了 TODO、死字段和 Celeborn 特有的缺口。

### 4.1 两个根问题（必须先 A2 再 A1）

**A1 ✅ `checkAuth` 在 `clientId == null` 时完全 fail-open** —— `BaseMessageHandler.java:50` 的条件是"有 ID 才校验"，不是"弱校验"而是条件反转。它是系统唯一的按应用鉴权模型，被 **17 处**调用（`Master.scala` 11、`PushDataHandler.java:995`、`FetchHandler.scala:151/367`、`Controller.scala:118/149/167`）。默认 `celeborn.auth.enabled=false` → `clientId` 恒为 null → 鉴权全部退化为 no-op。Scala 版 `RpcEndpoint.scala:149` 是第二份独立实现（`!=` vs `.equals`），两份已漂移。

**A2 ✅ Worker replicate 端口完全没有 SASL** —— `Worker.scala:251-252` 建 replicate server/client factory 时漏传 `getServerBootstraps`（`TransportContext` 一参重载走 `emptyList()`），而 :230/:272 的 push/fetch 都传了。任何能连 `worker.replicate.port` 的主机都可以任意 appId 推/复制数据，且因 A1 被 `PushDataHandler:995` 静默放行。修复需**成对**：server + client bootstrap。

### 4.2 其余发现

| # | 问题 | 影响 |
|---|---|---|
| A3 ⚠️ | 服务端 SASL 握手无超时、无轮次上限（`SaslRpcHandler:81-108` 可无限重入；`conf`/`channel` 字段赋值后从未读，是死字段） | 握手卡住时无日志、连接不关 |
| A4 ✅ | `SaslTimeoutException` 不可达：`TransportClient.sendRpcSync` 把超时包成 `IOException`，`catch (RuntimeException)` 永不命中；5 处 throw、0 处 catch | 死代码 + 吞 `InterruptedException` 不恢复标志 |
| A5 ⚠️ | SASL 回调里同步 master RPC 跑在 Netty event loop（`CelebornSaslServer:140 → WorkerSecretRegistryImpl:60 askSync`）；cache 无 `expireAfterWrite`、无 negative caching | 随机 appId 可卡 IO 线程 + 打 master + 挤掉合法缓存 |
| A6 ⚠️ | secret 全生命周期明文：`encodePassword` 只是 Base64；Ratis 日志/快照、注册请求、master→worker 全是明文；`RegistrationRpcHandler:236-250` 接受客户端自带 secret 且不校验 | 需 CIP 级改动 |
| A7 ⚠️ | `CelebornSaslServer:141` 区分"未注册"与"密码错"，配合 `Throwables.getStackTraceAsString` 把完整堆栈发给未认证对端 | appId 存在性 oracle |
| A8 ⚠️ | 应用结束后 worker 侧 secret 永不清除：`MasterSecretRegistryImpl.unregister` 无生产调用方；`handleApplicationLost` 不通知 worker；协议也没有 `APPLICATION_META_REMOVED` | 需改协议，先发 JIRA |
| A9 ⚠️ | 注册阶段的 "client auth" 是 ANONYMOUS，任何能连 master RPC 的人可抢注 appId，真应用注册时撞 `isRegistered` | 需讨论 |

**可观测性（B）**：全仓零 auth 指标（无 `AuthSource`，失败无专门日志，`unauthenticatedConnectionAccepted`、握手超时、secret 读取审计全部缺失）；测试只有 happy path，明确缺 **15 项**（错误 secret / 未知 appId / revoke 失效 / 握手超时 / 双端开关不一致 / `checkAuth null` / replicate 绕过 / 并发等），其中 `checkAuth clientId==null` 最该先补。

**配置与文档（C）**：`auth`/`security` 两个配置分类**没有任何文档页**，7 个配置（`celeborn.auth.enabled` + 6 个 deserialization filter）是孤儿；`networkIoSaslTimoutMs` 拼写错误（Timout）且 `Long→Int` 静默截断、无 `checkValue`；`docs/security.md` 没说"SASL 只做认证（`QOP=auth`，`wrap/unwrap` 无调用方）→ 数据明文无完整性保护"、没说注册本身未认证、没提 secret 生命周期；`CelebornConf.scala:3696/3705` 注释指向已迁移的 `common.authentication` 包（实为 `spi.authentication`）。

**协议（D）**：D1 机制可配置（CELEBORN-1158，仍 open，代码里 5 处硬编码 DIGEST-MD5）；D2 QOP 可配置（需接 `wrap/unwrap` 进 framer）；D3 secret TTL/轮换（接口无此概念）；D4 `Sasl.SERVER_AUTH` 放进了 server props（JDK 定义为客户端侧属性）；D5 `DEFAULT_REALM="default"` 硬编码；D6 `decodeIdentifier` 用宽松 Base64，不同输入可解出同一 userName、空串也接受。

### 4.3 PR 候选（按性价比）

| # | 建议标题 | 涉及 | 体量 | 备注 |
|---|---|---|---|---|
| 2 | Enable SASL on the worker replicate port | `Worker.scala:251-252`、`:1146` | 小 | **先做**，否则修 A1 会打断 replication |
| 1 | Fix checkAuth fail-open when clientId is null | `BaseMessageHandler.java:50`、`RpcEndpoint.scala:149` | 小 | 鉴权模型的根，17 处调用点 |
| 3 | Add auth metrics + failure logging | 新 counter、`SaslRpcHandler:81-108` | 小~中 | 纯增量、零兼容风险 |
| 4 | Add docs/configuration/auth.md + security.md | `ConfigurationSuite`、`docs/configuration/index.md` | 小 | 一次消除 7 个孤儿配置，CI 可守 |
| 5 | Add server-side SASL negotiation timeout | `SaslRpcHandler:51/54`（现成死字段） | 中 | "该做没做"证据充分 |

其余：6 让 `SaslTimeoutException` 可达或删除；7 ApplicationLost 清 worker secret（改协议）；8 补测试套件（可拆 2-3 个 PR）；9 注册加固（校验 secret、去 oracle）；10 CELEBORN-1158；11 secret 查询移出 event loop（SPI 异步化）。

**不建议直接提**：QOP 默认提到 `auth-int`（需协议层改造，混版本会握手失败）；secret 改 KDF/哈希存储（破坏三端兼容，CIP 级）。

---

## 5. CI：问题台账 + 优化提案

### 5.1 日志索引（2026-10-29，4 个 job 全绿）

| # | Job | 行数 | 结论 |
|---|---|---|---|
| 1 | Test with SBT (Spark 4.2) | 29207 | 124 succeeded / 0 failed / 7 canceled，无 `##[error]` |
| 2 | Test with Maven | 29698 | 2× BUILD SUCCESS，12 组 `Tests run` 0 失败 |
| 3 | celeborn_integration_test | 11679 | 3× BUILD SUCCESS；535 条 WARNING 见下 |
| 4 | celeborn_cpp_integration_test | 5053 | 10× BUILD SUCCESS，9 个混合读写测试全过；1368 条 WARNING（P10/P11） |

ERROR 绝大多数是故障注入/混沌测试的预期输出（CommitFiles 重试 ~620、Revive/杀 worker ~610、IntegrityCheck 损坏数据 ~120），真正要跟进的集中在 P1-P11。

### 5.2 问题清单

| # | 问题 | 定性 / 跟进 |
|---|---|---|
| **P1** | **Netty epoll 接收缓冲泄漏**：SBT 日志 12417（`push-server-1497-2`）与 Maven 日志 16788（`rpc_app_lifecyclemanager-server-1996-1`）**同一泄漏指纹**，创建于 `MaxMessageHandle.allocate ← AbstractEpollStreamChannel.epollInReady`，均紧邻 ReusedExchange 用例 | 两个 job 同源，不再像偶发噪音；指向请求收到但未走完处理/释放（连接被粗暴关闭）。加 `-Dio.netty.leakDetection.level=paranoid` 复跑 ReusedExchange suite 确认，沿 push server 收包 → `ChunkStreamManager` 生命周期追 release |
| **P2** | **`PartitionFilesSorter` composite buffer 泄漏**：Maven 日志 16274，堆栈直指 `PartitionFilesSorter.java:223 getSortedFileInfo` | worker 存储层业务代码漏 `release()`（排序异常 / 流提前关闭 / 异步删除分支），可直接作社区 PR 候选 |
| P3 | S3 e2e 在 CI 恒 CANCELED ×7 | 与 C4（S3 e2e 修复提案）互为印证：S3 从未真正执行，坏了也不会被发现 |
| P4 | GitHub Actions 弃用告警：`setup-java@v4`、Node 20 强制跑 Node 24（4 个 job 均现） | 仓库级维护：升 `setup-java@v5`、checkout/cache/setup-python 升 Node 24 兼容版本 |
| P5 | `WorkerStatusTracker` ERROR 消息以换行开头，空首行（两 job 各 14 次） | `log.error("\n" + ...)`；改单行，可作社区小 PR |
| P6 | Jersey WADL WARNING ×39（每次起 HTTP 服务一条） | REST 用不到 WADL：禁用 `WadlFeature` 或补 JAXB 依赖 |
| P7 | 构建 WARNING：~70 条 pom（artifactId/version/plugin version 缺失）+ javac deprecated + shade duplicate | 卫生项；代码级可用 `spotless:check` 一并清理（`SparkCryptoHandlerSuiteJ` 5 处等） |
| P8 | `MemoryManager.resumeByPinnedMemory` switch 缺 `NONE_PAUSED` 分支（errorprone，主源码） | 当前语义无事可做，但属"新增枚举易漏"隐患；补空 case 2 行，社区小 PR |
| P9 | 测试代码 ErrorProne：`MissingFail`（`ShuffleClientSuiteJ:703/739/775/906`）、`DefaultCharset` 大量、`UnusedMethod` 等 | `MissingFail` 会掩盖真回归，优先级最高；批量改 `assertThrows`/`getBytes(UTF_8)` |

**P10 `LifecycleManager.onStop` 在 dispatcher 线程内自等待 `awaitTermination`**（cpp job，9 个测试各 1 条，堆栈完全一致）：`onStop` 由 dispatcher 消息循环线程执行，`rpcEnv.shutdown()` 后同步 `awaitTermination()` 让当前线程等自己所在的线程池终止 → 正常路径永不返回，只能靠外部 interrupt 逃逸，被 `Inbox.safelyCall` 记为 ERROR。测试不受影响，但生产重启路径同样是潜在卡点。修复：去掉同步等待交给调用方，或最低限度 catch `InterruptedException` + 恢复中断标志并降级日志。

**P11 exec:java 退出阶段 shutdown hook 失败 + 线程 linger**（cpp job）：`ShutdownHookManager$2` 在 JVM 退出钩子阶段 `NoClassDefFoundError`（exec-maven-plugin 的 ClassLoader 已失效），导致所有 shutdown hook 未执行；`shutdownMiniCluster` 只 `stop(EXIT_IMMEDIATELY)` + `sleep(5000)` + 粗暴 interrupt，对战 97 个/测试的驻留线程（`LocalFlusher` 48、`worker-metrics-cleaner` 17 等）无效 → 873 条 linger WARNING + 每测试卡 ~15s，9 个测试累计 ~2.5 分钟。修复优先级：stop 路径对所有 metrics source 调 `destroy()` → 测试改 `exec:exec`（fork JVM，证据 A/B 一并消失）→ 补齐组件关闭。

### 5.3 Spark 4.2 job：44 分钟花在哪

| 阶段 | 耗时 | 占比 |
|---|---|---|
| checkout + setup-java | 15s | <1% |
| 下载 Maven 3.9.16 | 1m49s | 4% |
| `mvn clean install -DskipTests`（15 模块串行） | 6m32s | 15% |
| `mvn test`（spark-it 119 用例 + client-spark 单测） | 35m33s | 81% |

- **固定 sleep**：`MiniClusterFeature.scala` L123/L242/L303 三处 `Thread.sleep(5000)`（`setUpMaster` 随后还会轮询，5s 是重复付费；`workerNum=0` 也照睡），L304-308 还对已停 master 二次 stop worker → 107 次 `Fail report to master`。每个触集群 suite 保底 ~20s，22 个 suite × 两套矩阵 = 每次 CI 数分钟。
- **Leak 放大噪声**：`LifecycleManagerSuite.scala:111-127`（CELEBORN-1258 用例）从不 `stop()` 泄漏的 manager，其 heartbeater 在 master 停后持续发心跳 → 591×2 条 ERROR（约 15k 行，占日志 1/3）。根因是 `WithShuffleClientSuite.afterEach` 只 stop trait 字段，而用例用局部 `val` 遮蔽了它。
- **集群起停内联**：`ShuffleFallbackSuite.scala:32-38` 把 beforeAll/afterAll 置空，~20-30s 集群成本记在单个用例（136.5s）头上。
- 其余：`CelebornFetchFailureSuite` 6m27s 主要是每用例新建 SparkSession（优化空间有限）；`LifecycleManagerCommitFilesSuite` 110.6s 来自 `retryWait=10s`×多轮，测试 conf 可调小。

### 5.4 优化提案

| 优先级 | 提案 | 收益 | 验证 |
|---|---|---|---|
| **C1** | MiniClusterFeature 固定 sleep → 条件等待（`registered/initialized` 标志已存在）；`workerNum=0` 短路；shutdown 去掉二次 stop | 每个 suite 省 15-20s，单次 CI 省 5-7 分钟；顺带消 107 条 ERROR | 改前后对比 suite 耗时与 ERROR 行数 |
| **C2** | 修复 LifecycleManager 泄漏（用例补 `stop()` + `afterEach` 兜底） | 日志量 ↓~30%，ERROR 全部指向真实断言失败 | `AskSync HeartbeatFromApplication failed` 归零 |
| **C3** | 缓存 Maven 本体（`actions/cache` 缓存 `build/apache-maven-3.9.16/`，key 含 `maven.version`） | ~20 job × 25s~2min，累计 10-40 分钟；有 cb086f2ca 先例 | 日志不再出现 curl 进度条 |
| **C4** | 修 S3 e2e 从未执行：`hadoop-aws` 以 test scope 挂到 `tests/spark-it`，或给 master 的 aws profile 加 activation（测试命令没有 `-am`，`-Paws` 不作用于 reactor 外模块） | 正确性覆盖 | s3 用例从 CANCELED 变执行 |

次级（C5）：Maven `-T 1C` 并行构建（测试阶段保持串行）、`ShuffleFallbackSuite` 起停挪回 beforeAll/afterAll、UnregisterShuffle 120s 窗口收紧、CommitFilesSuite 调小 `CLIENT_COMMIT_FILE_REQUEST_RETRY_WAIT`。

**不建议提案**：pom 的 `artifactId/version contains an expression` WARNING（Scala 交叉构建通行做法，社区大概率 wontfix）。

---

## 6. 统一提交顺序

1. **T1** ThreadUtils 真 bug（半天、单测直白、零评审风险）
2. **Metrics PR-1** M1+M3+M2（3 个计数 bug + 单测）
3. **Auth PR-2 → PR-1**（先 replicate SASL，再修 checkAuth fail-open）
4. **CI C2** LifecycleManager 泄漏（几十行测试改动，消 1/3 日志噪声）
5. **CI C3** Maven 缓存（workflow + 10 行）
6. **T3+T4** auth init 校验 + 过时 TODO 清理
7. **C1** MiniCluster 条件等待（收益最大，需防 flaky）
8. **C4 / M15 / A8** 正确性与设计级问题：先发 JIRA / dev@ 讨论

每个 PR 遵循：CELEBORN-XXXX Jira → `./dev/reformat` → 相关模块单测 → 配置变更时 `UPDATE=1` 重跑 `ConfigurationSuite`。

---

## 7. 维护约定

本文件承接原 CI 日志台账。后续分析新 CI 日志时：日志索引加一行；新问题编 P12 起并在表中标注来源 job；老问题复现则在该行追加证据；预期内 ERROR 归入第 5.1 节的噪声统计，不再单列附录。
