# Celeborn auth/SASL 模块调研报告 —— 可提交社区的优化点

> 基线：外部工作树 `celeborn-main-1`（HEAD `626a07cfc` = `origin/main`）｜ 整理：2026-10 ｜ 状态：纯调研，未改代码 ｜ 关联待办：[TODOList.md §2.3](TODOList.md)

## Context

目标：在 `celeborn-main-1`（HEAD `626a07cfc` = `origin/main`）的 auth/SASL 模块里找出**真实存在、可复现、可独立成 PR** 的改进点，用于向 Apache Celeborn 社区提交。

结论：这个模块**从 Spark 的 `org.apache.spark.network.sasl` 移植过来后基本停在"能跑通 happy path"的状态**（CELEBORN-1157/1176/1212/1229/1251/1360），留下了大量 Spark 自己也没做完的东西（TODO、`(?)` 注释、死字段），并引入了几个 Celeborn 特有的缺口（replicate 端口没接 SASL、worker 侧 secret 永不清除）。下面 20+ 个发现里**至少 8 个可以独立成 PR**，其中 3 个社区大概率会高优先级接受。

交付形式：仅调研报告，不含代码改动。

---

## 模块地图

```
common/src/main/java/org/apache/celeborn/common/network/sasl/
  ├─ SaslClientBootstrap / SaslServerBootstrap      # 传输层握手入口
  ├─ CelebornSaslClient  / CelebornSaslServer      # JDK SASL 包装 + CallbackHandler
  ├─ SaslRpcHandler      / AbstractAuthRpcHandler  # 服务端状态机
  ├─ SaslUtils / SaslCredentials / SaslTimeoutException
  ├─ SecretRegistry(+Impl)
  ├─ anonymous/*                                    # ANONYMOUS 机制（"客户端认证"）
  └─ registration/*                                 # 应用注册 + secret 下发
worker/.../WorkerSecretRegistryImpl.java            # worker 侧 Guava 缓存 + 回源 master
master/.../MasterSecretRegistryImpl.java            # master 侧，落 Ratis
common/.../server/BaseMessageHandler.java:50        # checkAuth（Java 版）
common/.../rpc/RpcEndpoint.scala:149                # checkAuth（Scala 版，重复实现）
```

---

## A. 安全正确性缺陷

### A1 ✅ checkAuth 在 clientId == null 时完全 fail-open（最高价值）

```java
// common/src/main/java/org/apache/celeborn/common/network/server/BaseMessageHandler.java:50
protected void checkAuth(TransportClient client, String appId) {
  if (client.getClientId() != null && !client.getClientId().equals(appId)) {
    throw new IllegalStateException(...);
  }
}
```

这不是"弱校验"，是**条件反转**：有 ID 才校验，没 ID 直接放行。

- 这是整个系统**唯一的按应用鉴权模型**，被 17 处调用：`Master.scala` 11 处、`PushDataHandler.java:995`、`FetchHandler.scala:151/367`、`Controller.scala:118/149/167`。
- 只要 `celeborn.auth.enabled=false`（默认），`clientId` 永远是 null → 所有 shuffle 操作的鉴权退化成 no-op。
- Scala 版 `RpcEndpoint.scala:149` 是**独立复制**的第二份实现（`!=` vs `.equals`），两份已经漂移。

> 注意：直接改成 fail-closed 会**打断 replication**（见 A2），必须先做 A2 再做 A1。

### A2 ✅ Worker replicate 端口完全没有 SASL

```scala
// worker/src/main/scala/org/apache/celeborn/service/deploy/worker/Worker.scala
:230  transportContext.createServer(conf.workerPushPort,      getServerBootstraps(transportConf))  // 有
:251  transportContext.createServer(conf.workerReplicatePort)                                       // 无！
:252  transportContext.createClientFactory()                                                        // 无！
:272  transportContext.createServer(conf.workerFetchPort,     getServerBootstraps(transportConf))  // 有
```

`TransportContext.java:158` 的一参 `createServer(int)` 走的是 `Collections.emptyList()`。`getServerBootstraps`（`Worker.scala:1146`）明明就在手边，replicate 这行漏传了。

后果：任何能连到 `celeborn.worker.replicate.port` 的主机，可以以任意 appId 推/复制数据 —— 且因为 A1，`PushDataHandler:995` 的 checkAuth 对此静默放行。
修复需**成对**做：replicate server 加 `SaslServerBootstrap` + replicate client factory 加对应 client bootstrap。

### A3 ⚠️ 服务端 SASL 握手既无超时、也无次数上限

`SaslRpcHandler.java` 里 `conf`（:51）和 `channel`（:54）两个字段**赋值后从未被读取**（单测 `CelebornSaslSuiteJ.java:117` 直接传 `new SaslRpcHandler(null, null, handler, null)` 都没炸，是死字段的铁证）—— "服务端超时被设计过但没实现"的痕迹。

- `doAuthChallenge`（:81-108）：`if (saslServer == null || !saslServer.isComplete())` 可无限重入，无失败计数、无锁退、失败后不关连接。
- `TransportChannelHandler.userEventTriggered`（:195）只在 `closeIdleConnections=true` 时才真关；握手卡住时服务端没有 outstanding request，:185 的 `hasOutstandingRequests()` 为 false，**连日志都不打**。
- 客户端 `SaslClientBootstrap.java:83` `while (!saslClient.isComplete())` 同样无轮次上限、无总预算。

### A4 ✅ SaslTimeoutException 是不可达死代码

```java
// SaslClientBootstrap.java:94-103
try { response = client.sendRpcSync(msg.toByteBuffer(), conf.saslTimeoutMs()); }
catch (RuntimeException ex) {
  if (ex.getCause() instanceof TimeoutException) throw new SaslTimeoutException(ex.getCause());
```

`TransportClient.sendRpcSync`（`TransportClient.java:280-284`）是 `catch (Exception e) { throw new IOException(..., e) }`。`TimeoutException` 永远以 `IOException` 形式到达外层 :109，**这段 catch 永不触发**。全仓库 `SaslTimeoutException` 5 处 throw、0 处 catch、0 处测试；`RegistrationClientBootstrap` 还有 3 处同样的死代码。

顺带：`catch (Exception e)` 把 `InterruptedException` 也吞了，且**不恢复中断标志**（对比 `TransportClientFactory.retryCreateClient` 是特意处理了的）。

### A5 ⚠️ SASL 回调里同步阻塞的 master RPC，跑在 Netty event loop 上

`CelebornSaslServer.java:140` → `WorkerSecretRegistryImpl.getSecretKey` → Guava cache miss → `masterClient.askSync(...)`（`WorkerSecretRegistryImpl.java:60`）。

- 未缓存的 appId 每次都**同步阻塞一次网络往返**，直接卡住 worker 的整个 event loop。
- cache（:74）**没有** `expireAfterWrite`，也没有 negative caching：不存在的 appId 每次都回源。
- 未认证攻击者用随机 appId 刷，可同时做到：卡 IO 线程 + 打 master + 把 10000 条 LRU 缓存里的合法条目挤掉。

### A6 ⚠️ secret 全生命周期明文

- `SaslUtils.encodePassword`（:67）只是 Base64，**不是哈希**。`SecretRegistry` / `MasterSecretRegistryImpl`（落 Ratis 日志 + 快照文件）/ `WorkerSecretRegistryImpl` 存的都是明文 shared secret。
- 注册请求 `RegistrationClientBootstrap.java:196` 明文带 secret；master→worker 的 `PbApplicationMeta` 也是明文。`docs/security.md:86` 只承认了"注册时明文"，没说 master 内部链路也明文。
- `RegistrationRpcHandler:236-250` **接受客户端自带的 secret**，不校验长度/熵，空串也收。

### A7 ⚠️ appId 存在性 oracle + 完整堆栈回传

`CelebornSaslServer.java:141` `throw new RuntimeException("Registration information not found for " + userName)`：区分了"未注册"和"密码错"；配合 `TransportRequestHandler.java:134` / `RegistrationRpcHandler.java:129` 的 `Throwables.getStackTraceAsString(e)`，把完整服务端堆栈发给未认证对端。

### A8 ⚠️ 应用结束后 worker 侧 secret 永不清除

- `SecretRegistry.unregister` 全仓库**只有** `Worker.scala:849` 一处生产调用，且被 `!workerInfo.getApplicationIdSet.contains(applicationId)` 门控。
- `MasterSecretRegistryImpl.unregister`（:52）**无任何生产调用方** —— 定义好但没接。
- `Master.handleApplicationLost` 清了 `workersAssignedToApp` / Ratis 里的 `applicationMetas`，但**不通知 worker 丢 secret**。
- 协议层也没有 `APPLICATION_META_REMOVED` 消息（`TransportMessages.proto:94-102`）。

### A9 ⚠️ 未认证注册 = appId 抢占

registration 的"client auth"是 **ANONYMOUS**（`AnonymousSaslServerFactory.java:76-83` 无条件 `isCompleted = true` 返回空字节），即注册这一步**不建立任何身份**。任何能访问 master RPC 端口的人都能抢注 appId，之后真正的应用来注册会撞 `isRegistered` 被拒。

---

## B. 可观测性 + 测试补全

### B1 ✅ 零 auth 指标

全仓库搜不到 auth 相关 counter；`metrics/source/` 下没有 `AuthSource`，`WorkerSource` / `MasterSource` 也没有任何 auth 计数：

| 缺失 | 说明 |
|---|---|
| auth 成功/失败计数 | 目前只有 `SaslRpcHandler.java:103` 一行 `logger.debug`（成功），**失败没有任何专门日志** |
| `unauthenticatedConnectionAccepted` | A1 的 fail-open 完全不可见 |
| 握手超时计数 | `SaslTimeoutException` 被抛 5 次、被 catch 0 次 |
| secret 读取审计 | `Master.scala:1376 handleRequestForApplicationMeta` 返回任意 app 的 secret，只有 `logDebug` |

### B2 ⚠️ 15 项缺失测试

现有测试只有 happy path：`CelebornSaslSuiteJ`（5 个）、`RegistrationSuiteJ`（3 个）、`WorkerSecretRegistryImplSuiteJ`（4 个）。

**明确缺失：**
1. 真实 socket 上错误 secret → 连接被关闭
2. 未知 appId / 无 secret
3. secret 被 revoke 后失效
4. SASL 握手超时能产生 `SaslTimeoutException`
5. 客户端开了 auth、服务端没开
6. 服务端开了 auth、客户端没开
7. `validateServerResponse` 忽略 `getAuthEnabled()`（`RegistrationClientBootstrap.java:239`）
8. 握手轮次上限
9. 并发/线程安全
10. **`checkAuth` 在 `clientId == null` 时的行为**（最吃重的安全检查，居然没测）
11. replicate 端口绕过 auth
12. `AUTH_ENABLED` 无 `INTERNAL_PORT_ENABLED` 时抛异常
13. `SecretRegistryImpl` 重复注册
14. `celeborn.auth.enabled=true` 下端到端 push/fetch
15. `SslConnectivitySuiteJ.java:190` 的 `testNormalConnectivityWithAuthWorks` 其实是**空转的**：设了 `auth.enabled=true` 但没装任何 bootstrap，`TransportConf.authEnabled()` 根本没被调用

---

## C. 文档 + 配置治理

### C1 ✅ auth / security 两个配置分类没有任何文档页 —— 7 个配置项是孤儿

`CelebornConf.scala:6710` 标了 `.categories("auth")`，`:6719-6775` 有 6 个 `.categories("security")`。但：

- `docs/configuration/` 只有 client / columnar-shuffle / ha / index / master / metrics / network / network-module / quota / worker —— **没有 auth.md、没有 security.md**
- `ConfigurationSuite.scala:56-85` 也只生成 8 个分类，**没有 auth / security 的 test**
- `docs/configuration/index.md` 也没有对应 section

**受影响：** `celeborn.auth.enabled` + `celeborn.serializer.deserialization.filter.{enabled, allowedPackages, maxDepth, maxArrayLength, maxReferences, maxStreamBytes}` —— 全部"已声明 + 已读取 + 零文档"。

### C2 ✅ networkIoSaslTimoutMs 拼写错误 + 无校验

```scala
// CelebornConf.scala:652
def networkIoSaslTimoutMs(module: String): Int = {   // Timout → Timeout
  getTransportConfTimeAsMs(module, NETWORK_IO_SASL_TIMEOUT).toInt   // Long→Int 静默截断
}
```
`NETWORK_IO_SASL_TIMEOUT`（:6689）没有 `checkValue`，配 0 或负数会静默关掉超时；`TransportConf.java:167` 继承了这个 typo 和截断。

### C3 ⚠️ docs/security.md 的关键缺口

- 没说 **SASL 只做认证**（`SaslUtils.java:39-51` 硬编码 `QOP=auth`，`wrap/unwrap` 全仓库无调用方）→ 握手后的 shuffle 数据**明文且无完整性保护**
- 没说**注册本身是未认证的**（A9）
- 没说 **LifecycleManager 的 RPC 端口无认证地分发 app secret**（`LifecycleManager.scala:540` 的 `PbApplicationMetaRequest` 分支**没有 checkAuth**；`:186` 建 rpcEnv 时也没传 `RpcSecurityContext`）
- 没提 DIGEST-MD5 是 RFC 6331 Historic 机制、没提 secret 生命周期/轮换、没提 master→worker 明文传播
- 配置表（:90-95）漏了 `saslTimeout`、`worker.applicationRegistry.cache.size`、`send.applicationMeta.threads`、全部 `*.http.auth.*`、`redaction.regex`

### C4 ⚠️ 其他零散项

- `CelebornConf.scala:3696 / 3705`（worker 的 http auth provider）注释里写的类名是 `org.apache.celeborn.common.authentication.*`，但该 SPI 已迁到 `org.apache.celeborn.spi.authentication.*` —— **文档指向一个不存在的类**（master 侧 :2712 是对的）
- `conf/celeborn-defaults.conf.template` 里完全没有 auth 条目；CLI 也没有对应 option（对比 SSL 有完整 CLI 支持）
- `AuthenticationFilter.scala:70` 的 `bypassApiPaths` 可配置绕过列表未在文档中提示风险

---

## D. 协议增强

- **D1 SASL mechanism 可配置（CELEBORN-1158，仍 open）**：`SaslClientBootstrap.java:72`、`CelebornSaslClient.java:46-48`、`SaslRpcHandler.java:95`（硬编码 DIGEST-MD5 且**从不读** `saslMessage.getMethod()`）、`RegistrationRpcHandler.java:62` / `RegistrationClientBootstrap.java:74`（硬编码 mechanism 列表，各有同一个 TODO）
- **D2 QOP 可配置**：`SaslUtils.java:39-51` 钉死 `auth`；若要支持 `auth-int`/`auth-conf` 还需设 `Sasl.MAX_BUFFER`（当前依赖默认 65536）并把 `wrap/unwrap` 接进 framer
- **D3 secret TTL / 轮换**：`SecretRegistry`(:20-36) 接口里**没有** TTL/过期/轮换概念；`WorkerSecretRegistryImpl:74` 缺 `expireAfterWrite`
- **D4 `Sasl.SERVER_AUTH` 放错位置**：`SaslUtils.java:49` 把它放进了 **server** props，但 JDK 文档说它是**客户端侧**属性（"server 是否必须向 client 认证"）—— 至少是无效配置，至多错误暗示了双向认证
- **D5 `DEFAULT_REALM = "default"`**（`SaslUtils.java:42`）硬编码，所有集群共用，digest 的域分隔失效
- **D6 `decodeIdentifier` 用宽松 Base64**（`SaslUtils.java:59-64`）：`Base64.getDecoder()` 会丢弃非法字符，多个不同输入可解码出同一个 `userName`；空串也被接受

---

## 候选 PR 清单（按性价比排序）

| # | PR 标题（建议） | 涉及文件 | 体量 | 为什么社区会收 |
|---|---|---|---|---|
| 1 | **Fix checkAuth fail-open when clientId is null** | `BaseMessageHandler.java:50`、`RpcEndpoint.scala:149` | 小（**但必须先做 2**） | 整个按应用鉴权模型的根；17 处调用点 |
| 2 | **Enable SASL on the worker replicate port** | `Worker.scala:251-252`、`:1146` | 小 | 一行漏传 `getServerBootstraps`，端到端可复现的鉴权绕过 |
| 3 | **Add auth metrics + failure logging** | 新增 auth counter、`SaslRpcHandler:81-108`、`AbstractAuthRpcHandler:37` | 小-中 | 纯增量、零兼容性风险，运维刚需 |
| 4 | **Add docs/configuration/auth.md + security.md** | `ConfigurationSuite.scala`、`docs/configuration/index.md`、两个新 md | 小 | 一次性消除 7 个孤儿配置，CI 可守 |
| 5 | **Add server-side SASL negotiation timeout** | `SaslRpcHandler:51/54`（现成死字段）、`TransportChannelHandler:185-199` | 中 | 死字段本身就是"该做没做"的证据，好讲 |
| 6 | **Make SaslTimeoutException reachable / delete it** | `TransportClient.java:280-284`、`SaslClientBootstrap:94-103`、`RegistrationClientBootstrap` 3 处 | 小 | 要么修要么删，附带恢复中断标志 |
| 7 | **Purge worker secrets on ApplicationLost** | `TransportMessages.proto`（新消息）、`Master.scala:1216`、`worker/InternalRpcEndpoint.scala:41`、`Worker.scala:849`、`MasterSecretRegistryImpl:52` | 中 | secret 生命周期闭环，需改协议，建议先发 JIRA 对齐 |
| 8 | **Auth test suite completion** | `common/src/test/.../sasl/`、`worker/src/test/...` | 中 | 15 项，可拆成 2-3 个 PR；第 10 项（checkAuth null）最该先补 |
| 9 | **Harden registration: validate secret, remove existence oracle** | `RegistrationRpcHandler:236-250`、`CelebornSaslServer:141` | 中 | 安全语义改动，需讨论 |
| 10 | **Configurable SASL mechanism（CELEBORN-1158）** | `SaslUtils`、`SaslRpcHandler`、`SaslClientBootstrap`、`Registration*` | 大 | JIRA 已存在、已挂 TODO，社区认但工作量大 |
| 11 | **Move secret lookup off the Netty event loop + negative caching** | `WorkerSecretRegistryImpl:54-74`、`CelebornSaslServer:140` | 中-大 | 涉及 SASL SPI 异步化，架构改动，先发 JIRA |

**不建议直接提的：**
- 把 `QOP` 默认提到 `auth-int` —— 需要协议层改动（`maxbuffer`、`wrap/unwrap` 接 framer），且线上混版本会直接握手失败。建议先做"可配置 + 默认不变"。
- 把 secret 改成 KDF/哈希存储 —— 破坏 master/worker/client 三端兼容性，属于 CIP 级别改动。

---

## 核实记录

**直接读代码确认（✅）**：`BaseMessageHandler:50`、`RpcEndpoint.scala:149`、`Worker.scala:230/251/252/272/1146`、`TransportContext.java:158-160`、`SaslRpcHandler.java:50-130`、`TransportClient.java:254-285`、`SaslClientBootstrap.java:60-120`、`SaslUtils.java:28-77`、`WorkerSecretRegistryImpl.java`（全文）、`docs/configuration/` 目录 + `index.md`、`ConfigurationSuite.scala:56-85`、`CelebornConf.scala:652/6689/6710/6719-6775`、`docs/security.md:79-96`、`unregister` 全仓库调用点、`checkAuth` 全部 17 处调用点、`git log` on `network/sasl/`。

**子 agent 报告、未逐行复核（⚠️）**：A5/A6/A7/A8/A9、B2、C3/C4 部分条目、D1-D6。动工前需再核一遍。

## 下一步

本报告不含代码改动。若后续要挑几条落地，建议顺序 **2 → 1 → 3**（先补 replicate 的 SASL，否则修 checkAuth 会打断 replication），每个独立成 PR。