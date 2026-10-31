# Celeborn CLI 与 REST API 暴露面综合分析

> 基于版本:**0.7.0-SNAPSHOT**;整理日期:**2026-08-05**。
> 本文由以下三份文档合并去重而成,并对照当前代码重新核实了所有类名/路径/行号:
> - `cli-module-overview.md`(picocli + OpenAPI codegen 架构、命令分发、命令速查)
> - `cli-comparison.md`(Celeborn vs Uniffle CLI 代码级对比、REST 能力对照、缺陷与增强方案)
> - `openapi-rest-api-audit.md`(yaml 一致性审计 + RPC→REST→CLI 三层暴露缺口分析)
>
> 行号以当前工作树为准。Uniffle 侧证据来自外部基线工作树 `uniffle-master` 的快照,本次合并未重新核对该仓库,仅在第七节引用。

---

## 一、模块定位与架构

`cli/` 是 Apache Celeborn 的命令行管理工具,基于 **picocli** 构建,通过 **OpenAPI 生成的 REST 客户端** 与 Master/Worker 的 HTTP 服务通信。它不提供原生 RPC 通道,所有操作都依赖 REST API。构建产物:`celeborn-cli_${scala.binary.version}`(当前 Scala 2.12)。

### 1.1 spec-first 的 OpenAPI 体系

仓库的 openapi 模块采用 **spec-first** 设计:

- `openapi/openapi-client/src/main/openapi3/{master,worker}_rest_v1.yaml` 是手写的"规范源文件";
- 由 `openapi-generator-maven-plugin`(7.8.0,配置见 `openapi/openapi-client/pom.xml:205`,library `apache-httpclient` 见 `:238`)生成 Java 客户端 SDK,**生成代码提交进 git**,供 `cli` 模块与 `service` 模块(仅复用 model DTO)消费;
- 服务端 v1 端点通过 **JAX-RS 注解**(`@Path/@GET/@POST` + swagger `@Operation/@ApiResponse`)在运行时动态生成 `/openapi.json`、`/openapi.yaml`。

因为存在「手写 yaml」与「服务端注解动态生成」**两套独立来源**,它们之间的不一致不会被编译器捕获(见第五节审计发现)。

生成 client 的 package:

- Master:`org.apache.celeborn.rest.v1.master.*`,8 个 Api 类(`ApplicationApi`/`ConfApi`/`DefaultApi`/`LoggerApi`/`MasterApi`/`RatisApi`/`ShuffleApi`/`WorkerApi`);
- Worker:`org.apache.celeborn.rest.v1.worker.*`,6 个 Api 类(无 `MasterApi`/`RatisApi`)。

### 1.2 核心类与职责

| 类 | 路径(`cli/src/main/scala/org/apache/celeborn/cli/`) | 职责 |
|----|------|------|
| `CelebornCli.scala` | `CelebornCli.scala` | 入口类;声明 `master` / `worker` 两个子命令 |
| `BaseCommand.scala` | `common/BaseCommand.scala` | 命令基类;upsert/delete 动态配置的公共辅助方法 |
| `CommonOptions.scala` | `common/CommonOptions.scala` | 共享 `@Mixin`:hostport、cluster alias、worker-ids、apps、auth header、动态配置参数等 |
| `CliConfigManager.scala` | `config/CliConfigManager.scala` | 持久化集群别名到 `~/.celeborn-cli.conf`(Java Properties) |
| `CliLogging.scala` / `CliVersionProvider.scala` | `common/` | 控制台日志与 `--version` 输出 |
| `MasterSubcommand.scala` / `MasterSubcommandImpl.scala` | `master/` | Master 子命令 trait 与实现;构建 OpenAPI client,调用 Master REST API |
| `MasterOptions.scala` | `master/MasterOptions.scala` | Master 子命令的 `@ArgGroup` 互斥 flag(28 个 `@Option`) |
| `ReviseLostShuffleOptions.scala` | `master/ReviseLostShuffleOptions.scala` | `--revise-lost-shuffles` 的参数组 |
| `WorkerSubcommand.scala` / `WorkerSubcommandImpl.scala` | `worker/` | Worker 子命令 trait 与实现 |
| `WorkerOptions.scala` | `worker/WorkerOptions.scala` | Worker 子命令的 `@ArgGroup` 互斥 flag(15 个 `@Option`) |

---

## 二、命令解析与分发流程

### 2.1 入口与顶层路由

`CelebornCli.scala:38-44`:

```scala
object CelebornCli {
  def main(args: Array[String]): Unit = {
    val cmd = new CommandLine(new CelebornCli())
    cmd.setOptionsCaseInsensitive(false)
    cmd.setSubcommandsCaseInsensitive(false)
    new CommandLine(new CelebornCli()).execute(args: _*)
  }
}
```

注意:这里创建了**两个** `CommandLine` 实例,设置了 case-insensitive 的 `cmd` 被丢弃,实际执行的是新实例(缺陷,见第六节)。

`CelebornCli` 注解声明两个子命令(`CelebornCli.scala:25-30`),picocli 根据第一个位置参数分发到 `master` 或 `worker`。

### 2.2 子命令内部 dispatch:flag-based

每个子命令使用 **flag-based dispatch**,而非嵌套子子命令:

```bash
celeborn-cli master --show-workers
celeborn-cli worker --show-worker-info
```

`MasterSubcommand.scala:37-38` / `WorkerSubcommand.scala:34-35`:

```scala
@ArgGroup(exclusive = true, multiplicity = "1")
private[master] var masterOptions: MasterOptions = _
```

`exclusive=true` + `multiplicity="1"` 表示**互斥且必须恰好指定一个**动作;公共选项通过 `@Mixin commonOptions: CommonOptions` 复用(`MasterSubcommand.scala:43-44`、`WorkerSubcommand.scala:37-38`)。

`MasterSubcommandImpl.run()`(`:33-66`)与 `WorkerSubcommandImpl.run()`(`:28-44`)是一串**顺序 `if`** 判断(非 if/else 链),每个 flag 对应一个私有方法。互斥保证主要依赖 picocli 解析期校验,而非 run 逻辑本身。

### 2.3 与 Master / Worker 的交互

CLI **不直接使用 Celeborn RPC**,全部通过生成的 Java client 走 HTTP(`MasterSubcommand.scala:50-72`):

```scala
private def apiClient = new ApiClient().setBasePath(s"http://$connectionUrl")
private[master] def workerApi = new WorkerApi(apiClient)
```

**连接 URL 解析**:
- Master:优先 `--hostport`;否则从 `~/.celeborn-cli.conf` 查 `--cluster` 别名,取**第一个** host(`MasterSubcommand.scala:56` 的 `split(",")(0)`,无 failover)。
- Worker:**只支持 `--hostport`**(`WorkerSubcommand.scala:43-52`);`--cluster` 在 help 中显示但未被使用,缺失 `--hostport` 时直接抛 `ParameterException`。

**认证**:`--auth-header`(`CommonOptions.scala:96-101`)经 `getAuthHeader`(`:103-109`)包装为 `Authorization` header 传给每个 REST 调用,兼容 `Bearer <token>` / `Basic <base64>`。服务端 `AuthenticationFilter.scala` 支持 NEGOTIATE(SPNEGO)/BASIC/BEARER,写操作(POST/PUT/DELETE)要求调用方在 `*_HTTP_AUTH_ADMINISTERS` 集合内。注意 OpenAPI spec 顶层只声明了 `security: - basic: []`(`master_rest_v1.yaml:21-22`),与服务端实际支持范围不一致。

**CLI 相关配置**(复用服务端通用配置,CLI 无独立命名空间):

| 配置键 | 默认值 | 说明 |
|--------|--------|------|
| `celeborn.master.http.host` / `celeborn.master.http.port` | `<localhost>` / `9098` | Master REST 端点(`CelebornConf.scala:2608`) |
| `celeborn.worker.http.host` / `celeborn.worker.http.port` | `<localhost>` / `9096` | Worker REST 端点(`CelebornConf.scala:3548`) |
| `celeborn.http.auth.bypass.api.paths` | `[]` | 免认证路径(CLI 端无需处理) |

---

## 三、命令 / REST 端点全对照表

> 基于 `service/`、`master/`、`worker/` 资源类与 openapi yaml 逐条核对。yaml 规模:master **29 个路径 / 32 个操作**,worker **13 个路径 / 14 个操作**。

### 3.1 Master 子命令已暴露(26 个 REST 动作 + 2 个本地动作)

| CLI 选项 | client 方法 | REST 路径 | yaml 行号 |
|---|---|---|---|
| `--show-masters-info` | `masterApi.getMasterGroupInfo` | `GET /api/v1/masters` | `master_rest_v1.yaml:174` |
| `--show-cluster-apps` | `applicationApi.getApplications` | `GET /api/v1/applications` | `:316` |
| `--show-cluster-apps-info` | `applicationApi.getApplicationsInfo` | `GET /api/v1/applications/info` | `:330` |
| `--show-cluster-shuffles` | `shuffleApi.getShuffles` | `GET /api/v1/shuffles` | `:160` |
| `--exclude-worker` | `workerApi.excludeWorker`(add) | `POST /api/v1/workers/exclude` | `:208` |
| `--remove-excluded-worker` | `workerApi.excludeWorker`(remove) | `POST /api/v1/workers/exclude` | `:208` |
| `--remove-workers-unavailable-info` | `workerApi.removeWorkersUnavailableInfo` | `POST /api/v1/workers/remove_unavailable` | `:229` |
| `--send-worker-event` | `workerApi.sendWorkerEvent` | `POST /api/v1/workers/events` | `:269` |
| `--show-worker-event-info` | `workerApi.getWorkerEvents` | `GET /api/v1/workers/events` | `:269` |
| `--show-lost-workers` | `workerApi.getWorkers`(lostWorkers) | `GET /api/v1/workers` | `:192` |
| `--show-excluded-workers` | `workerApi.getWorkers`(excludedWorkers) | `GET /api/v1/workers` | `:192` |
| `--show-manual-excluded-workers` | `workerApi.getWorkers`(manualExcludedWorkers) | `GET /api/v1/workers` | `:192` |
| `--show-shutdown-workers` | `workerApi.getWorkers`(shutdownWorkers) | `GET /api/v1/workers` | `:192` |
| `--show-decommissioning-workers` | `workerApi.getWorkers`(decommissioningWorkers) | `GET /api/v1/workers` | `:192` |
| `--show-lifecycle-managers` | `applicationApi.getApplicationHostNames` | `GET /api/v1/applications/hostnames` | `:363` |
| `--show-workers` | `workerApi.getWorkers` | `GET /api/v1/workers` | `:192` |
| `--show-workers-topology` | `workerApi.getWorkersTopology` | `GET /api/v1/workers/topology` | `:302` |
| `--show-conf` | `confApi.getConf` | `GET /api/v1/conf` | `:30` |
| `--show-dynamic-conf` | `confApi.getDynamicConf` | `GET /api/v1/conf/dynamic` | `:56` |
| `--upsert-dynamic-conf` | `confApi.upsertDynamicConf` | `POST /api/v1/conf/dynamic/upsert` | `:96` |
| `--delete-dynamic-conf` | `confApi.deleteDynamicConf` | `POST /api/v1/conf/dynamic/delete` | `:122` |
| `--show-thread-dump` | `defaultApi.getThreadDump` | `GET /api/v1/thread_dump` | `:148` |
| `--show-container-info` | `defaultApi.getContainerInfo` | `GET /api/v1/container_info` | `:44` |
| `--revise-lost-shuffles` | `applicationApi.reviseLostShuffles` | `POST /api/v1/applications/revise_lost_shuffles` | `:377` |
| `--delete-apps` | `applicationApi.deleteApps` | `POST /api/v1/applications/delete_apps` | `:344` |
| `--update-interruption-notices` | `workerApi.updateInterruptionNotice` | `PUT /api/v1/workers/updateInterruptionNotice` | `:249` |
| `--add-cluster-alias` / `--remove-cluster-alias` | 本地 `CliConfigManager`(非 REST) | — | — |

### 3.2 Worker 子命令已暴露(15 个动作)

| CLI 选项 | client 方法 | REST 路径 | yaml 行号 |
|---|---|---|---|
| `--show-worker-info` | `workerApi.getWorkerInfo` | `GET /api/v1/workers` | `worker_rest_v1.yaml:191` |
| `--show-apps-on-worker` | `applicationApi.getApplicationList` | `GET /api/v1/applications` | `:241` |
| `--show-shuffles-on-worker` | `shuffleApi.getShuffles` | `GET /api/v1/shuffles` | `:160` |
| `--show-partition-location-info` | `shuffleApi.getShufflePartitions` | `GET /api/v1/shuffles/partitions` | `:176` |
| `--show-unavailable-peers` | `workerApi.unavailablePeers` | `GET /api/v1/workers/unavailable_peers` | `:205` |
| `--is-shutdown` | `workerApi.getWorkerInfo`(isShutdown) | `GET /api/v1/workers` | `:191` |
| `--is-decommissioning` | `workerApi.getWorkerInfo`(isDecommissioning) | `GET /api/v1/workers` | `:191` |
| `--is-registered` | `workerApi.getWorkerInfo`(isRegistered) | `GET /api/v1/workers` | `:191` |
| `--exit` | `workerApi.workerExit` | `POST /api/v1/workers/exit` | `:220` |
| `--show-conf` | `confApi.getConf` | `GET /api/v1/conf` | `:30` |
| `--show-dynamic-conf` | `confApi.getDynamicConf` | `GET /api/v1/conf/dynamic` | `:56` |
| `--upsert-dynamic-conf` | `confApi.upsertDynamicConf` | `POST /api/v1/conf/dynamic/upsert` | `:96` |
| `--delete-dynamic-conf` | `confApi.deleteDynamicConf` | `POST /api/v1/conf/dynamic/delete` | `:122` |
| `--show-thread-dump` | `defaultApi.getThreadDump` | `GET /api/v1/thread_dump` | `:148` |
| `--show-container-info` | `defaultApi.getContainerInfo` | `GET /api/v1/container_info` | `:44` |

### 3.3 已生成但 CLI 未暴露(RatisApi / LoggerApi / metrics)

| 能力 | REST 路径 | 生成 client | CLI(master/worker) |
|---|---|---|---|
| ratis leader 转移 | `POST /api/v1/ratis/election/transfer` | ✓ `RatisApi` | ✗ |
| ratis leader 下台 | `POST /api/v1/ratis/election/step_down` | ✓ `RatisApi` | ✗ |
| ratis 选举暂停 | `POST /api/v1/ratis/election/pause` | ✓ `RatisApi` | ✗ |
| ratis 选举恢复 | `POST /api/v1/ratis/election/resume` | ✓ `RatisApi` | ✗ |
| ratis 加 peer | `POST /api/v1/ratis/peer/add` | ✓ `RatisApi` | ✗ |
| ratis 删 peer | `POST /api/v1/ratis/peer/remove` | ✓ `RatisApi` | ✗ |
| ratis 设 peer 优先级 | `POST /api/v1/ratis/peer/set_priority` | ✓ `RatisApi` | ✗ |
| ratis 触发 snapshot | `POST /api/v1/ratis/snapshot/create` | ✓ `RatisApi` | ✗ |
| 下载 raft-meta.conf | `GET /api/v1/ratis/local/raft_meta_conf` | ✓ `RatisApi` | ✗ |
| 生成 new-raft-meta.conf | `POST /api/v1/ratis/local/raft_meta_conf` | ✓ `RatisApi` | ✗ |
| 查 logger 级别 | `GET /api/v1/loggers` | ✓ `LoggerApi` | ✗ / ✗ |
| 设 logger 级别 | `POST /api/v1/loggers` | ✓ `LoggerApi` | ✗ / ✗ |
| metrics(prometheus) | `GET /metrics/prometheus` | ✗(未在 OpenAPI) | ✗ / ✗ |
| metrics(json) | `GET /metrics/json` | ✗(未在 OpenAPI) | ✗ / ✗ |

> `RatisApi` 是 master 侧已生成的 8 个 Api 之一,**9 个路径 / 10 个操作**全部未封装(`MasterSubcommand.scala` 未 import `RatisApi`)。`LoggerApi` 在 master 与 worker 侧都已生成,各 2 个操作全部未封装。Metrics 端点(路径由 `celeborn.metrics.prometheus.path` / `celeborn.metrics.json.path` 配置,默认 `/metrics/prometheus`、`/metrics/json`,见 `CelebornConf.scala:6044-6060`)不在 OpenAPI spec 内,未生成 client。

### 3.4 master vs worker 端点差异

- **master 独有**:`/api/v1/masters`、`/api/v1/workers/topology`、`/api/v1/workers/exclude`、`/api/v1/workers/remove_unavailable`、`/api/v1/workers/events`、`/api/v1/workers/updateInterruptionNotice`、`/api/v1/applications/info`、`/api/v1/applications/delete_apps`、`/api/v1/applications/hostnames`、`/api/v1/applications/revise_lost_shuffles`、ratis 全套 9 路径。
- **worker 独有**:`/api/v1/workers/exit`、`/api/v1/workers/unavailable_peers`、`/api/v1/shuffles/partitions`。
- **两端语义不同**:`GET /api/v1/workers`(master 返回集群 workers 列表 `WorkersResponse`;worker 返回自身信息 `WorkerInfoResponse`)、`GET /api/v1/applications`(master 返回集群 app 心跳 `ApplicationsHeartbeatResponse`;worker 返回本机 app id 列表 `ApplicationsResponse`)。

---

## 四、RPC → REST → CLI 三层暴露缺口

> 对 master RPC(`Master.receiveAndReply` 18 条,`Master.scala:462`;one-way `receive` 在 `:440`)、worker RPC(`Controller.receiveAndReply` 3 条,`Controller.scala:104`)、REST v1 端点(master 32 / worker 14 操作)、CLI 已封装端点(master 20 / worker 12)做三方差集。client 模块(LifecycleManager)不启动 HTTP 服务,无 REST 端点遗漏自该侧。

### 4.1 Master RPC 能力全清单

**Client(LifecycleManager)→ Master(9 条)**:

| RPC 消息 | 功能 | 返回 |
|---|---|---|
| `RegisterApplicationInfo` | 注册 app | OneWayMessageResponse |
| `HeartbeatFromApplication` | app 心跳,上报写入量/文件数,返回排除/下线 worker 列表 | HeartbeatFromApplicationResponse |
| `RequestSlots` | 为 shuffle 分区分配 primary/replica slot(核心高频) | RequestSlotsResponse |
| `PbBatchUnregisterShuffles` | 批量注销 shuffle 元数据 | PbBatchUnregisterShuffleResponse |
| `PbUnregisterShuffle` | 注销单个 shuffle | PbUnregisterShuffleResponse |
| `ApplicationLost` | app 结束清理(元数据/worker 分配/quota/DFS 目录) | ApplicationLostResponse |
| `CheckQuota` | 检查用户 quota 是否可用 | CheckQuotaResponse(isAvailable, reason) |
| `PbCheckWorkersAvailable` | 检查集群是否有可用 worker | PbCheckWorkersAvailableResponse(available) |
| `PbReviseLostShuffles` | 修正 master 丢失的 shuffle 登记 | PbReviseLostShufflesResponse |

**Worker → Master(9 条)**:

| RPC 消息 | 功能 | 返回 |
|---|---|---|
| `PbRegisterWorker` | worker 注册 | PbRegisterWorkerResponse |
| `HeartbeatFromWorker` | worker 心跳,返回过期 shuffle/事件 | HeartbeatFromWorkerResponse |
| `ReportWorkerUnavailable` | 上报其他 worker 不可用 | OneWayMessageResponse |
| `ReportWorkerDecommission` | 上报 decommission | OneWayMessageResponse |
| `PbWorkerExclude` | exclude 列表增删 | PbWorkerExcludeResponse |
| `PbWorkerLost` | 标记 worker 丢失 | PbWorkerLostResponse |
| `PbRemoveWorkersUnavailableInfo` | 清除 worker 不可用记录 | PbRemoveWorkersUnavailableInfoResponse |
| `PbWorkerEventRequest` | 触发 worker 事件(decommission/recycle) | PbWorkerEventResponse |
| `PbApplicationMetaRequest` | 获取 app 认证 secret | PbApplicationMeta |

**Master 内部定时消息**(self.send,5 条,非外部调用,**不暴露**):`PbCheckForWorkerTimeout`、`CheckForWorkerUnavailableInfoTimeout`、`CheckForApplicationTimeOut`、`CheckForDFSExpiredDirsTimeout`、`PbWorkerLost`(内部触发)。

### 4.2 RPC 与 REST 的复用关系(已被 REST 覆盖,不构成缺口)

经逐条 grep 核实 `handle*` 实现,以下 4 条 RPC 与 REST v1 端点**共享同一实现**:

| RPC | REST 端点 | 共享实现 |
|---|---|---|
| `PbWorkerExclude` | POST `/api/v1/workers/exclude` | `HttpService.exclude:173` → `Master.exclude:1460` |
| `PbWorkerEventRequest` | POST `/api/v1/workers/events` | `HttpService.handleWorkerEvent:188` → `Master.handleWorkerEvent:1339` |
| `PbRemoveWorkersUnavailableInfo` | POST `/api/v1/workers/remove_unavailable` | `Master.removeWorkersUnavailableInfo:1489` |
| `PbReviseLostShuffles` | POST `/api/v1/applications/revise_lost_shuffles` | `Master.reviseLostShuffles`(经 statusSystem) |

### 4.3 REST 真正缺失的 Master RPC(4 条)

**【强烈建议】`CheckQuota` → REST + CLI**
- RPC:`Master.scala:632` → `handleCheckQuota`(`:1274`),返回 `CheckQuotaResponse(isAvailable, reason)`。
- REST 缺失:master yaml 无 quota 查询端点;`QuotaManager` 有 `checkUserQuotaStatus`/`checkApplicationQuotaStatus` 等能力,REST 层完全未暴露。
- 价值:多租户场景下,运维/调度方常需查"某用户/租户 quota 是否触顶、剩余多少"。
- 建议端点:`GET /api/v1/quota?level=&tenant=&name=` 或 `/api/v1/quota/user/{tenantId}/{name}`、`/api/v1/quota/application/{appId}`。

**【建议】`PbCheckWorkersAvailable` → REST + CLI**
- RPC:`Master.scala:635` → `handleCheckWorkersAvailable`,返回 `available`(bool)。
- 价值:"集群是否还有可用 worker"这一快速判定无独立端点,可与健康检查一并作为集群可用性探针。
- 建议端点:`GET /api/v1/workers/available`(返回 `{available, count}`),或并入 `/api/v1/health`。

**【建议】`ApplicationLost` → CLI(REST 可选)**
- RPC:`Master.scala:555` → `handleApplicationLost:1148`(清理 app 元数据 + worker 分配 + quota + DFS 目录)。
- 注意:REST 现有 `POST /api/v1/applications/delete_apps` 调的是 `statusSystem.deleteApp`(只删 shuffle 资源),**与 `ApplicationLost` 不同**——后者是完整 app 收尾,`deleteApps` 不能替代。
- 价值:运维需要"强制清理某 app 在 master 侧的全部痕迹"(僵尸 app)。建议 CLI `master --application-lost <appId>`(走 RPC);REST 暴露需谨慎(误调会清掉在线 app 资源),可不做。

**【可选,低优】`PbWorkerLost` → CLI**
- REST 只能**查** lost workers,不能主动**标记** lost。已有 `exclude` + `sendWorkerEvent(DECOMMISSION)` 覆盖大部分场景,语义重叠度高,可不做。

### 4.4 不建议暴露的 Master RPC

- `RegisterApplicationInfo`/`HeartbeatFromApplication`/`HeartbeatFromWorker`/`PbRegisterWorker`/`ReportWorkerUnavailable`/`ReportWorkerDecommission`/`PbApplicationMetaRequest`:内部协议,高频或鉴权耦合,REST 暴露无收益且增加攻击面。
- `RequestSlots`/`PbUnregisterShuffle(s)`:slot 分配是 shuffle 协议核心,shuffle 注销应由 app 生命周期驱动,手动暴露易破坏一致性。

### 4.5 Worker RPC(控制面 3 条,均不建议暴露)

Worker 的控制端点是 **`Controller`**(`worker/.../Controller.scala`,endpoint 名 `WorkerEndpoint`;代码中无 `WorkerRpcEndpoint` 类),`receiveAndReply`(`:104`)只处理 3 条消息;另有 `InternalRpcEndpoint` 仅接收 master 推送的 `PbApplicationMeta`,非客户端面。push/fetch/replicate 属 Netty TransportServer 数据面协议,非 `RpcEndpoint`。

| # | 消息 | 功能 | 返回 | 鉴权 |
|---|---|---|---|---|
| 1 | `ReserveSlots` | 为 shuffle 预分配 partition writer/槽位(primary+replica) | `ReserveSlotsResponse(status, reason)` | `checkAuth(applicationId)`(`Controller.scala:118`) |
| 2 | `CommitFiles` | 提交关闭并持久化 partition 数据文件(epoch 幂等、并发、超时取消) | `CommitFilesResponse(status, committed/failed ids, storageInfos, ...)` | `checkAuth(applicationId)`(`:149`) |
| 3 | `DestroyWorkerSlots` | 销毁/清理指定 shuffle 的 partition 文件并释放槽位(回滚) | `DestroyWorkerSlotsResponse(status, failedPrimarys, failedReplicas)` | `checkAuth(appId)`(`:167`) |

worker REST v1 资源层**完全不引用**这 3 条 RPC;现有端点全是只读监控 + 退出管理。**不建议暴露的理由**:

1. 是 shuffle 引擎内部协议,由 LifecycleManager 在 shuffle write 流程中按 stage 自动调用,手动触发会破坏分区一致性;
2. 鉴权耦合 app secret(SASL),REST 的 HTTP Basic/Bearer 体系无法表达"某 app 对某 shuffle 的权限";
3. worker 运维需求(查 shuffle/分区/状态、退出、改日志/配置)已被 REST v1 全覆盖,无语义缺口。

### 4.6 三层缺口总矩阵

| 能力 | RPC | REST | CLI | 缺口 |
|---|---|---|---|---|
| Master:worker exclude/event/remove-unavailable/revise-shuffles | ✅ | ✅ | ✅ | 无 |
| Master:Ratis 选举/成员/快照/meta-conf(10 操作) | ✅ | ✅ | ❌ | **CLI 缺 10** |
| Master:Logger 动态级别(2) | — | ✅ | ❌ | **CLI 缺 2** |
| Master:CheckQuota 查询 | ✅ | ❌ | ❌ | **REST+CLI 缺** |
| Master:CheckWorkersAvailable 健康判定 | ✅ | ❌ | ❌ | **REST+CLI 缺** |
| Master:ApplicationLost 强制清理 app | ✅ | ❌(deleteApps 非等价) | ❌ | **CLI 缺**(REST 可选) |
| Master:WorkerLost 主动标记 | ✅ | ❌(仅能查) | ❌ | REST+CLI 缺(低优) |
| `/ping` 健康检查 | — | ❌(仅 Deprecated 根端点) | ❌ | REST+CLI 缺(见 5.4) |
| slot 分配/注册/心跳/secret | ✅ | — | — | 不应暴露(内部协议) |
| Worker:shuffle/状态/退出监控 | — | ✅ | ✅ | 无 |
| Worker:Logger 动态级别(2) | — | ✅ | ❌ | **CLI 缺 2** |
| Worker:ReserveSlots/CommitFiles/DestroyWorkerSlots | ✅ | ❌ | ❌ | **不应暴露**(内部协议+鉴权耦合) |

---

## 五、OpenAPI yaml 一致性审计发现

> 端点层面(method + path),master yaml 的 32 个操作、worker yaml 的 14 个操作**全部覆盖**了代码中实际存在的 v1 端点,无遗漏;路由入口(`ApiV1MasterResource`/`ApiV1WorkerResource`/`ApiV1BaseResource` 的子资源定位器)与 yaml paths 完全对应。以下为不一致点与可优化项。

### 5.1 【Bug】`SendWorkerEventRequest` 的 `required` 引用了不存在的字段 `worker`

- `master_rest_v1.yaml:1261-1263`:属性名是 `workers`(复数,`:1256`),`required` 却写了 `worker`。
- 服务端实现 `master/.../v1/WorkerResource.scala:133` 校验的是 `request.getWorkers.isEmpty`;生成的 `SendWorkerEventRequest.java` 也只有 `workers` 字段。
- 影响:openapi-generator 静默忽略了不存在的 `worker`,但 Swagger UI 会把 `worker` 当必填项展示,对 API 消费者产生误导。
- 修复:`worker` → `workers`,改完需 `-Pgenerate` 重新生成。

### 5.2 【语义不一致】Worker `exit` 端点默认值 `NONE` 落到兜底分支,非 no-op(**已按当前代码修正**)

- yaml `worker_rest_v1.yaml:750-761`:`WorkerExitRequest.type` 枚举含 `NONE` 且 `default: NONE`;生成的 `WorkerExitRequest.java` 字段默认 `TypeEnum.NONE`。
- 服务端 `Worker.scala:949-962` 的 `exit` **现在有 `case _` 兜底分支**(`:960-961`),不再抛 `MatchError`:

```scala
override def exit(exitType: String): String = {
  exitType.toUpperCase(Locale.ROOT) match {
    case "DECOMMISSION" => ... WorkerEventType.Decommission
    case "GRACEFUL"     => ... WorkerEventType.Graceful
    case "IMMEDIATELY"  => ... WorkerEventType.Immediately
    case _ =>
      workerStatusManager.doTransition(workerStatusManager.exitEventType)
  }
  ...
}
```

- 但兜底分支的语义**不是 no-op**:`exitEventType` 默认为 `WorkerEventType.Immediately`(`WorkerStatusManager.scala:38`),当 `celeborn.worker.graceful.shutdown.enabled=true`(`CelebornConf.scala:4491`)时为 `Graceful`(`WorkerStatusManager.scala:42-45`)。即:**不传 `type`(→ NONE)或传任何非法值,都会按"当前默认退出类型"触发 worker 退出流程——默认配置下等同于立即退出**。
- 这既与 `NONE` 的字面语义不符,也意味着一个不带 `type` 的 `POST /api/v1/workers/exit` 请求会默认触发立即退出,属于危险的默认值设计。
- 建议二选一:(a) 服务端显式增加 `case "NONE" =>` 返回 no-op、非法值返回 400;或 (b) yaml 去掉 `NONE` 枚举值与 `default`,改为 required。推荐 (a),与 `SendWorkerEventRequest` 中 `NONE` 语义对齐。

### 5.3 【不一致】`ReviseLostShufflesRequest` 的 `appId` 在 yaml 中非 required,服务端却直接使用

- yaml `master_rest_v1.yaml:1346-1357`:无 `required` 块,`appId`、`shuffleIds` 均可选。
- 服务端 `ApplicationResource.scala:113-114` 直接 `request.getAppId` / `request.getShuffleIds` 传入 `statusSystem.reviseLostShuffles`,appId 为 null 会引发 NPE 或下游异常。
- 建议:yaml 增加 `required: [appId]`。

### 5.4 端点覆盖完整性补充

- **Deprecated 根路径端点**(如 `GET /conf`、`GET /workerInfo`、`POST /exit` 等 21 个,标注 `@Tag("Deprecated")`)功能上都有 v1 等价物,不记录在 openapi 属合理设计;它们返回 `text/plain` 而非 JSON,即使记录也需单独 content-type,不建议塞进现有 yaml。
- **真正"未暴露到 openapi 且 v1 无等价物"的端点只有 1 个**:`GET /ping`(`ApiBaseResource.ping()` 返回 `"pong"`,master/worker 均有)。建议在两个 yaml 各补 `GET /api/v1/ping`(或 `/api/v1/health`),供客户端/K8s 探针使用——当前生成的 SDK 无法做健康检查。
- 非 JAX-RS 端点(openapi 不记录属合理):`/openapi.{json|yaml}`(hidden)、`/metrics/prometheus`、`/metrics/json`(Jetty servlet)、`/swagger*`、`/help`、`/docs`。

### 5.5 可优化点(非 bug)

1. **公共 schema 在两个 yaml 中重复定义**:`ConfigData`、`ConfResponse`、`DynamicConfig`、`ThreadStack`、`ContainerInfo`、`LoggerInfo`、`HandleResponse`、`ShufflesResponse` 等十余个 schema 逐字复制,靠人工同步极易漂移。建议抽取 `common_rest_v1.yaml` 或跨文件 `$ref`(worker 已对 `UpsertDynamicConfigRequest` 这样做)。
2. **缺少错误响应 schema**:多个端点声明了 `"503"`/`"400"` 等错误码但响应体无 schema;服务端 `RestExceptionMapper` 实际返回结构化错误。建议为 4xx/5xx 统一补错误响应 schema。
3. **`WorkerInfoResponse` 与 `WorkerData` 高度重复**:仅多 3 个 bool,可用 `allOf` 复用。
4. **部分端点缺 `tags`**:`/api/v1/container_info`、`/api/v1/thread_dump` 在 yaml 中无 `tags`,Swagger UI 归到 default tag,组织不一致。
5. **`servers` 写死 localhost 端口**:master `http://localhost:9098/`、worker `http://localhost:9096/`;可变量化为 `{scheme}://{host}:{port}`。
6. **无 CI 校验"yaml 与生成代码同步"**:生成代码已提交 git,但无"重新生成后 diff 必须为空"的 CI 检查,存在手改生成代码或 yaml 漂移风险。建议 CI 增加 `build/mvn -pl openapi/openapi-client -Pgenerate` 后 `git diff --exit-code`。
7. **`supportUrlQuery=false` 的副作用**(`openapi-client/pom.xml:240`):生成的客户端不把 query 参数拼到 URL 而是走 httpClient 参数 API,功能正常,但与"可复制 URL 直接调"的预期不同。仅作记录。

### 5.6 字段级一致性(已逐字段核对一致,备查)

`WorkersResponse`(6 个数组字段)、`WorkerInfoResponse`(多 3 个 bool 且 required 正确)、`PartitionLocationData`、`WorkerResourceConsumption`、`MasterInfoResponse`/`MasterCommitData`/`RatisLogInfo`、`Ratis*` 请求体、`Upsert/DeleteDynamicConfigRequest`(worker yaml 跨文件 `$ref` 共享)、`Logger`/`Conf`/`ThreadStack`/`ContainerInfo` 公共 schema——均与服务端 `ApiUtils` 构造逻辑及生成 model 一致。

---

## 六、缺陷与增强方案(三份去重合并,按优先级)

> 每条带当前代码证据。P0 = 必现 bug / 最重要能力缺口;P1 = 行为一致性与健壮性小修;P2 = 补齐已生成未暴露的 REST 能力;P3 = 运维体验增强(可选)。

| # | 优先级 | 类型 | 问题 / 方案 | 证据 | 改法 |
|---|--------|------|------------|------|------|
| 1 | P0 | Bug | yaml `required: [eventType, worker]` 笔误(属性名是 `workers`) | `master_rest_v1.yaml:1261-1263` | `worker`→`workers`,`-Pgenerate` 重新生成 |
| 2 | P0 | Bug | `--send-worker-event DECOMMISSION_THEN_IDLE` 必现失败:paramLabel 提示带下划线,但枚举常量是 `DECOMMISSIONTHENIDLE`,`valueOf` 必抛 `IllegalArgumentException`(转为 ParameterException) | `MasterOptions.scala:48-53`;`SendWorkerEventRequest.java:52`;`MasterSubcommandImpl.scala:109-115` | paramLabel 改为与枚举/yaml 一致的 `DECOMMISSIONTHENIDLE` |
| 3 | P0 | 语义缺陷 | Worker `exit` 默认值 `NONE` 落到 `case _` 兜底,按默认退出类型(默认 Immediately)触发退出,非 no-op | `worker_rest_v1.yaml:750-761`;`Worker.scala:949-962`;`WorkerStatusManager.scala:38,42-45` | 显式 `case "NONE" =>` no-op + 非法值 400;或 yaml 去 `NONE`/default 改 required |
| 4 | P0 | 增强 | 全局 `--output-format`(`pretty`/`json`/`table`)。当前输出是 `println(model.toString())` 伪 JSON(如 `HandleResponse.java:119-124` 的 `class HandleResponse {...}`),无法喂 `jq`;无表格渲染 | `CliLogging.scala:28-30`;`CommonOptions.scala` 无此选项 | json 用 Jackson 序列化 model(无需新增依赖);table 自研轻量 ASCII 渲染;pretty 保持现状为默认 |
| 5 | P1 | Bug | 入口创建两个 `CommandLine`,设置了 case-insensitive 的第一个被丢弃,配置未生效 | `CelebornCli.scala:40-43` | 用设置好的 `cmd` 执行 |
| 6 | P1 | Bug | `--version` 提示字符串无 `s` 插值,`$CELEBORN_HOME` 被原样输出 | `CliVersionProvider.scala:47-48` | 加 `s` 插值并用 `${CELEBORN_HOME}` |
| 7 | P1 | Bug | `upsertDynamicConf` 的 `split(':')` 无 limit:value 含冒号或缺冒号时 `val Array(k, v)` 抛 `MatchError` | `BaseCommand.scala:49-50` | 改 `split(':', 2)` + 长度校验抛 `ParameterException`(对照 `MasterSubcommandImpl.scala:291` 已正确用 `split("=", 2)`) |
| 8 | P1 | Bug | `--exit` 输入校验不足:`TypeEnum.valueOf(workerOptions.exitType)` 无 `toUpperCase`、无 catch,非法输入抛 `IllegalArgumentException` | `WorkerSubcommandImpl.scala:67-68` | 用 `TypeEnum.fromValue`(`WorkerExitRequest.java:68`,大小写不敏感),与 master 侧 `--send-worker-event` 的 ParameterException 处理对齐 |
| 9 | P1 | Bug | `getWorkerIds` 错误提示张冠李戴:缺的是 `--worker-ids`,提示却是 "Host list must be provided" | `MasterSubcommandImpl.scala:189-191` | 改为 "Worker ids must be provided via --worker-ids." |
| 10 | P1 | 健壮性 | `toWorkerId` 解析脆弱:`split(":")` 后直接取下标 + `toInt`,格式错误抛 `ArrayIndexOutOfBoundsException`/`NumberFormatException` | `MasterSubcommandImpl.scala:201-210` | 校验 5 段并抛 `ParameterException` |
| 11 | P1 | 健壮性 | Master HA 无 failover:alias 多 host 时只取第一个 | `MasterSubcommand.scala:56` | 遍历地址直到成功,或用 `MasterApi` 查当前 leader |
| 12 | P1 | 一致性 | worker 子命令不认 `--cluster`(help 显示但未使用),缺 `--hostport` 直接抛异常,UX 混乱 | `WorkerSubcommand.scala:43-52` | 与 `MasterSubcommand.scala:50-66` 对齐,复用 `CliConfigManager` |
| 13 | P1 | 健壮性 | `CliLogging` 颜色无结尾 RESET,可能污染后续终端输出 | `CliLogging.scala:32-38` | `logInfo`/`logError` 末尾补 `${RESET}` |
| 14 | P1 | 性能 | `ApiClient` 每次调用都新建,无复用;`runIsShutdown`/`runIsDecommissioning`/`runIsRegistered` 各自调 `runShowWorkerInfo`(def),同时指定会发三次相同请求 | `MasterSubcommand.scala:50,67-72`;`WorkerSubcommand.scala:43,53-57`;`WorkerSubcommandImpl.scala:61-65` | `apiClient` 改 lazy val;状态布尔值共享一次响应 |
| 15 | P1 | 语义 | 全模块无 `System.exit`,退出码语义未文档化 | `CelebornCli.scala:39-44` | 明确退出码(成功 0、参数错误 2、执行异常 1)并写入 `docs/celeborn_cli.md` |
| 16 | P1 | 测试 | `captureErrorAndValidateResponse` 用 `System.setErr` 全局副作用,未在 finally 恢复 | `TestCelebornCliCommands.scala:394-406` | finally 中恢复 `System.err` |
| 17 | P2 | 暴露缺口 | `RatisApi` → CLI(master 10 操作):leader 转移/下台、选举暂停/恢复、peer 增删/优先级、snapshot、raft-meta.conf 下载/生成 | 见 3.3、4.6 | 加 `--ratis-*` 系列 flag;后两个文件端点需 CLI 增加二进制/文件输出能力 |
| 18 | P2 | 暴露缺口 | `LoggerApi` → CLI(master + worker 各 2 操作):动态日志级别管理 | 见 3.3 | `--show-loggers` / `--set-logger <name>=<level>` |
| 19 | P2 | 暴露缺口 | metrics 查询:prometheus/json 端点未入 OpenAPI,SDK 无法访问 | `CelebornConf.scala:6044-6060` | `--show-metrics`,可直接 HTTP GET `/metrics/*` |
| 20 | P2 | 暴露缺口 | `CheckQuota` → REST + CLI(需 yaml spec → 生成 → resource → CLI 全链路) | 见 4.3 | `GET /api/v1/quota/...` + `--show-quota` |
| 21 | P2 | 暴露缺口 | `PbCheckWorkersAvailable` + `/ping` 健康探针,可合并为一个 `/api/v1/health` | 见 4.3、5.4 | 新增端点 + `--check-workers-available` |
| 22 | P3 | 暴露缺口 | `ApplicationLost` → CLI(走 RPC,绕开 REST),清理僵尸 app | 见 4.3 | `master --application-lost <appId>` |
| 23 | P3 | 增强 | `--output-file` 落盘(对标 Uniffle `-f`,注意避免其取值 bug,见 7.4) | — | 新增选项 |
| 24 | P3 | 增强 | 环境变量回退:`CELEBORN_CLI_HOST`/`CELEBORN_CLI_AUTH_HEADER` | — | 新增 |
| 25 | P3 | 增强 | 批量操作:`--exclude-worker` 等接受文件输入(一行一个 workerId) | — | 新增 |
| 26 | P3 | 增强 | `--quiet`/`--verbose` 日志级别开关 | `CliLogging.scala` | 新增 |
| 27 | P3 | 架构 | flag-based dispatch 导致 `run()` 是长 `if` 链,可重构为 picocli 嵌套子子命令(如 `celeborn-cli master show-workers`) | `MasterSubcommandImpl.scala:33-66` | 重构(注意兼容性) |
| 28 | P3 | 架构 | 仅支持 HTTP,`setBasePath(s"http://...")` 写死;无 RPC fallback(HTTP 被禁用时无法管理集群) | `MasterSubcommand.scala:60`;`WorkerSubcommand.scala:46` | 支持 HTTPS scheme;评估 RPC 通道 |

---

## 七、与 Uniffle CLI 对比

> ⚠️ Uniffle 侧证据基于外部基线工作树 `uniffle-master` 的快照(2026-08-05 整理,行号以该快照工作树为准),**本次合并未重新核对该仓库**。Celeborn 侧证据均已对照当前代码重新核实。

### 7.1 设计维度总览

| 维度 | Celeborn CLI | Uniffle CLI |
|---|---|---|
| 解析框架 | picocli 4.7.6(注解驱动;父 `pom.xml:113`,`cli/pom.xml:34-36`) | commons-cli 1.5.0(手工 `Option` + if 链) |
| 命令模型 | 两级子命令 `master`/`worker`,`@ArgGroup(exclusive=true, multiplicity="1")` | 标志位 + Hadoop 风格 `bin/uniffle` shell 子命令框架 |
| 通信层 | OpenAPI codegen 生成的 client(`apache-httpclient` 库),产物已提交 git | 自研四层:CLI → `AdminRestApi` → `UniffleRestClient` → `RestClientImpl`(Apache HttpClient 4.5.3) |
| 命令覆盖面 | master 28 个 `@Option` + worker 15 个 `@Option`,覆盖全部已生成 Api(除 Ratis/Logger) | 仅 `--applications` + `--refreshChecker` 两个真实命令(+ `-c`/`-a` 两个 echo) |
| 输出 | `println(model.toString())` 伪 JSON,无表格无 `--output-format` | ASCII 表格 + Gson JSON + `-f` 落盘(落盘有 bug) |
| 配置 | `~/.celeborn-cli.conf`(Properties),仅 master 子命令用 | 无配置文件,每次手敲 host/port |
| 认证 | `--auth-header` 透传 + 服务端 SPNEGO/BASIC/BEARER + 写操作要求管理员 | auth header 全部传 null,无认证选项 |
| 超时/重试 | 依赖生成 client 默认(默认无超时) | `maxAttempts=3` 定义但从未接线(死代码) |
| 测试 | MiniCluster 端到端 + 子串断言,无 golden file | System.out 重定向 + golden file + mock RestClient |

要点:

- **解析框架**:picocli 的注解模型(子命令、`@ArgGroup` 互斥校验、`@Mixin` 复用、自动 usage/completion)远优于 commons-cli 的手工 `Option` + if 链,Celeborn 明显领先。
- **通信层**:Celeborn 端点/模型/序列化全自动生成且与服务端 spec 同源;Uniffle 手写四层,序列化/反序列化/URL 构造全自行实现,维护成本高。
- **输出**:Uniffle 明显领先——ASCII 表格(`CLIContentUtils`,列宽动态计算 + 居中对齐)+ Gson JSON(`-o json`)+ `-f` 落盘,Celeborn 三者皆无。这是 Celeborn 最该借鉴的。
- **认证**:Celeborn 端到端完整(透传 + 服务端多 scheme + 管理员鉴权);Uniffle 完全无认证。
- **测试**:Celeborn 用 MiniCluster 起真实 master + 1 worker 做端到端断言(`TestCelebornCliCommands.scala`,BASIC 认证 + H2 内存 DB),`--show-masters-info` 测试因 HA 未启用被 `cancel`(`:182`);Uniffle 有 golden file 比对与 mock RestClient 单测,但 `--applications` 实际 REST 路径无测试。

### 7.2 Uniffle 有而 Celeborn CLI 没有的能力(3 类)

1. **`--refreshChecker`** — 动态刷新 coordinator 的访问检查器。Celeborn 无 "access checker" 概念,鉴权为静态配置,无运行时刷新命令。
2. **应用列表的过滤/分页/心跳时间范围参数**(`--app-list`/`--appId-regex`/`--app-pageSize`/`--app-currentPage`/`--app-heartbeatTimeRange`)— Celeborn 的 `--show-cluster-apps` 对应 `GET /api/v1/applications`,该端点**不接受任何 query 参数**(OpenAPI yaml 无 `parameters`、生成方法签名仅 `additionalHeaders`、服务端直接返回全部,三方互证),故无任何过滤能力。
3. **结构化输出(JSON 输出 + 落盘)** — Celeborn 完全没有(见第六节 #4、#23)。

需要澄清、**不算 Uniffle 独有**的:decommission/cancelDecommission 与 addExcludeNodes/removeExcludeNodes 在 Uniffle 服务端有端点但 CLI 未封装;Celeborn CLI 的 `--send-worker-event DECOMMISSION/RECOMMISSION` 与 `--exclude-worker`/`--remove-excluded-worker` 功能等价。

此外 Uniffle 服务端还暴露了一批 Celeborn 没有的端点(应用总数/用户维度统计、按 ID 查节点、节点汇总、dashboard 模块),但连 Uniffle 自己的 CLI 都没封装,属服务端能力差异而非 CLI 差异。

### 7.3 Celeborn 有而 Uniffle CLI 没有的能力

Uniffle CLI 真实命令仅 2 个;Celeborn CLI 覆盖了 worker 全生命周期管理(exclude/event/decommission/shutdown 查询)、动态配置增删查、thread dump、container info、revise lost shuffles、delete apps、interruption notice、cluster alias 持久化等 41 个动作,且全部带认证透传。Uniffle 侧这些能力要么服务端也没有,要么有端点但 CLI 未封装。

### 7.4 Uniffle 侧代码缺陷(快照证据,供借鉴时避雷)

1. **`-f` 落盘取值 bug(必现)**:`UniffleCLI.java:190` 取的是 `-o` 的值(通常为 `"json"`)当文件名,用户指定的 `-f` 路径被忽略,落盘文件名恒为 `"json"`。
2. **调试输出残留**:`AbstractCustomCommandLine.java:106` 每次建客户端都 `println("host:...port:...")`,污染正常输出。
3. **连接池未配置且 client 未 close**:HttpClient 默认 2 per route / 20 total;`UniffleRestClient` 实现 `AutoCloseable` 但无人调用 close。
4. **POST/GET 参数放置不一致**:GET 拼 query string,POST 序列化为 JSON body。
5. **心跳时间范围按 UTC 转换**,coordinator 时区非 UTC 时会偏移。
6. **重试是死代码**:`maxAttempts=3` 写入配置但 `HttpClientFactory`/`RestClientImpl` 从不读取,无重试循环。
7. **`-c`/`-a` 无功能 echo 命令**,疑似占位未完成。
8. **测试覆盖薄弱**:仅测 help 与 echo,`--applications` 的 REST 路径、`-o json`、`-f` 落盘、过滤参数均无测试。

---

## 八、延伸阅读与关键文件索引

### 8.1 延伸阅读

- `service-and-security-deep-dive.md` — REST 服务、认证、动态配置服务端实现(同目录)。
- `celeborn-conf-functional-map.md` — 配置参数功能地图(同目录)。
- `docs/celeborn_cli.md` — CLI 用户文档。
- picocli 文档:https://picocli.info/

### 8.2 关键文件索引

- 规范源:`openapi/openapi-client/src/main/openapi3/master_rest_v1.yaml`、`worker_rest_v1.yaml`
- 生成配置:`openapi/openapi-client/pom.xml`(generate profile)、`project/CelebornBuild.scala`(CelebornOpenApi)
- master v1 资源:`master/.../http/api/v1/{ApiV1MasterResource,WorkerResource,ApplicationResource,RatisResource,MasterResource,ShuffleResource}.scala`
- worker v1 资源:`worker/.../http/api/v1/{ApiV1WorkerResource,WorkerResource,ShuffleResource,ApplicationResource}.scala`
- 共享 v1 资源:`service/.../http/api/v1/{ApiV1BaseResource,ConfResource,LoggerResource,ApiUtils}.scala`
- Worker.exit 实现:`worker/.../Worker.scala:949`
- Master RPC 分发:`master/.../Master.scala:462`(receiveAndReply)、`:440`(receive)
- Worker 控制 RPC:`worker/.../Controller.scala:104`(receiveAndReply,3 条消息)
- Worker 内部 RPC:`worker/.../InternalRpcEndpoint.scala`(接收 PbApplicationMeta)
- RPC 消息定义:`common/.../protocol/message/ControlMessages.scala`
- Master client wrapper:`common/.../client/MasterClient.java`
- CLI 入口:`cli/.../CelebornCli.scala`;子命令:`cli/.../master/MasterSubcommandImpl.scala`、`cli/.../worker/WorkerSubcommandImpl.scala`
- 生成客户端 API 类:`openapi/openapi-client/src/main/java/.../rest/v1/master/{RatisApi,LoggerApi,...}.java`、`.../worker/{LoggerApi,...}.java`
- CLI 测试:`cli/src/test/scala/org/apache/celeborn/cli/TestCelebornCliCommands.scala`
- 契约测试:`worker/.../http/api/v1/ApiV1WorkerOpenapiClientSuite.scala`、`ApiV1OpenapiClientSuite.scala`;`master` 侧 `ApiV1MasterResourceSuite`

### 8.3 验证方式(yaml/服务端改动后)

1. 重新生成客户端并确认 diff 仅在预期文件:`build/mvn -pl openapi/openapi-client clean package -Pgenerate` 后 `git diff`。
2. 跑 openapi client 契约测试(端到端校验):
   `build/mvn -pl worker -Pspark-3.5 test -Dtest=ApiV1WorkerOpenapiClientSuite,ApiV1OpenapiClientSuite`
   `build/mvn -pl master -Pspark-3.5 test -Dtest=ApiV1MasterResourceSuite`
3. 若修复 5.2 的 `NONE` 语义,补充用例:不传 `type` 调 `/api/v1/workers/exit` 应返回 no-op(而非触发退出)。
