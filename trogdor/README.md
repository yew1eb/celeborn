---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Celeborn Trogdor

Celeborn Trogdor 是一个分布式测试与故障注入框架,灵感来自
[Apache Kafka Trogdor](https://kafka.apache.org/documentation/#trogdor)。它以独立的
Coordinator 与 Agent 进程运行,可以向 Celeborn 集群提交长时运行的工作负载或注入故障。

## 架构

- **Trogdor Coordinator**:维护任务状态机,将任务调度到 agent 上。
- **Trogdor Agent**:运行在目标节点上,执行真正的 workload/fault worker。
- **TaskSpec**:描述要运行什么;JSON 中的 `class` 字段选择具体实现类。
- **TaskController**(coordinator 侧):决定任务应在哪些 agent 节点运行。
- **TaskWorker**(agent 侧):在后台线程中执行真正的逻辑。

Coordinator 与 agent 之间通过 HTTP 通信。Coordinator 以固定心跳(默认 1s)轮询每个 agent 的
状态,把"期望运行的 worker 集合"与"agent 上报的实际集合"对账,当某个任务的所有 worker 都
到达 `DONE` 时推进任务状态机。

### 数据流

```
                        createTask(TaskSpec)
   client ──────────────────────────────────────►  Coordinator
                                                       │
                            heartbeat (1s)             │ runTask: 分配 workerId,
                  ┌────────────────────────────────────│  在 endMs 处调度 stopTask
                  ▼                                    ▼
               Agent  ◄────── createWorker ──────  NodeManager
            (每节点)           stopWorker            (每节点)
                 │
                 │ WorkerManager.start(TaskWorker)
                 ▼
           TaskWorker ── haltFuture.complete("") ──►  worker DONE
                                                      │
                  心跳上报 worker 状态 ◄──────────────┘
                  → 所有 worker DONE 时 maybeFinishTask
```

### 任务状态机

```
   PENDING ──runTask──► RUNNING ──所有 worker DONE──► DONE
      │                    │
      │ stopTask           │ stopTask
      ▼                    ▼
   DONE(err=stopped)   STOPPING ──► DONE
```

任务在 `runTask` 触发前处于 `PENDING`(由于 coordinator 在提交时会把过去的 `startMs` 重置为
`now`,因此 `startMs <= now` 时会立即触发,这是常见情况)。worker 分配后转为 `RUNNING`,收到
`stopTask` 转为 `STOPPING`,所有 worker 上报 `DONE` 后转为 `DONE`。当 worker 失败或 `runTask`
抛异常(例如未知的目标节点)时,任务的 `error` 字段非空。

## 快速开始

### 1. 配置集群拓扑

Trogdor 用一个 JSON 拓扑文件描述有哪些节点、如何访问其 agent。复制模板并按你的集群修改:

```shell
cd $CELEBORN_HOME/conf
cp trogdor.conf.template trogdor.conf
```

`trogdor.conf` 是一个 JSON 文档(以 `#` 开头的行是注释):

```json
{
  "platform": "org.apache.celeborn.trogdor.platform.BasicPlatform",
  "nodes": {
    "node0": {
      "hostname": "localhost",
      "config": {
        "trogdor.agent.port": "19090"
      }
    }
  }
}
```

每个节点条目包含 `hostname` 和一个 `config` map。`trogdor.agent.port` 的值必须与该节点 agent
绑定的 HTTP 端口一致(见 `celeborn.trogdor.agent.http.port`)。

### 2. 启动 agent

在拓扑中列出的每个节点上启动 agent:

```shell
cd $CELEBORN_HOME
./sbin/start-trogdor-agent.sh
```

默认绑定 `0.0.0.0:19090`。可用标准 Celeborn 配置项覆盖,例如写入
`$CELEBORN_HOME/conf/celeborn-defaults.conf`:

```shell
celeborn.trogdor.agent.http.host=0.0.0.0
celeborn.trogdor.agent.http.port=19090
```

### 3. 启动 coordinator

启动单个 coordinator(不必运行在 Celeborn master/worker 节点上):

```shell
cd $CELEBORN_HOME
./sbin/start-trogdor-coordinator.sh
```

默认绑定 `0.0.0.0:19091`:

```shell
celeborn.trogdor.coordinator.http.host=0.0.0.0
celeborn.trogdor.coordinator.http.port=19091
```

### 4. 验证服务已启动

```shell
# agent 状态(列出 agent 上正在运行的 worker)
$ celeborn-cli trogdor agent -t localhost:19090 --status

# coordinator 状态
$ celeborn-cli trogdor coordinator -t localhost:19091 --status

# 或直接用 curl
$ curl http://localhost:19090/api/v1/trogdor/agent/status
$ curl http://localhost:19091/api/v1/trogdor/coordinator/status
```

### 5. 提交任务

将任务 spec 写入 JSON 文件(`class` 字段选择 worker 实现类),在 coordinator 上创建:

```shell
$ cat > /tmp/noop.json <<'EOF'
{
  "class": "org.apache.celeborn.trogdor.task.NoOpTaskSpec",
  "startMs": 0,
  "durationMs": 5000,
  "targetNodes": ["node0"]
}
EOF

$ celeborn-cli trogdor coordinator -t localhost:19091 \
    --create-task /tmp/noop.json -i noop-1

$ celeborn-cli trogdor coordinator -t localhost:19091 --show-task -i noop-1
```

可用的 spec 类与 JSON 形态见 [内置工作负载](#内置工作负载)、[内置故障](#内置故障)与
[混沌测试](#混沌测试)。

## Task spec 约定

每个 task spec 都是一个多态 JSON 对象(见[扩展 Trogdor](#扩展-trogdor)):

- `class`(必填):实现类的全限定名,例如 `org.apache.celeborn.trogdor.workload.PushBenchSpec`。
  该字段用于选择 `TaskWorker`,值必须与源码包路径完全一致,否则反序列化失败。
- `startMs`(`long`):起始时刻(epoch 毫秒)。用 `0` 表示立即开始;coordinator 提交时会把过去
  的 `startMs` 重置为 `now`。
- `durationMs`(`long`):任务运行时长(毫秒)。会被截断到 `[0, 1000000000000000]`。
  coordinator 会在 `startMs + durationMs` 处自动调度 `stopTask`。
- `targetNodes`(字符串数组):运行该任务的 agent 节点名(来自拓扑)。

下文各字段表中标注为"必填"的字段没有默认值,JSON 中必须存在。

## 内置工作负载

### PushBench

以尽可能快的速度向 Celeborn 推送合成的 shuffle 数据。

```json
{
  "class": "org.apache.celeborn.trogdor.workload.PushBenchSpec",
  "startMs": 0,
  "durationMs": 60000,
  "targetNodes": ["node0"],
  "masterHost": "localhost",
  "masterPort": 9097,
  "numMappers": 4,
  "numPartitions": 100,
  "bytesPerPush": 1024,
  "totalPushes": 100000,
  "userIdentifier": "default:default"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 执行 push 的 agent 节点名 |
| `masterHost` | `string` | 是 | Celeborn master 主机名 |
| `masterPort` | `int` | 是 | Celeborn master 端口 |
| `numMappers` | `int` | 是 | 并发 mapper 数(截断到 `>= 1`) |
| `numPartitions` | `int` | 是 | 分区数(截断到 `>= 1`) |
| `bytesPerPush` | `int` | 是 | 每次 push 的字节数(截断到 `>= 1`) |
| `totalPushes` | `long` | 是 | 总 push 次数(截断到 `>= 0`) |
| `userIdentifier` | `string` | 否 | `<tenant>:<name>`;默认 `"default"` |

### FetchBench

向每个分区写入一条种子记录,然后反复拉取所有分区。

```json
{
  "class": "org.apache.celeborn.trogdor.workload.FetchBenchSpec",
  "startMs": 0,
  "durationMs": 60000,
  "targetNodes": ["node0"],
  "masterHost": "localhost",
  "masterPort": 9097,
  "numPartitions": 10,
  "fetchesPerPartition": 1000,
  "userIdentifier": "default:default"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 执行 fetch 的 agent 节点名 |
| `masterHost` | `string` | 是 | Celeborn master 主机名 |
| `masterPort` | `int` | 是 | Celeborn master 端口 |
| `numPartitions` | `int` | 是 | 分区数(截断到 `>= 1`) |
| `fetchesPerPartition` | `long` | 是 | 每个分区的 fetch 次数(截断到 `>= 0`) |
| `userIdentifier` | `string` | 否 | `<tenant>:<name>`;默认 `"default"` |

### RpcBench

通过向本地 echo 端点发送同步 ask 请求,测量 Celeborn RPC 往返延迟。

```json
{
  "class": "org.apache.celeborn.trogdor.workload.RpcBenchSpec",
  "startMs": 0,
  "durationMs": 30000,
  "targetNodes": ["node0"],
  "totalRpcs": 5000,
  "payload": "hello"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 执行 RPC bench 的 agent 节点名 |
| `totalRpcs` | `long` | 是 | 总 RPC 次数(截断到 `>= 0`) |
| `payload` | `string` | 否 | echo 请求的 payload;默认 `"hello"` |

push/fetch 工作负载的 `masterHost`/`masterPort`/`userIdentifier` 字段省略时,会回退到
`celeborn.trogdor.workload.*` 默认值。

## 内置故障

故障与工作负载使用相同的 task spec 形态(`class` 字段选择故障类型),但它在目标 agent 所在
主机上运行,而非针对 Celeborn 集群。端到端示例见[故障注入实战](#故障注入实战)。

### ProcessStopFault

用 `pgrep` 和 `kill -STOP` / `kill -CONT` 暂停并恢复匹配名称的进程。依赖 Linux/Unix 进程管理
工具。

```json
{
  "class": "org.apache.celeborn.trogdor.fault.ProcessStopFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["node0"],
  "processName": "celeborn-worker"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 注入故障的节点 |
| `processName` | `string` | 是 | 要暂停/恢复的进程名(通过 `pgrep` 匹配) |

### NetworkPartitionFault

用 `iptables` 阻断到一组节点的出站流量。仅 Linux 且有 `iptables` 时可用,且需要 root 权限。

```json
{
  "class": "org.apache.celeborn.trogdor.fault.NetworkPartitionFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["worker-0"],
  "blockedNodes": ["worker-1", "master-0"]
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 被隔离的源节点 |
| `blockedNodes` | `string[]` | 是 | `targetNodes` 无法访问的节点 |

### DiskSlowFault

模拟慢磁盘 IO。当前实现仅记录预期延迟;生产环境应扩展为真实的块设备延迟配置(例如
`device-mapper` 或 `tc`)。

```json
{
  "class": "org.apache.celeborn.trogdor.fault.DiskSlowFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["node0"],
  "device": "/dev/sda",
  "delayMs": 100
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 注入故障的节点 |
| `device` | `string` | 是 | 块设备路径(如 `/dev/sda`) |
| `delayMs` | `long` | 是 | 模拟的 IO 延迟毫秒数 |

### ExternalCommandFault

在目标 agent 上执行任意外部命令。命令会带上可选的环境变量执行,进程退出时 worker 完成。

```json
{
  "class": "org.apache.celeborn.trogdor.fault.ExternalCommandFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["node0"],
  "command": ["sh", "-c", "echo hello && sleep 2"],
  "env": {"KEY": "VALUE"}
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `targetNodes` | `string[]` | 是 | 执行命令的节点 |
| `command` | `string[]` | 是 | 命令的 argv 数组 |
| `env` | `map<string,string>` | 否 | 进程的环境变量 |

## 故障注入实战

本节演示向运行中的 Celeborn worker 注入 `ProcessStopFault`、观察效果并清理的全流程。所有
故障的流程相同,只是 spec 不同。

1. **选定目标。** 假设拓扑节点 `node0` 上运行着名为 `celeborn-worker` 的 Celeborn worker 进程。
   确认 agent 已启动且 worker 进程存在:

   ```shell
   $ celeborn-cli trogdor agent -t localhost:19090 --status
   $ pgrep -f celeborn-worker   # 在 node0 上
   ```

2. **编写故障 spec。** 暂停 worker 10 秒(故障结束时自动恢复):

   ```shell
   $ cat > /tmp/procstop.json <<'EOF'
   {
     "class": "org.apache.celeborn.trogdor.fault.ProcessStopFaultSpec",
     "startMs": 0,
     "durationMs": 10000,
     "targetNodes": ["node0"],
     "processName": "celeborn-worker"
   }
   EOF
   ```

3. **提交并观察任务状态。**

   ```shell
   $ celeborn-cli trogdor coordinator -t localhost:19091 \
       --create-task /tmp/procstop.json -i procstop-1
   $ celeborn-cli trogdor coordinator -t localhost:19091 --show-task -i procstop-1
   ```

   任务会经历 `PENDING → RUNNING → DONE`。`RUNNING` 期间目标进程被停止(`ps`/`top` 中显示
   `T` 状态);任务到达 `DONE` 时恢复。

4. **查看 agent 侧 worker。** agent 的 status 响应会列出该故障 worker 及其状态;正常运行时
   `error` 为空:

   ```shell
   $ celeborn-cli trogdor agent -t localhost:19090 --status
   ```

5. **清理。** 任务到达 `DONE` 后,记录仍保留在 coordinator 上供查看;用 `destroy` 删除:

   ```shell
   $ celeborn-cli trogdor coordinator -t localhost:19091 --destroy-task -i procstop-1
   ```

   若想在 `durationMs` 到期前提前结束故障,改用 `--stop-task`;worker 的 `stop()` 会恢复进程,
   任务转为 `STOPPING → DONE`。

> **注意:** `NetworkPartitionFault`/`DiskSlowFault` 需要 root 或相关 capability——以 root
> 运行 agent(或授予 `CAP_NET_ADMIN`)并确认 OS 工具存在;否则 worker 会以 command-not-found
> 报错。见[平台限制](#平台限制)。

## 混沌测试

Celeborn Trogdor 还集成了最初在 CELEBORN-1492 中提出的混沌测试框架。一个混沌计划(plan)
描述一组动作(action)序列(例如 `occupy-cpu`、`stop-worker`、`hang-io`),外加触发器
(trigger)和校验器(checker)。coordinator 侧的 `ChaosOrchestrator` 解析计划并编译为原生
Trogdor 任务:

- 每个目标节点一个长时运行的 `ChaosPlanSpec` 参与者任务。
- 当前 action 选中的每个 operation 生成一个 `ChaosOperationSpec` 任务。

### 计划结构

混沌计划是一个含三个顶层字段的 JSON 对象。它以 `planJson` **字符串字段**的形式提交(属于
`ChaosPlanSpec` 或 `SubmitChaosPlanRequest`),即作为转义后的 JSON 字符串内嵌,而非嵌套对象。

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `actions` | array | 是 | — | action 对象列表 |
| `trigger` | object | 是 | — | 触发策略 |
| `checker` | string | 否 | `dummy` | 校验器类型:`dummy` 或 `resource` |

**action 对象:**

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `id` | string | 是 | — | action id(见下表) |
| `selector` | object | 否 | `dummySelector` | 节点/磁盘选择器 |
| `cores` | int | 仅 `occupy-cpu` | — | 占用的 CPU 核数 |
| `duration` | string | 仅 `occupy-cpu` | `10s` | 每次突发占用 CPU 的时长(受 `chaos.plan.action.occupycpu.maxduration` 上限约束) |

**selector 对象:**

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `type` | string | 否 | `assign` | `assign`(固定节点)或 `random` |
| `interval` | string | 否 | `5s` | 选择器间隔(时间字符串) |
| `indices` | `int[]` | 仅 `assign` | — | 从 0 开始的节点索引 |
| `device` | `int[]` | 否 | `[]` | 磁盘索引(仅磁盘类 action) |

**trigger 对象:**

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `policy` | string | 否 | `random` | `random` 或 `sequence` |
| `repeat` | int | 否 | `1` | 重复次数 |
| `interval` | object | 是 | — | 间隔对象(见下) |

trigger 的 `interval` 对象:

| `type` | 额外字段 | 默认 | 说明 |
|--------|----------|------|------|
| `fix` | `value`(string) | `5s` | 固定间隔 |
| `range` | `start`、`end`(string) | `5s`–`10s` | `[start, end)` 区间内的随机间隔 |

### Action id

| id | Action 类 | 额外字段 |
|----|-----------|----------|
| `occupy-cpu` | `OccupyCpuAction` | `cores`、`duration` |
| `corrupt-disk` | `CorruptDiskAction` | — |
| `resume-disk` | `ResumeDiskAction` | — |
| `hang-io` | `HangIoAction` | — |
| `resume-io` | `ResumeIoAction` | — |
| `corrupt-meta` | `CorruptMetaAction` | — |
| `start-master` | `StartMasterAction` | —(脚本来自 `ChaosConf`) |
| `stop-master` | `StopMasterAction` | —(脚本来自 `ChaosConf`) |
| `start-worker` | `StartWorkerAction` | —(脚本来自 `ChaosConf`) |
| `stop-worker` | `StopWorkerAction` | —(脚本来自 `ChaosConf`) |

未知 id 会被拒绝并抛 `PlanInvalidException`。`random` 触发器与 `corrupt-meta` 组合也会被拒绝
(该动作不可逆)。

### 提交混沌计划

最小计划(一次 `occupy-cpu` 突发,sequence 触发器):

```json
{
  "actions": [
    {
      "id": "occupy-cpu",
      "selector": {"type": "assign", "indices": [0], "interval": "100ms"},
      "cores": 1,
      "duration": "100ms"
    }
  ],
  "trigger": {
    "policy": "sequence",
    "repeat": 1,
    "interval": {"type": "fix", "value": "100ms"}
  },
  "checker": "dummy"
}
```

通过专用 REST 端点提交:

```bash
curl -X POST http://localhost:19091/api/v1/trogdor/coordinator/chaos/plans \
  -H 'Content-Type: application/json' \
  -d '{
    "planId": "my-chaos-plan",
    "planJson": "<上面的计划 JSON 作为转义字符串>",
    "targetNodes": ["node0"]
  }'
```

或用 CLI(`--submit-chaos-plan` 读取含 `planId`/`planJson`/`targetNodes` 的
`SubmitChaosPlanRequest` JSON 文件):

```shell
$ cat > /tmp/plan.json <<'EOF'
{
  "planId": "my-chaos-plan",
  "planJson": "{\"actions\":[{\"id\":\"occupy-cpu\",\"selector\":{\"type\":\"assign\",\"indices\":[0],\"interval\":\"100ms\"},\"cores\":1,\"duration\":\"100ms\"}],\"trigger\":{\"policy\":\"sequence\",\"repeat\":1,\"interval\":{\"type\":\"fix\",\"value\":\"100ms\"}},\"checker\":\"dummy\"}",
  "targetNodes": ["node0"]
}
EOF

$ celeborn-cli trogdor coordinator -t localhost:19091 --submit-chaos-plan /tmp/plan.json
```

或者将计划作为 `ChaosPlanSpec` 任务提交(coordinator 会自动把 `ChaosPlanSpec` 路由到
orchestrator):

```json
{
  "class": "org.apache.celeborn.trogdor.chaos.ChaosPlanSpec",
  "startMs": 0,
  "durationMs": 0,
  "targetNodes": ["node0"],
  "planJson": "{ \"actions\": [...], \"trigger\": {...}, \"checker\": \"dummy\" }"
}
```

查看与停止计划(`-i` 兼作 plan id):

```shell
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-chaos-plan -i my-chaos-plan
$ celeborn-cli trogdor coordinator -t localhost:19091 --stop-chaos-plan -i my-chaos-plan
```

## CLI

`trogdor` 命令组在 `celeborn-cli` 上与 `master`、`worker` 并列注册。所有 trogdor 子命令共享
标准 `-h/--help` 与 `-V/--version` 标志,并用 `-t, --target <host:port>` 定位目标服务(host
部分可为 IPv6 字面量)。

```
celeborn-cli
└── trogdor
    ├── agent ...        # agent 操作    (-t localhost:19090)
    └── coordinator ... # coordinator 操作 (-t localhost:19091)
```

### `celeborn-cli trogdor agent`

| 标志 | 说明 |
|------|------|
| `-t, --target <host:port>` | agent 的 host:port,如 `localhost:19090`(必填) |
| `--status` | 查看 agent 状态 |
| `--uptime` | 查看 agent 运行时长 |

```shell
$ celeborn-cli trogdor agent -t localhost:19090 --status
$ celeborn-cli trogdor agent -t localhost:19090 --uptime
```

### `celeborn-cli trogdor coordinator`

| 标志 | 说明 |
|------|------|
| `-t, --target <host:port>` | coordinator 的 host:port,如 `localhost:19091`(必填) |
| `--status` | 查看 coordinator 状态 |
| `--uptime` | 查看 coordinator 运行时长 |
| `--create-task <file>` | 从 JSON spec 文件创建任务(需配合 `-i`) |
| `-i, --task-id <id>` | 任务 id(混沌计划的 show/stop 也用它作为 plan id) |
| `--show-task` | 查看单个任务(需 `-i`) |
| `--show-tasks` | 查看所有任务 |
| `--stop-task` | 停止任务(需 `-i`) |
| `--destroy-task` | 销毁任务(需 `-i`) |
| `--submit-chaos-plan <file>` | 从 `SubmitChaosPlanRequest` JSON 文件提交混沌计划 |
| `--show-chaos-plan` | 查看混沌计划状态(以 `-i` 作为 plan id) |
| `--stop-chaos-plan` | 停止混沌计划(以 `-i` 作为 plan id) |

```shell
# 创建 / 查看 / 停止 / 销毁任务
$ celeborn-cli trogdor coordinator -t localhost:19091 --create-task /tmp/noop.json -i noop-1
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-tasks
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-task -i noop-1
$ celeborn-cli trogdor coordinator -t localhost:19091 --stop-task -i noop-1
$ celeborn-cli trogdor coordinator -t localhost:19091 --destroy-task -i noop-1

# 提交 / 查看 / 停止混沌计划(-i 兼作 plan id)
$ celeborn-cli trogdor coordinator -t localhost:19091 --submit-chaos-plan /tmp/plan.json
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-chaos-plan -i my-chaos-plan
$ celeborn-cli trogdor coordinator -t localhost:19091 --stop-chaos-plan -i my-chaos-plan
```

`--create-task` 的文件按多态 `TaskSpec` 反序列化(`class` 字段选择实现类)。
`--submit-chaos-plan` 的文件按 `SubmitChaosPlanRequest`(`planId`、`planJson`、`targetNodes`)
反序列化。

## REST API

所有端点均为 JSON,位于 `/api/v1/trogdor` 下。OpenAPI 定义见
`openapi/openapi-client/src/main/openapi3/trogdor_coordinator_rest_v1.yaml` 与
`trogdor_agent_rest_v1.yaml`。

### Coordinator — `http://<coordinator>:19091/api/v1/trogdor/coordinator`

| 方法 | 路径 | Body / 参数 | 说明 |
|------|------|-------------|------|
| GET | `/status` | — | coordinator 启动时间 |
| GET | `/uptime` | — | 服务运行时长 |
| POST | `/tasks` | `CreateTaskRequest` | 根据 spec 创建任务 |
| GET | `/tasks` | — | 列出所有任务 |
| GET | `/tasks/{taskId}` | `taskId` | 查看单个任务状态 |
| PUT | `/tasks/{taskId}/stop` | `taskId` | 停止任务 |
| DELETE | `/tasks/{taskId}` | `taskId` | 销毁任务 |
| POST | `/chaos/plans` | `SubmitChaosPlanRequest` | 提交混沌计划 |
| GET | `/chaos/plans/{planId}` | `planId` | 查看混沌计划状态 |
| PUT | `/chaos/plans/{planId}/stop` | `planId` | 停止混沌计划 |

### Agent — `http://<agent>:19090/api/v1/trogdor/agent`

| 方法 | 路径 | Body / 参数 | 说明 |
|------|------|-------------|------|
| GET | `/status` | — | agent 状态及 worker 状态 |
| GET | `/uptime` | — | 服务运行时长 |
| POST | `/workers` | `CreateWorkerRequest` | 在 agent 上创建 worker |
| PUT | `/workers/{workerId}/stop` | `workerId` | 停止 worker |
| DELETE | `/workers/{workerId}` | `workerId` | 销毁 worker |

## 扩展 Trogdor

Trogdor 是一个框架:内置的工作负载与故障都只是 `TaskSpec` 实现。要添加自己的工作负载或故障,
实现三部分——spec、controller、worker——并用其全限定类名注册即可(无需单独的注册中心)。

### Spec

继承 `TaskSpec`,用 `@JsonCreator` 标注唯一构造器。前两个参数按惯例是 `startMs`/`durationMs`,
转发给 `super(...)`。每个参数用 `@JsonProperty("...")` 指定 JSON 字段名,每个要序列化的字段都要
有带 `@JsonProperty` 的 getter(否则在 JSON 往返中会被丢弃——参见变更日志中 NoOpTaskSpec 的
修复)。示例形态参考 `PushBenchSpec`:

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")  // 继承自 TaskSpec
public class MyWorkloadSpec extends TaskSpec {
  private final Set<String> targetNodes;
  private final int rate;

  @JsonCreator
  public MyWorkloadSpec(
      @JsonProperty("startMs") long startMs,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("targetNodes") Set<String> targetNodes,
      @JsonProperty("rate") int rate) {
    super(startMs, durationMs);
    this.targetNodes = targetNodes;
    this.rate = Math.max(1, rate);
  }

  @JsonProperty
  public Set<String> targetNodes() { return targetNodes; }

  @JsonProperty
  public int rate() { return rate; }

  @Override
  public TaskController newController(String id) {
    return topology -> targetNodes;
  }

  @Override
  public TaskWorker newTaskWorker(String id) {
    return new MyWorkloadWorker(rate);
  }
}
```

### Controller

`TaskController` 是单方法函数式接口——返回目标节点名:

```java
public interface TaskController {
  Set<String> targetNodes(Topology topology);
}
```

### Worker

`TaskWorker` 有 `start` 与 `stop` 两个方法。`start` 必须快速返回——耗时操作放到后台线程——
完成时 complete `haltFuture`:空字符串表示成功,非空字符串视为错误。`stop` 释放所有资源;
若 `start` 成功返回则总会调用 `stop`(失败时也调);但若 `start` 自身抛异常,则**不会**调用
`stop`。

```java
public interface TaskWorker {
  void start(Platform platform, TaskSpec spec, WorkerStatusTracker status,
             CompletableFuture<String> haltFuture) throws Exception;
  void stop(Platform platform) throws Exception;
}
```

然后用 `"class": "<你的-spec-全限定类名>"` 提交任务,它会走与内置实现相同的 coordinator →
agent → worker 管道。

## 配置

### HTTP 与工作负载默认值

以下为标准 Celeborn 配置项(类别 `trogdor`,自 0.7.0 起),可写入 `celeborn-defaults.conf`:

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `celeborn.trogdor.agent.http.host` | `0.0.0.0` | Trogdor agent HTTP 服务绑定的主机。用 `<localhost>` 可解析为本机主机名。 |
| `celeborn.trogdor.agent.http.port` | `19090` | Trogdor agent HTTP 服务绑定的端口(1024–65535)。 |
| `celeborn.trogdor.coordinator.http.host` | `0.0.0.0` | Trogdor coordinator HTTP 服务绑定的主机。 |
| `celeborn.trogdor.coordinator.http.port` | `19091` | Trogdor coordinator HTTP 服务绑定的端口(1024–65535)。 |
| `celeborn.trogdor.workload.master.host` | `localhost` | push/fetch 工作负载默认的 Celeborn master 主机。 |
| `celeborn.trogdor.workload.master.port` | `9097` | push/fetch 工作负载默认的 Celeborn master 端口。 |
| `celeborn.trogdor.workload.user.identifier` | `default:default` | 工作负载默认的 Celeborn 用户标识,形如 `<tenant>:<name>`。 |

### 混沌引擎

混沌引擎在运行时读取以下键(它们未注册为 `ConfigEntry`):

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `celeborn.trogdor.chaos.plan.participant.duration` | `5m` | 参与者任务的存活时长。 |
| `celeborn.trogdor.chaos.plan.action.default.interval` | `5s` | action 之间的默认间隔。 |
| `celeborn.trogdor.chaos.plan.action.selector.default.interval` | `5s` | 选择器默认间隔。 |
| `celeborn.trogdor.chaos.plan.action.occupycpu.maxduration` | `120s` | CPU 占用最大时长。 |
| `celeborn.trogdor.chaos.plan.action.block.bad.inflight.location` | `/root/badblock/inflight` | 坏块 inflight 标记文件位置。 |
| `celeborn.trogdor.chaos.runner.test.mode` | `false` | 以测试模式运行混沌 runner。 |
| `celeborn.trogdor.chaos.scripts.master.start.script` | `$CELEBORN_HOME/sbin/start-master.sh` | 启动 master 的脚本。 |
| `celeborn.trogdor.chaos.scripts.master.stop.script` | `$CELEBORN_HOME/sbin/stop-master.sh` | 停止 master 的脚本。 |
| `celeborn.trogdor.chaos.scripts.worker.start.script` | `$CELEBORN_HOME/sbin/start-worker.sh` | 启动 worker 的脚本。 |
| `celeborn.trogdor.chaos.scripts.worker.stop.script` | `$CELEBORN_HOME/sbin/stop-worker.sh` | 停止 worker 的脚本。 |

脚本默认值在 `$CELEBORN_HOME/sbin` 目录存在时解析为 `$CELEBORN_HOME/sbin/...`,否则为 `null`。

### 启动脚本

`sbin/start-trogdor-agent.sh` 与 `sbin/start-trogdor-coordinator.sh` 都会:

- 通过 `celeborn-daemon.sh` 启动单实例(入口类分别为 `TrogdorAgentApp` 与 `TrogdorCoordinatorApp`),
- 默认堆 `1g`,可用 `CELEBORN_TROGDOR_AGENT_MEMORY` / `CELEBORN_TROGDOR_COORDINATOR_MEMORY` 覆盖,
- 通过 `CELEBORN_TROGDOR_AGENT_JAVA_OPTS` / `CELEBORN_TROGDOR_COORDINATOR_JAVA_OPTS` 追加 JVM 选项,
- 加载 `$CELEBORN_HOME/sbin/load-celeborn-env.sh` 以获取共享环境变量。

## 平台限制

若干故障注入依赖仅 Linux 可用、且常需提权的 OS 工具:

| 故障 | 所需工具 | 平台 | 权限 |
|------|----------|------|------|
| ProcessStopFault | `pgrep`、`kill` | Linux/Unix | agent 自身子进程通常无需提权;其他用户进程可能需要提权。 |
| NetworkPartitionFault | `iptables` | 仅 Linux | 需要 `root` 或 `CAP_NET_ADMIN`。 |
| DiskSlowFault | `device-mapper` / `tc`(生产) | 仅 Linux | 需要 `root`。 |
| ExternalCommandFault | 用户指定 | 取决于命令 | 取决于命令。 |

在 macOS 与 Windows 上,这些故障要么不支持,要么需要等价的平台特定实现。

## 故障排查

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| agent `--status` 连接失败 | agent 未启动,或 `celeborn.trogdor.agent.http.port` 在 agent 与拓扑间不一致 | 启动 agent;确保拓扑中的 `trogdor.agent.port` 等于 agent 的 `http.port`。 |
| `createTask` 返回但任务卡在 `PENDING` | `startMs` 在未来,或 `runTask` 尚未调度 | 用 `startMs: 0` 立即开始;轮询 `--show-task`——`PENDING → RUNNING` 最多需要一个心跳(1s)。 |
| 任务以 `"No node names specified."` 报错转 `DONE` | spec 中 `targetNodes` 缺失/为空,或 spec 在 JSON 往返中丢失字段(某字段缺少 `@JsonProperty` getter) | 确保 `targetNodes` 存在且匹配拓扑节点名;自定义 spec 要为每个字段加 `@JsonProperty` getter(见[扩展 Trogdor](#扩展-trogdor))。 |
| 任务报 `"Unknown node names: ..."` | `targetNodes` 中有不在拓扑内的节点 | 使用 `trogdor.conf` 中定义的节点名。 |
| 故障 worker 报 `iptables`/`pgrep` not found | 缺少 OS 工具或 agent 非 root 运行 | 在装有所需工具与权限的 Linux 上运行 agent(见[平台限制](#平台限制))。 |
| coordinator 从不在 agent 上创建 worker | agent 不在拓扑中,或 coordinator 无法访问该节点的 `trogdor.agent.port` | 用正确的 `hostname`/`trogdor.agent.port` 将节点加入拓扑;检查 coordinator 与 agent 之间的网络/防火墙。 |
| `createTask` 时 `ClassNotFoundException` | spec 的 `class` 字段与真实全限定名不符,或该类不在 classpath 上 | 使用精确的包路径(如 `org.apache.celeborn.trogdor.workload.PushBenchSpec`);自定义 spec 需确保类在 agent classpath 上。 |

查看 coordinator/agent 内部日志:测试环境自带 `slf4j-simple` 绑定;生产环境使用标准 Celeborn 日志配置。

## 指标

Coordinator 与 Agent 都会注册一个 Trogdor 指标源,暴露当前任务/worker 数量及状态。当
`celeborn.metrics.enabled` 为 `true` 时,通过标准 Celeborn 指标端点暴露。
