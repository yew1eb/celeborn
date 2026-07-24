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

Celeborn Trogdor is a distributed testing and fault-injection framework inspired by
[Apache Kafka Trogdor](https://kafka.apache.org/documentation/#trogdor). It runs as independent
Coordinator and Agent processes and can submit long-running workloads or inject faults into a
Celeborn cluster.

## Architecture

- **Trogdor Coordinator** maintains the task state machine and schedules tasks onto agents.
- **Trogdor Agent** runs on the target nodes and executes the actual workload/fault workers.
- **TaskSpec** describes what to run; the JSON `class` field selects the concrete implementation.
- **TaskController** (coordinator side) decides which agent nodes should run the task.
- **TaskWorker** (agent side) performs the actual work in a background thread.

The coordinator and agent communicate over HTTP. The coordinator polls each agent's status on a
fixed heartbeat (1s by default), reconciles the desired set of workers with what the agent reports,
and advances the task state machine once every worker for a task reaches `DONE`.

### Data flow

```
                        createTask(TaskSpec)
   client ──────────────────────────────────────►  Coordinator
                                                       │
                            heartbeat (1s)             │ runTask: assign workerId,
                  ┌────────────────────────────────────│  schedule stop at endMs
                  ▼                                    ▼
               Agent  ◄────── createWorker ──────  NodeManager
            (per node)           stopWorker          (per node)
                 │
                 │ WorkerManager.start(TaskWorker)
                 ▼
           TaskWorker ── haltFuture.complete("") ──►  worker DONE
                                                      │
                  heartbeat reports worker states ◄──┘
                  → maybeFinishTask when all DONE
```

### Task state machine

```
   PENDING ──runTask──► RUNNING ──all workers DONE──► DONE
      │                    │
      │ stopTask           │ stopTask
      ▼                    ▼
   DONE(err=stopped)   STOPPING ──► DONE
```

A task is `PENDING` until `runTask` fires (immediately when `startMs <= now`, the common case since
the coordinator rebases a past `startMs` to `now` on submission). It becomes `RUNNING` once workers
are assigned, `STOPPING` on `stopTask`, and `DONE` once every worker reports `DONE`. The task
`error` field is non-empty when a worker fails or `runTask` throws (e.g. unknown target node).

## Quick Start

### 1. Configure the cluster topology

Trogdor uses a JSON topology file to describe which nodes exist and how to reach their agents.
Copy the template and edit it for your cluster:

```shell
cd $CELEBORN_HOME/conf
cp trogdor.conf.template trogdor.conf
```

`trogdor.conf` is a JSON document (lines starting with `#` are comments):

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

Each node entry carries a `hostname` and a `config` map. The `trogdor.agent.port` value must match
the HTTP port the agent on that node binds to (see `celeborn.trogdor.agent.http.port`).

### 2. Start the agent(s)

Start an agent on every node listed in the topology:

```shell
cd $CELEBORN_HOME
./sbin/start-trogdor-agent.sh
```

By default the agent binds to `0.0.0.0:19090`. Override with the standard Celeborn config keys, for
example in `$CELEBORN_HOME/conf/celeborn-defaults.conf`:

```shell
celeborn.trogdor.agent.http.host=0.0.0.0
celeborn.trogdor.agent.http.port=19090
```

### 3. Start the coordinator

Start a single coordinator (it does not need to run on a Celeborn master/worker node):

```shell
cd $CELEBORN_HOME
./sbin/start-trogdor-coordinator.sh
```

By default the coordinator binds to `0.0.0.0:19091`:

```shell
celeborn.trogdor.coordinator.http.host=0.0.0.0
celeborn.trogdor.coordinator.http.port=19091
```

### 4. Verify the services are up

```shell
# Agent status (lists workers running on the agent)
$ celeborn-cli trogdor agent -t localhost:19090 --status

# Coordinator status
$ celeborn-cli trogdor coordinator -t localhost:19091 --status

# Or with plain curl
$ curl http://localhost:19090/api/v1/trogdor/agent/status
$ curl http://localhost:19091/api/v1/trogdor/coordinator/status
```

### 5. Submit a task

Write a task spec to a JSON file (the `class` field selects the worker implementation) and create it
on the coordinator:

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

See [Built-in Workloads](#built-in-workloads), [Built-in Faults](#built-in-faults) and
[Chaos Testing](#chaos-testing) for the available spec classes and their JSON shapes.

## Task spec conventions

Every task spec is a polymorphic JSON object (see [Extending Trogdor](#extending-trogdor)):

- `class` (required) — the fully-qualified implementation class, e.g.
  `org.apache.celeborn.trogdor.workload.PushBenchSpec`. This selects the `TaskWorker`; the value
  must exactly match the source package path or deserialization fails.
- `startMs` (long) — start time in epoch milliseconds. Use `0` to start immediately; the
  coordinator rebases a past `startMs` to `now` on submission.
- `durationMs` (long) — how long the task runs, in milliseconds. Clamped to
  `[0, 1000000000000000]`. The coordinator schedules an automatic `stopTask` at `startMs + durationMs`.
- `targetNodes` (array of string) — the agent node names (from the topology) to run the task on.

Fields marked *required* in the tables below have no default and must be present in the JSON.

## Built-in Workloads

### PushBench

Pushes synthetic shuffle data to Celeborn as fast as possible.

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

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Agent node names to push from |
| `masterHost` | `string` | yes | Celeborn master host |
| `masterPort` | `int` | yes | Celeborn master port |
| `numMappers` | `int` | yes | Concurrent mappers (clamped to `>= 1`) |
| `numPartitions` | `int` | yes | Number of partitions (clamped to `>= 1`) |
| `bytesPerPush` | `int` | yes | Bytes per push (clamped to `>= 1`) |
| `totalPushes` | `long` | yes | Total push count (clamped to `>= 0`) |
| `userIdentifier` | `string` | no | `<tenant>:<name>`; defaults to `"default"` |

### FetchBench

Writes a small seed record to every partition and then repeatedly fetches all partitions.

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

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Agent node names to fetch from |
| `masterHost` | `string` | yes | Celeborn master host |
| `masterPort` | `int` | yes | Celeborn master port |
| `numPartitions` | `int` | yes | Number of partitions (clamped to `>= 1`) |
| `fetchesPerPartition` | `long` | yes | Fetches per partition (clamped to `>= 0`) |
| `userIdentifier` | `string` | no | `<tenant>:<name>`; defaults to `"default"` |

### RpcBench

Benchmarks Celeborn RPC round-trip latency by sending synchronous ask requests to a local echo
endpoint.

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

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Agent node names to run on |
| `totalRpcs` | `long` | yes | Total RPC count (clamped to `>= 0`) |
| `payload` | `string` | no | Echo payload; defaults to `"hello"` |

The `masterHost`/`masterPort`/`userIdentifier` fields on the push/fetch benchmarks fall back to the
`celeborn.trogdor.workload.*` defaults when omitted.

## Built-in Faults

Faults share the same task-spec shape (the `class` field selects the fault) but run on the target
agent's host rather than against the Celeborn cluster. See [Fault injection walkthrough](#fault-injection-walkthrough)
for an end-to-end example.

### ProcessStopFault

Pauses and resumes a process matching the given name using `pgrep` and `kill -STOP` / `kill -CONT`.
Requires Linux/Unix process management utilities.

```json
{
  "class": "org.apache.celeborn.trogdor.fault.ProcessStopFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["node0"],
  "processName": "celeborn-worker"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Nodes to inject on |
| `processName` | `string` | yes | Process name to pause/resume (matched via `pgrep`) |

### NetworkPartitionFault

Blocks outbound traffic to a set of nodes using `iptables`. This fault requires root privileges and
only works on Linux with `iptables` available.

```json
{
  "class": "org.apache.celeborn.trogdor.fault.NetworkPartitionFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["worker-0"],
  "blockedNodes": ["worker-1", "master-0"]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Source nodes that will be partitioned |
| `blockedNodes` | `string[]` | yes | Nodes that `targetNodes` cannot reach |

### DiskSlowFault

Simulates slow disk IO. The current implementation logs the intended delay; production deployments
should extend it to configure real block-device latency (for example via `device-mapper` or `tc`).

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

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Nodes to inject on |
| `device` | `string` | yes | Block device path (e.g. `/dev/sda`) |
| `delayMs` | `long` | yes | Simulated IO delay in milliseconds |

### ExternalCommandFault

Runs an arbitrary external command on the target agent. The command is executed with the optional
environment variables and the worker completes when the process exits.

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

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `targetNodes` | `string[]` | yes | Nodes to run the command on |
| `command` | `string[]` | yes | Command as an argv array |
| `env` | `map<string,string>` | no | Environment variables for the process |

## Fault injection walkthrough

This walks through injecting a `ProcessStopFault` against a running Celeborn worker, observing the
effect, and cleaning up. The flow is the same for every fault — only the spec differs.

1. **Pick a target.** Suppose a Celeborn worker process named `celeborn-worker` is running on
   topology node `node0`. Confirm the agent is up and the worker process exists:

   ```shell
   $ celeborn-cli trogdor agent -t localhost:19090 --status
   $ pgrep -f celeborn-worker   # on node0
   ```

2. **Write the fault spec.** Pause the worker for 10 seconds (the fault auto-resumes when it ends):

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

3. **Submit and watch the task state.**

   ```shell
   $ celeborn-cli trogdor coordinator -t localhost:19091 \
       --create-task /tmp/procstop.json -i procstop-1
   $ celeborn-cli trogdor coordinator -t localhost:19091 --show-task -i procstop-1
   ```

   The task moves `PENDING → RUNNING → DONE`. While `RUNNING` the target process is stopped
   (`T` state in `ps`/`top`); it resumes when the task reaches `DONE`.

4. **Inspect the agent-side worker.** The agent status response lists the fault worker and its
   state; `error` is empty on a clean run:

   ```shell
   $ celeborn-cli trogdor agent -t localhost:19090 --status
   ```

5. **Clean up.** Once `DONE` the task record stays on the coordinator for inspection; remove it
   with `destroy`:

   ```shell
   $ celeborn-cli trogdor coordinator -t localhost:19091 --destroy-task -i procstop-1
   ```

   To cut a fault short before its `durationMs` elapses, use `--stop-task` instead; the worker's
   `stop()` resumes the process and the task moves to `STOPPING → DONE`.

> **Note:** `NetworkPartitionFault`/`DiskSlowFault` need root or capabilities — run the agent as
> root (or grant `CAP_NET_ADMIN`) and confirm the OS tools exist; otherwise the worker errors with
> a command-not-found message. See [Platform limitations](#platform-limitations).

## Chaos Testing

Celeborn Trogdor also integrates the chaos testing framework originally proposed in
CELEBORN-1492. A chaos plan describes a sequence of actions (for example `occupy-cpu`,
`stop-worker`, `hang-io`) together with a trigger and a checker. The coordinator-side
`ChaosOrchestrator` parses the plan and compiles it into native Trogdor tasks:

- One long-running `ChaosPlanSpec` participant task per target node.
- One `ChaosOperationSpec` task for each operation selected by the current action.

### Plan structure

A chaos plan is a JSON object with three top-level keys. It is submitted as the `planJson` **string
field** of a `ChaosPlanSpec` (or `SubmitChaosPlanRequest`) — i.e. it is embedded as an escaped JSON
string, not as a nested object.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `actions` | array | yes | — | List of action objects |
| `trigger` | object | yes | — | Trigger policy |
| `checker` | string | no | `dummy` | Checker type: `dummy` or `resource` |

**action object:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `id` | string | yes | — | Action id (see below) |
| `selector` | object | no | `dummySelector` | Node/disk selector |
| `cores` | int | only `occupy-cpu` | — | CPU cores to occupy |
| `duration` | string | only `occupy-cpu` | `10s` | Per-burst CPU occupation (capped by `chaos.plan.action.occupycpu.maxduration`) |

**selector object:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | string | no | `assign` | `assign` (fixed nodes) or `random` |
| `interval` | string | no | `5s` | Selector interval (time string) |
| `indices` | `int[]` | `assign` only | — | 0-based node indices |
| `device` | `int[]` | no | `[]` | Disk indices (disk actions only) |

**trigger object:**

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `policy` | string | no | `random` | `random` or `sequence` |
| `repeat` | int | no | `1` | Number of repetitions |
| `interval` | object | yes | — | Interval object (see below) |

trigger `interval` object:

| `type` | Extra fields | Default | Description |
|--------|--------------|---------|-------------|
| `fix` | `value` (string) | `5s` | Fixed interval |
| `range` | `start`, `end` (strings) | `5s`–`10s` | Random interval in `[start, end)` |

### Action ids

| id | Action class | Extra fields |
|----|--------------|--------------|
| `occupy-cpu` | `OccupyCpuAction` | `cores`, `duration` |
| `corrupt-disk` | `CorruptDiskAction` | — |
| `resume-disk` | `ResumeDiskAction` | — |
| `hang-io` | `HangIoAction` | — |
| `resume-io` | `ResumeIoAction` | — |
| `corrupt-meta` | `CorruptMetaAction` | — |
| `start-master` | `StartMasterAction` | — (script from `ChaosConf`) |
| `stop-master` | `StopMasterAction` | — (script from `ChaosConf`) |
| `start-worker` | `StartWorkerAction` | — (script from `ChaosConf`) |
| `stop-worker` | `StopWorkerAction` | — (script from `ChaosConf`) |

Unknown ids are rejected with a `PlanInvalidException`. Random trigger combined with
`corrupt-meta` is also rejected (it is not reversible).

### Submitting a chaos plan

Minimal plan (one `occupy-cpu` burst, sequence trigger):

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

Submit it through the dedicated REST endpoint:

```bash
curl -X POST http://localhost:19091/api/v1/trogdor/coordinator/chaos/plans \
  -H 'Content-Type: application/json' \
  -d '{
    "planId": "my-chaos-plan",
    "planJson": "<plan-json-from-above-as-escaped-string>",
    "targetNodes": ["node0"]
  }'
```

Or with the CLI (`--submit-chaos-plan` reads a `SubmitChaosPlanRequest` JSON file containing
`planId` / `planJson` / `targetNodes`):

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

Alternatively, submit the plan as a `ChaosPlanSpec` task (the coordinator routes a `ChaosPlanSpec`
to the orchestrator automatically):

```json
{
  "class": "org.apache.celeborn.trogdor.chaos.ChaosPlanSpec",
  "startMs": 0,
  "durationMs": 0,
  "targetNodes": ["node0"],
  "planJson": "{ \"actions\": [...], \"trigger\": {...}, \"checker\": \"dummy\" }"
}
```

Inspect and stop a plan (`-i` doubles as the plan id):

```shell
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-chaos-plan -i my-chaos-plan
$ celeborn-cli trogdor coordinator -t localhost:19091 --stop-chaos-plan -i my-chaos-plan
```

## CLI

The `trogdor` group is registered on `celeborn-cli` alongside `master` and `worker`. All trogdor
subcommands share the standard `-h/--help` and `-V/--version` flags, and locate their target
service with `-t, --target <host:port>` (the host portion may be an IPv6 literal).

```
celeborn-cli
└── trogdor
    ├── agent ...        # agent operations    (-t localhost:19090)
    └── coordinator ... # coordinator ops    (-t localhost:19091)
```

### `celeborn-cli trogdor agent`

| Flag | Description |
|------|-------------|
| `-t, --target <host:port>` | Agent host:port, e.g. `localhost:19090` (required) |
| `--status` | Show agent status |
| `--uptime` | Show agent uptime |

```shell
$ celeborn-cli trogdor agent -t localhost:19090 --status
$ celeborn-cli trogdor agent -t localhost:19090 --uptime
```

### `celeborn-cli trogdor coordinator`

| Flag | Description |
|------|-------------|
| `-t, --target <host:port>` | Coordinator host:port, e.g. `localhost:19091` (required) |
| `--status` | Show coordinator status |
| `--uptime` | Show coordinator uptime |
| `--create-task <file>` | Create a task from a JSON spec file (requires `-i`) |
| `-i, --task-id <id>` | Task id (also used as the plan id for chaos plan show/stop) |
| `--show-task` | Show a single task (requires `-i`) |
| `--show-tasks` | Show all tasks |
| `--stop-task` | Stop a task (requires `-i`) |
| `--destroy-task` | Destroy a task (requires `-i`) |
| `--submit-chaos-plan <file>` | Submit a chaos plan from a `SubmitChaosPlanRequest` JSON file |
| `--show-chaos-plan` | Show the status of a chaos plan (uses `-i` as the plan id) |
| `--stop-chaos-plan` | Stop a chaos plan (uses `-i` as the plan id) |

```shell
# create / inspect / stop / destroy a task
$ celeborn-cli trogdor coordinator -t localhost:19091 --create-task /tmp/noop.json -i noop-1
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-tasks
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-task -i noop-1
$ celeborn-cli trogdor coordinator -t localhost:19091 --stop-task -i noop-1
$ celeborn-cli trogdor coordinator -t localhost:19091 --destroy-task -i noop-1

# submit / inspect / stop a chaos plan (-i doubles as the plan id)
$ celeborn-cli trogdor coordinator -t localhost:19091 --submit-chaos-plan /tmp/plan.json
$ celeborn-cli trogdor coordinator -t localhost:19091 --show-chaos-plan -i my-chaos-plan
$ celeborn-cli trogdor coordinator -t localhost:19091 --stop-chaos-plan -i my-chaos-plan
```

The `--create-task` file is deserialized as a polymorphic `TaskSpec` (the `class` field selects the
implementation). The `--submit-chaos-plan` file is deserialized as a `SubmitChaosPlanRequest`
(`planId`, `planJson`, `targetNodes`).

## REST API

All endpoints are JSON and live under `/api/v1/trogdor`. OpenAPI definitions are available at
`openapi/openapi-client/src/main/openapi3/trogdor_coordinator_rest_v1.yaml` and
`trogdor_agent_rest_v1.yaml`.

### Coordinator — `http://<coordinator>:19091/api/v1/trogdor/coordinator`

| Method | Path | Body / Param | Description |
|--------|------|---------------|-------------|
| GET | `/status` | — | Coordinator start time |
| GET | `/uptime` | — | Server uptime |
| POST | `/tasks` | `CreateTaskRequest` | Create a task from a spec |
| GET | `/tasks` | — | List all tasks |
| GET | `/tasks/{taskId}` | `taskId` | Show one task state |
| PUT | `/tasks/{taskId}/stop` | `taskId` | Stop a task |
| DELETE | `/tasks/{taskId}` | `taskId` | Destroy a task |
| POST | `/chaos/plans` | `SubmitChaosPlanRequest` | Submit a chaos plan |
| GET | `/chaos/plans/{planId}` | `planId` | Show chaos plan status |
| PUT | `/chaos/plans/{planId}/stop` | `planId` | Stop a chaos plan |

### Agent — `http://<agent>:19090/api/v1/trogdor/agent`

| Method | Path | Body / Param | Description |
|--------|------|---------------|-------------|
| GET | `/status` | — | Agent status and worker states |
| GET | `/uptime` | — | Server uptime |
| POST | `/workers` | `CreateWorkerRequest` | Create a worker on the agent |
| PUT | `/workers/{workerId}/stop` | `workerId` | Stop a worker |
| DELETE | `/workers/{workerId}` | `workerId` | Destroy a worker |

## Extending Trogdor

Trogdor is a framework: the built-in workloads and faults are just `TaskSpec` implementations. To
add your own workload or fault, implement three pieces — a spec, a controller, and a worker — and
register them by their fully-qualified class name (no separate registry is needed).

### Spec

Extend `TaskSpec` and annotate a single constructor with `@JsonCreator`. The first two parameters
are conventionally `startMs`/`durationMs`, forwarded to `super(...)`. Every parameter gets a
`@JsonProperty("...")` naming its JSON field, and every field you want serialized gets a
`@JsonProperty` getter (otherwise it is dropped on the JSON round-trip — see the NoOpTaskSpec fix
referenced in the changelog). Example shape, mirroring `PushBenchSpec`:

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")  // inherited from TaskSpec
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

`TaskController` is a single-method functional interface — return the target node names:

```java
public interface TaskController {
  Set<String> targetNodes(Topology topology);
}
```

### Worker

`TaskWorker` has `start` and `stop`. `start` must return quickly — push long work onto a background
thread — and complete `haltFuture` when done: an empty string means success, a non-empty string is
treated as an error. `stop` releases all resources; it is always called if `start` returned
successfully (even on failure), but **not** called if `start` itself threw.

```java
public interface TaskWorker {
  void start(Platform platform, TaskSpec spec, WorkerStatusTracker status,
             CompletableFuture<String> haltFuture) throws Exception;
  void stop(Platform platform) throws Exception;
}
```

Then submit a task with `"class": "<fqcn-of-your-spec>"` and it runs through the same coordinator →
agent → worker pipeline as the built-ins.

## Configuration

### HTTP and workload defaults

These are registered as standard Celeborn config entries (category `trogdor`, since 0.7.0) and can
be set in `celeborn-defaults.conf`:

| Key | Default | Description |
|-----|---------|-------------|
| `celeborn.trogdor.agent.http.host` | `0.0.0.0` | Host to bind the Trogdor agent HTTP server to. Use `<localhost>` to resolve the local hostname. |
| `celeborn.trogdor.agent.http.port` | `19090` | Port to bind the Trogdor agent HTTP server to (1024–65535). |
| `celeborn.trogdor.coordinator.http.host` | `0.0.0.0` | Host to bind the Trogdor coordinator HTTP server to. |
| `celeborn.trogdor.coordinator.http.port` | `19091` | Port to bind the Trogdor coordinator HTTP server to (1024–65535). |
| `celeborn.trogdor.workload.master.host` | `localhost` | Default Celeborn master host for push/fetch benchmarks. |
| `celeborn.trogdor.workload.master.port` | `9097` | Default Celeborn master port for push/fetch benchmarks. |
| `celeborn.trogdor.workload.user.identifier` | `default:default` | Default Celeborn user identifier for benchmarks, in the form `<tenant>:<name>`. |

### Chaos engine

The chaos engine reads these keys at runtime (they are not registered as `ConfigEntry`s):

| Key | Default | Description |
|-----|---------|-------------|
| `celeborn.trogdor.chaos.plan.participant.duration` | `5m` | How long participant tasks stay alive. |
| `celeborn.trogdor.chaos.plan.action.default.interval` | `5s` | Default interval between actions. |
| `celeborn.trogdor.chaos.plan.action.selector.default.interval` | `5s` | Default selector interval. |
| `celeborn.trogdor.chaos.plan.action.occupycpu.maxduration` | `120s` | Maximum CPU occupation duration. |
| `celeborn.trogdor.chaos.plan.action.block.bad.inflight.location` | `/root/badblock/inflight` | Bad-block inflight marker file location. |
| `celeborn.trogdor.chaos.runner.test.mode` | `false` | Run the chaos runner in test mode. |
| `celeborn.trogdor.chaos.scripts.master.start.script` | `$CELEBORN_HOME/sbin/start-master.sh` | Script to start a master. |
| `celeborn.trogdor.chaos.scripts.master.stop.script` | `$CELEBORN_HOME/sbin/stop-master.sh` | Script to stop a master. |
| `celeborn.trogdor.chaos.scripts.worker.start.script` | `$CELEBORN_HOME/sbin/start-worker.sh` | Script to start a worker. |
| `celeborn.trogdor.chaos.scripts.worker.stop.script` | `$CELEBORN_HOME/sbin/stop-worker.sh` | Script to stop a worker. |

The script defaults resolve to `$CELEBORN_HOME/sbin/...` when that directory exists, otherwise to
`null`.

### Startup scripts

Both `sbin/start-trogdor-agent.sh` and `sbin/start-trogdor-coordinator.sh`:

- start a single instance via `celeborn-daemon.sh` (entry classes `TrogdorAgentApp` and
  `TrogdorCoordinatorApp` respectively),
- default to a `1g` heap, overridable via `CELEBORN_TROGDOR_AGENT_MEMORY` /
  `CELEBORN_TROGDOR_COORDINATOR_MEMORY`,
- accept extra JVM options via `CELEBORN_TROGDOR_AGENT_JAVA_OPTS` /
  `CELEBORN_TROGDOR_COORDINATOR_JAVA_OPTS`,
- load `$CELEBORN_HOME/sbin/load-celeborn-env.sh` for shared environment.

## Platform Limitations

Several fault injections rely on OS-level tools that are only available on Linux and often require
elevated privileges:

| Fault | Required tools | Platform | Privileges |
|-------|----------------|----------|------------|
| ProcessStopFault | `pgrep`, `kill` | Linux/Unix | Usually none for the agent's own child processes; may need privileges for other users' processes. |
| NetworkPartitionFault | `iptables` | Linux only | Requires `root` or `CAP_NET_ADMIN`. |
| DiskSlowFault | `device-mapper` / `tc` (production) | Linux only | Requires `root`. |
| ExternalCommandFault | User-specified | Depends on command | Depends on command. |

On macOS and Windows these faults are either unsupported or require equivalent platform-specific
implementations.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Agent `--status` fails to connect | Agent not started, or `celeborn.trogdor.agent.http.port` mismatched between agent and topology | Start the agent; ensure the topology `trogdor.agent.port` equals the agent's `http.port`. |
| `createTask` returns but task is stuck in `PENDING` | `startMs` is in the future, or `runTask` has not been scheduled yet | Use `startMs: 0` to start immediately; poll `--show-task` — a `PENDING → RUNNING` transition takes up to one heartbeat (1s). |
| Task goes `DONE` with error `"No node names specified."` | `targetNodes` is missing/empty in the spec, or the spec lost fields in the JSON round-trip (a spec without a `@JsonProperty` getter) | Ensure `targetNodes` is present and matches a topology node name; for custom specs, give every field a `@JsonProperty` getter (see [Extending Trogdor](#extending-trogdor)). |
| Task error `"Unknown node names: ..."` | A `targetNodes` entry is not in the topology | Use a node name defined in `trogdor.conf`. |
| Fault worker errors with `iptables`/`pgrep` not found | OS tool missing or agent not running as root | Run the agent on Linux with the required tools and privileges (see [Platform limitations](#platform-limitations)). |
| Coordinator never creates workers on the agent | Agent not in the topology, or coordinator cannot reach `trogdor.agent.port` on that node | Add the node to the topology with the correct `hostname`/`trogdor.agent.port`; check network/firewall between coordinator and agent. |
| `ClassNotFoundException` on `createTask` | The `class` field in the spec does not match a real FQN, or the class is not on the classpath | Use the exact package path (e.g. `org.apache.celeborn.trogdor.workload.PushBenchSpec`); for custom specs, ensure the class is on the agent classpath. |

To see coordinator/agent internals, enable logging (the tests ship with an `slf4j-simple`
binding; in production use the standard Celeborn log configuration).

## Metrics

Both the Coordinator and the Agent register a Trogdor metrics source exposing the current number of
tasks/workers and their states. Metrics are served via the standard Celeborn metrics endpoint when
`celeborn.metrics.enabled` is `true`.
