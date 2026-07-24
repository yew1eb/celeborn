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

## Built-in Workloads

### PushBench

Pushes synthetic shuffle data to Celeborn as fast as possible.

```json
{
  "class": "org.apache.celeborn.trogdor.workload.PushBenchSpec",
  "startMs": 0,
  "durationMs": 60000,
  "targetNodes": ["agent-1"],
  "masterHost": "celeborn-master",
  "masterPort": 9097,
  "numMappers": 4,
  "numPartitions": 100,
  "bytesPerPush": 1024,
  "totalPushes": 100000,
  "userIdentifier": "default:default"
}
```

### FetchBench

Writes a small seed record to every partition and then repeatedly fetches all partitions.

```json
{
  "class": "org.apache.celeborn.trogdor.workload.FetchBenchSpec",
  "startMs": 0,
  "durationMs": 60000,
  "targetNodes": ["agent-1"],
  "masterHost": "celeborn-master",
  "masterPort": 9097,
  "numPartitions": 10,
  "fetchesPerPartition": 1000,
  "userIdentifier": "default:default"
}
```

### RpcBench

Benchmarks Celeborn RPC round-trip latency by sending synchronous ask requests to a local echo
endpoint.

```json
{
  "class": "org.apache.celeborn.trogdor.workload.RpcBenchSpec",
  "startMs": 0,
  "durationMs": 30000,
  "targetNodes": ["agent-1"],
  "totalRpcs": 5000,
  "payload": "hello"
}
```

The `masterHost`/`masterPort`/`userIdentifier` fields on the push/fetch benchmarks fall back to the
`celeborn.trogdor.workload.*` defaults when omitted.

## Built-in Faults

### ProcessStopFault

Pauses and resumes a process matching the given name using `pgrep` and `kill -STOP` / `kill -CONT`.
Requires Linux/Unix process management utilities.

```json
{
  "class": "org.apache.celeborn.trogdor.fault.ProcessStopFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["agent-1"],
  "processName": "celeborn-worker"
}
```

### NetworkPartitionFault

Blocks outbound traffic to a set of nodes using `iptables`. This fault requires root privileges and
only works on Linux with `iptables` available.

```json
{
  "class": "org.apache.celeborn.trogdor.fault.NetworkPartitionFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["agent-1"],
  "blockedNodes": ["agent-2", "agent-3"]
}
```

### DiskSlowFault

Simulates slow disk IO. The current implementation logs the intended delay; production deployments
should extend it to configure real block-device latency (for example via `device-mapper` or `tc`).

```json
{
  "class": "org.apache.celeborn.trogdor.fault.DiskSlowFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["agent-1"],
  "device": "/dev/sda",
  "delayMs": 100
}
```

### ExternalCommandFault

Runs an arbitrary external command on the target agent. The command is executed with the optional
environment variables and the worker completes when the process exits.

```json
{
  "class": "org.apache.celeborn.trogdor.fault.ExternalCommandFaultSpec",
  "startMs": 0,
  "durationMs": 10000,
  "targetNodes": ["agent-1"],
  "command": ["sleep", "5"],
  "env": {"KEY": "VALUE"}
}
```

## Chaos Testing

Celeborn Trogdor also integrates the chaos testing framework originally proposed in
CELEBORN-1492. A chaos plan describes a sequence of actions (for example `occupy-cpu`,
`stop-worker`, `hang-io`) together with a trigger and a checker. The coordinator-side
`ChaosOrchestrator` parses the plan and compiles it into native Trogdor tasks:

- One long-running `ChaosPlanSpec` participant task per target node.
- One `ChaosOperationSpec` task for each operation selected by the current action.

### Submitting a chaos plan

You can submit a plan either through the dedicated REST endpoint or by creating a
`ChaosPlanSpec` task.

```bash
curl -X POST http://localhost:19091/api/v1/trogdor/coordinator/chaos/plans \
  -H 'Content-Type: application/json' \
  -d '{
    "planId": "my-chaos-plan",
    "planJson": "{ \"actions\": [...], \"trigger\": {...}, \"checker\": \"dummy\" }",
    "targetNodes": ["agent-1"]
  }'
```

Alternatively:

```json
{
  "class": "org.apache.celeborn.trogdor.chaos.ChaosPlanSpec",
  "startMs": 0,
  "durationMs": 0,
  "targetNodes": ["agent-1"],
  "planJson": "{ \"actions\": [...], \"trigger\": {...}, \"checker\": \"dummy\" }"
}
```

### Supported actions

The following action identifiers are recognized in a plan:

- `occupy-cpu` — consumes CPU cores on the target node for a configurable duration.
- `start-master`, `stop-master`, `start-worker`, `stop-worker` — run the configured
  lifecycle scripts on the target node.
- `corrupt-disk`, `resume-disk`, `hang-io`, `resume-io` — manipulate disk availability
  and IO hang state (Linux only, often requires root).
- `corrupt-meta` — removes the configured HA master Ratis storage directory.

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

## Metrics

Both the Coordinator and the Agent register a Trogdor metrics source exposing the current number of
tasks/workers and their states. Metrics are served via the standard Celeborn metrics endpoint when
`celeborn.metrics.enabled` is `true`.
