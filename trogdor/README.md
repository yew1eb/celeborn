<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

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

### Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `celeborn.trogdor.chaos.plan.participant.duration` | `5m` | How long participant tasks stay alive. |
| `celeborn.trogdor.chaos.plan.action.default.interval` | `5s` | Default interval between actions. |
| `celeborn.trogdor.chaos.plan.action.selector.default.interval` | `5s` | Default selector interval. |
| `celeborn.trogdor.chaos.plan.action.occupycpu.maxduration` | `120s` | Maximum CPU occupation duration. |
| `celeborn.trogdor.chaos.scripts.master.start.script` | `$CELEBORN_HOME/sbin/start-master.sh` | Script to start a master. |
| `celeborn.trogdor.chaos.scripts.master.stop.script` | `$CELEBORN_HOME/sbin/stop-master.sh` | Script to stop a master. |
| `celeborn.trogdor.chaos.scripts.worker.start.script` | `$CELEBORN_HOME/sbin/start-worker.sh` | Script to start a worker. |
| `celeborn.trogdor.chaos.scripts.worker.stop.script` | `$CELEBORN_HOME/sbin/stop-worker.sh` | Script to stop a worker. |

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

## REST API

OpenAPI definitions for the Coordinator and Agent REST APIs are available at
`openapi/openapi-client/src/main/openapi3/trogdor_coordinator_rest_v1.yaml` and
`trogdor_agent_rest_v1.yaml`.
