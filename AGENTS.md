# Apache Celeborn — AI Coding Guide

This file provides guidance to AI coding assistants (Claude Code, Cursor, Copilot, etc.) working in this repository.

## Project Overview

Apache Celeborn is an elastic, high-performance shuffle service for distributed compute engines (Spark, Flink, MapReduce, Tez). It disaggregates compute from storage via a **Master-Worker-Client** architecture.

**Key Features:**
- Push-based shuffle write with merged shuffle read
- High availability via Raft consensus (Apache Ratis)
- Multi-engine: Spark 2.4/3.x/4.x, Flink 1.16-2.2, Hadoop MapReduce, Tez
- Multi-tenant dynamic configuration (SYSTEM < TENANT < TENANT_USER)

## Architecture

### Core Components

| Component | Location | Role |
|-----------|----------|------|
| **Master** | `master/` | Cluster resource management, worker registration, slot allocation, HA via Raft |
| **Worker** | `worker/` | Data push/fetch handling, storage management, partition flushing |
| **LifecycleManager** | `client/` | Control plane in Driver/JobMaster — manages shuffle metadata and slot allocation |
| **ShuffleClient** | `client/` | Data plane in Executor/TaskManager — handles read/write operations |

### Shuffle Data Flow

```
1. Mapper  → LifecycleManager.registerShuffle()
2. LifecycleManager → Master: requestSlots()
3. Master  → Workers: reserveSlots(), create partition files
4. Mappers → Workers: pushData() [Workers merge + optionally replicate]
5. Workers → Disk/HDFS/OSS: flush
6. Reducers → Workers: fetchChunk()
```

### Module Structure

| Module | Purpose |
|--------|---------|
| `common/` | Shared utilities, Netty RPC framework, Protobuf definitions, `CelebornConf` |
| `client/` | Engine-agnostic core client (`LifecycleManager`, `ShuffleClientImpl`) |
| `master/` | Master server (slot allocation, HA state machine) |
| `worker/` | Worker server (storage, memory management, congestion control) |
| `service/` | HTTP REST service base, dynamic config service |
| `spi/` | Service Provider Interface |
| `cli/` | Command-line interface |
| `client-spark/` | Spark 2.x/3.x/4.x shuffle clients (Java + Scala) |
| `client-flink/` | Flink 1.16–2.2 shuffle clients (Java) |
| `client-mr/` | Hadoop MapReduce client |
| `client-tez/` | Apache Tez client (experimental) |
| `tests/` | Integration tests per engine |

### Key Source Files

| File | Notes |
|------|-------|
| `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala` | All config definitions (~5000 lines), use builder pattern |
| `common/src/main/proto/TransportMessages.proto` | All RPC message definitions |
| `common/src/main/scala/org/apache/celeborn/common/rpc/` | Netty-based RPC framework |
| `master/src/main/scala/.../Master.scala` | Master entry point |
| `master/src/main/scala/.../SlotsAllocator.scala` | Slot allocation algorithms |
| `worker/src/main/scala/.../Worker.scala` | Worker entry point |
| `worker/src/main/scala/.../storage/StorageManager.scala` | Storage backend management |
| `client/src/main/scala/.../LifecycleManager.scala` | Shuffle lifecycle control plane |
| `client/src/main/java/.../ShuffleClientImpl.java` | Data plane client implementation |
| `client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/SparkShuffleManager.java` | Spark entry point |
| `client-flink/common/src/main/java/org/apache/celeborn/plugin/flink/RemoteShuffleServiceFactory.java` | Flink entry point |

## Build Commands

### Maven (Primary)

```bash
# Build core modules (master, worker, cli) — fastest
./build/mvn clean package -DskipTests

# Build with compute engine client
./build/mvn clean package -DskipTests -Pspark-3.5
./build/mvn clean package -DskipTests -Pspark-4.0
./build/mvn clean package -DskipTests -Pflink-1.20
./build/mvn clean package -DskipTests -Pmr

# Distribution package
./build/make-distribution.sh -Pspark-3.5
./build/make-distribution.sh -Pflink-1.20 --sbt-enabled
./build/make-distribution.sh -Pspark-3.4 -Paws    # AWS S3 support
./build/make-distribution.sh -Pspark-3.5 -Pjdk-21 # Java 21
```

### SBT (Alternative)

```bash
./build/sbt clean package
./build/sbt -Pspark-3.5 test
./build/sbt -Pflink-1.20 celeborn-flink-group/test
```

## Testing

```bash
# All unit tests
./build/mvn test

# Per module
./build/mvn test -pl common
./build/mvn test -pl master
./build/mvn test -pl worker

# Single test class
./build/mvn test -pl common -Dtest=ConfigurationSuite

# Single test method
./build/mvn test -pl master -Dtest=SlotsAllocatorSuiteJ#testAllocateSlotsForSinglePartitionId

# Integration tests
./build/mvn -Pspark-3.5 -pl tests/spark-it test
./build/mvn -Pflink-1.20 -pl tests/flink-it test
./build/mvn -Pmr -pl tests/mr-it test
```

## Code Style

```bash
# Format ALL code before committing (required)
./dev/reformat

# Format web UI code
./dev/reformat --web

# Verify formatting
./build/mvn spotless:check

# Verify license headers
./build/mvn org.apache.rat:apache-rat-plugin:check
```

**Import order** (enforced by Spotless): `javax.*`/`java.*` → `scala.*` → third-party → `org.apache.celeborn.*`

## Key Configuration

- **Config class**: `common/src/main/scala/org/apache/celeborn/common/CelebornConf.scala`
- **Main config file**: `conf/celeborn-defaults.conf`
- **Dynamic config levels** (ascending precedence): `SYSTEM` < `TENANT` < `TENANT_USER`
- **Dynamic config backends**: filesystem (`FS`) or database (`DB`)

## RPC and Protocol Buffers

- RPC framework: Netty-based, lives in `common/src/main/scala/org/apache/celeborn/common/rpc/`
- Protocol definitions: `common/src/main/proto/TransportMessages.proto`
- Protobuf compiled automatically during Maven `compile` phase

## Development Patterns

### Adding a Config Entry

1. Add to `CelebornConf.scala` using the builder pattern
2. Regenerate config docs:
   ```bash
   UPDATE=1 build/mvn test -pl common -Dtest=ConfigurationSuite
   ```

### Adding a Dependency

1. Add version to root `pom.xml` under `<dependencyManagement>`
2. Update `LICENSE-binary` with the dependency's license info

### Adding an RPC Message

1. Add to `common/src/main/proto/TransportMessages.proto`
2. Naming convention: `MessageName` + `MessageNameResponse`
3. Use `repeated` instead of `map` fields (avoids reflection overhead)

### Spark Client Integration

- Entry point: `SparkShuffleManager` (implements Spark's `ShuffleManager`)
- Location: `client-spark/spark-3/src/main/java/org/apache/spark/shuffle/celeborn/`
- Two writers: `HashBasedShuffleWriter`, `SortBasedShuffleWriter`

### Flink Client Integration

- Entry point: `RemoteShuffleServiceFactory` (standard) or `CelebornTierFactory` (hybrid shuffle)
- Location: `client-flink/common/src/main/java/org/apache/celeborn/plugin/flink/`

## PR Checklist

1. Run `./dev/reformat` (format all code)
2. If configs changed, regenerate docs (see above)
3. Ensure all relevant tests pass
4. Link to a `CELEBORN-XXXX` Jira ticket in the PR description
