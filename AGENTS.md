# Apache Celeborn™

## Project Overview

Apache Celeborn is an elastic and high-performance service for shuffle and spilled data in distributed compute engines. It improves the efficiency and elasticity of map-reduce engines by providing an elastic, high-efficient management service for intermediate data including shuffle data, spilled data, and result data.

### Key Features
- **Disaggregated Computing and Storage**: Separates compute from storage for better resource utilization
- **Push-based Shuffle Write and Merged Shuffle Read**: Reorganizes shuffle data for improved disk and network efficiency
- **High Availability and Fault Tolerance**: Master nodes use Raft consensus for HA
- **Multi-Engine Support**: Apache Spark (2.4/3.x/4.x), Apache Flink (1.16-2.2), Hadoop MapReduce, and Apache Tez
- **Multi-Tenant Support**: Dynamic configuration at SYSTEM, TENANT, and TENANT_USER levels

### Architecture Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Apache Celeborn Cluster                        │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐            │
│  │    Master    │────▶│    Master    │────▶│    Master    │  (Raft HA) │
│  │   (Leader)   │◀────│   (Follower) │◀────│   (Follower) │            │
│  └──────────────┘     └──────────────┘     └──────────────┘            │
│         │                                                             │
│    ┌────┴────┬────────┬────────┐                                      │
│    ▼         ▼        ▼        ▼                                      │
│ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐                                   │
│ │Worker│ │Worker│ │Worker│ │Worker│                                   │
│ │  1   │ │  2   │ │  3   │ │  4   │                                   │
│ └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘                                   │
└────┼────────┼────────┼────────┼────────────────────────────────────────┘
     │        │        │        │
     └────────┴────────┴────────┘
              ▲
              │ gRPC/Netty RPC
┌─────────────┴──────────────────────────────────────────────────────────┐
│                     Compute Engine (Spark/Flink/MR)                    │
│  ┌─────────────────┐         ┌─────────────────────────────────────┐  │
│  │ LifecycleManager│         │           ShuffleClient             │  │
│  │ (Driver/JM)     │         │    (Executor/TaskManager)           │  │
│  │ - Metadata mgmt │         │    - Push/Fetch data                │  │
│  │ - Slot allocation│        │    - Handle failures                │  │
│  └─────────────────┘         └─────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

## Technology Stack

### Core Technologies
| Component | Technology | Version |
|-----------|------------|---------|
| Build Tool | Apache Maven | 3.9.12 |
| Alternative Build | SBT | 1.9.4 |
| Language | Java | 8/11/17/21 |
| Language | Scala | 2.12.18 (default), 2.11, 2.13 |
| RPC Framework | gRPC | 1.44.0 |
| Netty | Netty | 4.2.10.Final |
| Consensus | Apache Ratis | 3.2.1 |
| Protocol Buffers | Protobuf | 3.25.5 |
| Metrics | Dropwizard Metrics | 4.2.25 |

### Client Support Matrix
| Engine | Scala 2.11 | Scala 2.12 | Scala 2.13 |
|--------|------------|------------|------------|
| Spark 2.4 | Java 8 | Java 8/11 | ❌ |
| Spark 3.0-3.5 | ❌ | Java 8/11/17 | Java 11/17 |
| Spark 4.0-4.1 | ❌ | ❌ | Java 17 |
| Flink 1.16-1.20 | ❌ | Java 8/11 | ❌ |
| Flink 2.0-2.2 | ❌ | Java 11/17 | Java 11/17 |

### Web UI
| Technology | Purpose |
|------------|---------|
| Vue 3 | Frontend Framework |
| TypeScript | Language |
| Vite | Build Tool |
| Naive UI | Component Library |
| Pinia | State Management |
| pnpm | Package Manager |

## Project Structure

```
celeborn/
├── pom.xml                          # Root Maven configuration
├── version.sbt                      # SBT version definition
├── build.sbt (generated)            # SBT build definition
│
├── bin/                             # Utility scripts
├── sbin/                            # Daemon control scripts (start/stop/status)
├── conf/                            # Configuration templates
├── build/                           # Build scripts and tools
│   ├── make-distribution.sh         # Main build script
│   ├── mvn                          # Maven wrapper
│   └── sbt                          # SBT wrapper
│
├── dev/                             # Development utilities
│   ├── reformat                     # Code formatting script
│   ├── merge_pr.py                  # PR merge script
│   └── dependencies.sh              # Dependency management
│
├── docs/                            # Documentation (MkDocs)
├── docker/                          # Dockerfile
├── charts/                          # Helm charts for Kubernetes
├── web/                             # Vue.js Web UI
│
├── project/                         # SBT build definitions
│   ├── CelebornBuild.scala          # Main SBT build configuration
│   └── plugins.sbt                  # SBT plugins
│
├── openapi/                         # OpenAPI specification and client
│   └── openapi-client/              # Generated REST client
│
├── spi/                             # Service Provider Interface
├── common/                          # Common utilities and protocols
│   └── src/main/proto/              # Protobuf definitions
│
├── client/                          # Core client library (engine-agnostic)
├── cli/                             # Command-line interface
├── service/                         # HTTP REST service
├── master/                          # Master server
├── worker/                          # Worker server
│
├── client-spark/                    # Spark client modules
│   ├── common/                      # Common Spark client code
│   ├── spark-2/                     # Spark 2.x support
│   ├── spark-3/                     # Spark 3.x support
│   └── spark-3-columnar-shuffle/    # Columnar shuffle optimization
│
├── client-flink/                    # Flink client modules
│   ├── common/                      # Common Flink client code
│   ├── flink-1.16 to flink-2.2/     # Version-specific modules
│   └── common-tiered/               # Tiered storage support
│
├── client-mr/                       # MapReduce client
├── client-tez/                      # Apache Tez client
│
├── multipart-uploader/              # S3/OSS multipart upload support
├── toolkit/                         # Utility toolkit
├── cpp/                             # C++ native components
└── tests/                           # Integration tests
    ├── spark-it/                    # Spark integration tests
    ├── flink-it/                    # Flink integration tests
    ├── mr-it/                       # MapReduce integration tests
    ├── tez-it/                      # Tez integration tests
    └── kubernetes-it/               # Kubernetes integration tests
```

## Build Commands

### Maven Build (Primary)

```bash
# Build with default profile (Java 8, Scala 2.12)
./build/mvn clean package -DskipTests

# Build for specific Spark version
./build/make-distribution.sh -Pspark-3.5
./build/make-distribution.sh -Pspark-2.4
./build/make-distribution.sh -Pspark-4.0

# Build for specific Flink version
./build/make-distribution.sh -Pflink-1.20
./build/make-distribution.sh -Pflink-2.0

# Build for MapReduce
./build/make-distribution.sh -Pmr

# Build with AWS S3 support
./build/make-distribution.sh -Pspark-3.4 -Paws

# Build with Aliyun OSS support
./build/make-distribution.sh -Pspark-3.4 -Paliyun

# Build for Java 21 (Spark 3.5/4.0 only)
./build/make-distribution.sh -Pspark-3.5 -Pjdk-21
```

### SBT Build (Alternative)

```bash
# Enable SBT in make-distribution.sh
./build/make-distribution.sh --sbt-enabled -Pspark-3.5

# Direct SBT commands
./build/sbt clean package
./build/sbt "project worker" run
```

### Web UI Build

```bash
cd web
pnpm install
pnpm run build
pnpm run dev        # Development server
```

### Documentation Build

```bash
# Using Make (requires Docker)
make docs
make docs-serve     # Serve locally on port 8000

# Or using Python directly
pip install -r requirements.txt
mkdocs build
mkdocs serve
```

## Testing Commands

### Unit Tests

```bash
# Run all unit tests
./build/mvn test

# Run tests for specific module
./build/mvn test -pl common
./build/mvn test -pl master
./build/mvn test -pl worker

# Run with specific profile
./build/mvn test -Pspark-3.5 -pl client-spark/spark-3
```

### Integration Tests

```bash
# Spark integration tests
./build/mvn -Pspark-3.5 -pl tests/spark-it test

# Flink integration tests
./build/mvn -Pflink-1.20 -pl tests/flink-it test

# MapReduce integration tests
./build/mvn -Pmr -pl tests/mr-it test
```

### Test Coverage

```bash
# Generate coverage report (JaCoCo)
./build/mvn clean test jacoco:report

# Coverage config in codecov.yml
# Reports uploaded to Codecov on CI
```

## Code Style Guidelines

### Java/Scala Code Formatting

The project uses **Spotless** with **Google Java Format** for code formatting.

```bash
# Apply formatting to all files
./dev/reformat

# Check formatting without applying
./build/mvn spotless:check

# Apply formatting to specific profile
./build/mvn spotless:apply -Pspark-3.5
```

### Scala Style Configuration

Scala formatting is controlled by `.scalafmt.conf`:
- Max column width: 100
- Runner dialect: scala212
- Import grouping: java → scala → third-party → celeborn
- Align: disabled (preset = none)

### Web UI Code Style

```bash
# Format and lint web code
cd web
pnpm run format
pnpm run lint
```

### Import Order

Required import order (enforced by Spotless):
1. `javax.*` / `java.*`
2. `scala.*`
3. Third-party libraries
4. `org.apache.celeborn.*`

### License Headers

All source files must include Apache License 2.0 header. Use RAT plugin to check:
```bash
./build/mvn org.apache.rat:apache-rat-plugin:check
```

## Configuration System

### Static Configuration

- **Location**: `$CELEBORN_HOME/conf/celeborn-defaults.conf`
- **Template**: `conf/celeborn-defaults.conf.template`
- **Class**: `CelebornConf`

### Dynamic Configuration

Supports three levels (in order of precedence):
1. **TENANT_USER**: Specific to tenant + user
2. **TENANT**: Specific to tenant
3. **SYSTEM**: System-wide defaults

Storage backends:
- **Filesystem**: `celeborn.dynamicConfig.store.backend=FS`
- **Database**: `celeborn.dynamicConfig.store.backend=DB`

### Key Configuration Files

| File | Purpose |
|------|---------|
| `conf/celeborn-defaults.conf` | Main server configuration |
| `conf/celeborn-env.sh` | Environment variables (memory, JVM opts) |
| `conf/log4j2.xml` | Logging configuration |
| `conf/metrics.properties` | Metrics reporting configuration |
| `conf/dynamicConfig.yaml` | Dynamic configuration (optional) |

## Security

### TLS/SSL Encryption

Enable TLS for different transport modules:
- `rpc_service`: Client ↔ Server communication
- `rpc_app`: LifecycleManager ↔ Executors
- `data`: Data push/fetch operations
- `replicate`: Worker-to-worker replication

```properties
# Enable TLS for RPC service
celeborn.ssl.rpc_service.enabled=true
celeborn.ssl.rpc_service.keyStore=/path/to/server.jks
celeborn.ssl.rpc_service.keyStorePassword=password
celeborn.ssl.rpc_service.trustStore=/path/to/truststore.jks
```

### Authentication (SASL)

```properties
# Enable authentication
celeborn.auth.enabled=true
celeborn.internal.port.enabled=true
```

**Note**: SASL requires internal port to be enabled.

## Deployment

### Binary Package Layout

```
apache-celeborn-0.7.0-SNAPSHOT-bin/
├── bin/                    # Utility scripts
├── sbin/                   # Start/stop scripts
├── conf/                   # Configuration files
├── jars/                   # Common JARs
├── master-jars/            # Master-specific JARs
├── worker-jars/            # Worker-specific JARs
├── cli-jars/               # CLI JARs
├── spark/                  # Spark client JARs (if built)
├── flink/                  # Flink client JARs (if built)
├── mr/                     # MapReduce client JARs (if built)
└── RELEASE                 # Release info
```

### Docker Deployment

```bash
# Build Docker image
docker build -f docker/Dockerfile .

# Base image: eclipse-temurin:8-jdk-noble
# Default user: celeborn (uid=10006)
# Default home: /opt/celeborn
```

### Kubernetes Deployment

Helm charts available in `charts/celeborn/`:
```bash
helm install celeborn charts/celeborn
```

## Development Workflow

### Creating a Pull Request

1. **Format code**: Run `./dev/reformat` before submitting
2. **Update docs**: If changing configs, run:
   ```bash
   UPDATE=1 build/mvn clean test -pl common -am -Dtest=none \
     -DwildcardSuites=org.apache.celeborn.ConfigurationSuite
   ```
3. **Check licenses**: Ensure all files have proper headers
4. **Run tests**: Ensure all relevant tests pass
5. **Jira ticket**: Link to CELEBORN-XXXX ticket

### Adding RPC Messages

When adding new RPC messages:
- Follow Protobuf naming: `RegisterWorker` and `RegisterWorkerResponse`
- Use `repeated` instead of `map` type fields
- Add to `common/src/main/proto/` directory

### Adding Dependencies

When introducing new dependencies:
1. Add to root `pom.xml` dependencyManagement
2. Update `LICENSE-binary` with license info
3. Ensure consistent versions across modules

### Protocol Buffer Generation

Protobuf files are in `common/src/main/proto/`:
```bash
# Generated automatically during Maven compile phase
# Or manually:
./build/mvn protobuf:compile
```

## REST API

Celeborn provides REST APIs for monitoring and management:

| Endpoint | Description |
|----------|-------------|
| `/conf` | List configuration |
| `/listDynamicConfigs` | List dynamic configs |
| `/masterGroupInfo` | Master HA information |
| `/workerInfo` | Worker status |
| `/shuffles` | Active shuffle information |

OpenAPI specification is in `openapi/` directory.

## Troubleshooting

### Common Build Issues

1. **Java version mismatch**: Ensure JAVA_HOME matches target profile
2. **Scala version conflicts**: Use `-Dscala.binary.version=2.12`
3. **Out of memory**: Increase MAVEN_OPTS: `-Xmx4g`

### Test Failures

Test logs are stored in:
- `**/target/test-reports/`
- `**/target/unit-tests.log`

### CI/CD

GitHub Actions workflows:
- `maven.yml`: Main CI workflow (Java 8/11/17, Spark/Flink/MR tests)
- `sbt.yml`: SBT build verification
- `style.yml`: Code style checks
- `cpp_integration.yml`: C++ components
- `integration.yml`: Integration tests

## Resources

- **Website**: https://celeborn.apache.org/
- **Documentation**: https://celeborn.apache.org/docs/
- **Jira**: https://issues.apache.org/jira/projects/CELEBORN
- **Slack**: https://join.slack.com/t/apachecelebor-kw08030/shared_invite/...
- **Git Repository**: https://gitbox.apache.org/repos/asf/celeborn.git
