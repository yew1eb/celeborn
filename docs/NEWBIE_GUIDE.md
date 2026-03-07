# Apache Celeborn 新手贡献指南 🤝

> 欢迎来到 Apache Celeborn 社区！本指南专为第一次参与开源贡献的新手设计。

## 目录
1. [开始之前](#1-开始之前)
2. [第一个贡献](#2-第一个贡献)
3. [进阶任务](#3-进阶任务)
4. [学习资源](#4-学习资源)
5. [常见问题](#5-常见问题)

---

## 1. 开始之前

### 1.1 开发环境准备

```bash
# 1. 克隆代码库
git clone https://github.com/apache/celeborn.git
cd celeborn

# 2. 安装依赖（Java 8/11/17，Maven 3.6+）
java -version  # 确认 Java 版本
mvn -version   # 确认 Maven 版本

# 3. 编译项目（跳过测试以加快速度）
./build/mvn clean package -DskipTests

# 4. 运行代码格式化工具
./dev/reformat
```

### 1.2 项目结构速览

```
celeborn/
├── common/              # ✅ 新手推荐：基础工具类、协议定义
├── client/              # ✅ 新手推荐：客户端逻辑
├── master/              # ⚠️ 中等难度：集群管理
├── worker/              # ⚠️ 中等难度：数据存储
├── service/             # ✅ 新手推荐：HTTP 服务、配置
├── cli/                 # ✅ 新手推荐：命令行工具
├── tests/               # ✅ 新手推荐：测试用例
└── docs/                # ✅ 新手推荐：文档改进
```

### 1.3 新手友好的标签

在 Jira 上寻找这些标签的问题：
- `newbie` / `good-first-issue` - 适合新手的简单任务
- `documentation` - 文档改进
- `test` - 测试补充
- `refactoring` - 代码重构

---

## 2. 第一个贡献

### 🎯 推荐的首个贡献类型

#### 类型 1: 文档改进 (难度 ⭐)

**适合场景**：
- 发现文档有拼写错误或表述不清
- 某个配置项缺少说明
- 缺少示例代码

**示例任务**：
```markdown
任务：为 celeborn.worker.storage.dirs 配置添加示例

位置：docs/configuration/worker.md

改进内容：
- 添加配置示例
- 解释每个参数的含义
- 给出生产环境建议
```

**如何开始**：
1. 阅读现有文档，找出改进点
2. 在 docs/ 目录下找到对应文件
3. 修改后运行 `make docs-serve` 预览
4. 提交 PR

---

#### 类型 2: 单元测试补充 (难度 ⭐⭐)

**适合场景**：
- 提高代码覆盖率
- 验证边界条件
- 补充缺失的测试场景

**示例任务 1 - 测试工具类**：
```scala
// common/src/test/scala/org/apache/celeborn/common/util/StringUtilsSuite.scala
package org.apache.celeborn.common.util

class StringUtilsSuite extends CelebornFunSuite {
  
  test("bytesToString should format bytes correctly") {
    assert(Utils.bytesToString(1024) === "1.0 KiB")
    assert(Utils.bytesToString(1024 * 1024) === "1.0 MiB")
    assert(Utils.bytesToString(0) === "0.0 B")
  }
  
  test("bytesToString should handle edge cases") {
    assert(Utils.bytesToString(-1) === "0.0 B")  // 发现 bug!
    assert(Utils.bytesToString(Long.MaxValue) === "8.0 EiB")
  }
}
```

**示例任务 2 - 测试配置类**：
```scala
// common/src/test/scala/org/apache/celeborn/common/CelebornConfSuite.scala
class CelebornConfSuite extends CelebornFunSuite {
  
  test("should parse time duration correctly") {
    val conf = new CelebornConf()
      .set("celeborn.worker.timeout", "30s")
    
    assert(conf.workerTimeoutMs === 30000)
  }
}
```

**如何开始**：
1. 找到覆盖率较低的文件（参考 codecov 报告）
2. 研究现有测试写法（参考 CelebornFunSuite）
3. 补充测试用例
4. 运行 `./build/mvn test -pl common` 验证

---

#### 类型 3: 代码重构 (难度 ⭐⭐)

**适合场景**：
- 提取重复代码
- 简化复杂表达式
- 改善命名

**示例任务 - 提取公共方法**：
```scala
// 重构前：多处重复代码
// Master.scala
val workers = statusSystem.workersMap.values().asScala
  .filter(_.status == WorkerStatus.HEALTHY)
  .toList

// Worker.scala  
val disks = diskInfos.values().asScala
  .filter(_.status == DiskStatus.HEALTHY)
  .toList

// 重构后：提取到工具类
// common/src/main/scala/org/apache/celeborn/common/util/CollectionUtils.scala
object CollectionUtils {
  def filterHealthy[T <: { def status: Status }](
      items: Iterable[T]
  ): List[T] = {
    items.filter(_.status == Status.HEALTHY).toList
  }
}
```

**如何开始**：
1. 使用 IDE 的 "Duplicate Code" 检测功能
2. 寻找长方法（超过 50 行）进行拆分
3. 改善变量命名
4. 确保重构后有测试覆盖

---

#### 类型 4: 简单的 Bug 修复 (难度 ⭐⭐⭐)

**示例任务 - 修复空指针异常**：
```java
// client/src/main/java/org/apache/celeborn/client/ShuffleClientImpl.java

// 修复前
DiskInfo diskInfo = diskInfos.get(mountPoint);
long freeSpace = diskInfo.getAvailableSpace();  // 可能 NPE

// 修复后
DiskInfo diskInfo = diskInfos.get(mountPoint);
if (diskInfo == null) {
  logger.warn("DiskInfo not found for mount point: {}", mountPoint);
  return 0L;
}
long freeSpace = diskInfo.getAvailableSpace();
```

**如何开始**：
1. 在 Jira 上查找标记为 `bug` 且优先级较低的问题
2. 阅读相关代码，理解问题根源
3. 编写复现测试
4. 修复问题并验证

---

### 📝 提交第一个 PR 的完整流程

```bash
# 1. Fork 仓库并在本地配置上游
fork 仓库到个人账户
git clone https://github.com/YOUR_USERNAME/celeborn.git
cd celeborn
git remote add upstream https://github.com/apache/celeborn.git

# 2. 创建分支
git checkout -b CELEBORN-XXXX-fix-typo-in-docs

# 3. 进行修改
# ... 编辑文件 ...

# 4. 格式化代码
./dev/reformat

# 5. 运行相关测试
./build/mvn test -pl common -Dtest=YourTestClass

# 6. 提交变更
git add .
git commit -m "[CELEBORN-XXXX] Fix typo in configuration docs

### What changes were proposed in this pull request?
Fixed typos in worker configuration documentation.

### Why are the changes needed?
To improve documentation quality and readability.

### Does this PR introduce any user-facing change?
No.

### How was this patch tested?
Manually reviewed the generated documentation."

# 7. 推送到你的 Fork
git push origin CELEBORN-XXXX-fix-typo-in-docs

# 8. 在 GitHub 上创建 PR
# - 确保标题包含 Jira 编号
# - 填写 PR 模板
# - 关联对应的 Jira Issue
```

---

## 3. 进阶任务

### 3.1 功能开发路线

```
新手阶段 ────────────────▶ 进阶阶段 ────────────────▶ 专家阶段
   │                          │                        │
   ▼                          ▼                        ▼
文档/测试              新功能开发               架构改进
代码清理               性能优化                 核心重构
Bug 修复               协议扩展                 设计决策
```

### 3.2 推荐进阶任务

#### 任务 1: 添加新的 Metrics 指标

**难度**：⭐⭐⭐

**示例**：添加 Worker 级别的文件描述符使用指标
```scala
// worker/src/main/scala/org/apache/celeborn/service/deploy/worker/WorkerSource.scala
object WorkerSource {
  // 添加新指标定义
  val OPEN_FILE_DESCRIPTOR_COUNT = "OpenFileDescriptorCount"
  val MAX_FILE_DESCRIPTOR_COUNT = "MaxFileDescriptorCount"
}

// Worker.scala 中注册指标
workerSource.addGauge(WorkerSource.OPEN_FILE_DESCRIPTOR_COUNT) { () =>
  ManagementFactory.getOperatingSystemMXBean match {
    case unix: UnixOperatingSystemMXBean => unix.getOpenFileDescriptorCount
    case _ => 0L
  }
}
```

---

#### 任务 2: 实现新的 REST API 端点

**难度**：⭐⭐⭐

**示例**：添加查看 Worker 磁盘使用情况的 API
```scala
// worker/src/main/scala/org/apache/celeborn/service/deploy/worker/http/api/v1/WorkerResource.scala

@GET
@Path("/disks")
@ApiOperation(value = "Get disk usage information", 
              response = classOf[DiskInfoResponse])
def getDiskInfo(): Response = {
  val diskInfos = worker.storageManager.disksSnapshot()
  val response = diskInfos.map { disk =>
    DiskInfoDTO(
      mountPoint = disk.mountPoint,
      totalSpace = disk.totalSpace,
      usedSpace = disk.usedSpace,
      availableSpace = disk.availableSpace,
      status = disk.status.toString
    )
  }
  Response.ok(response).build()
}
```

---

#### 任务 3: 优化配置系统

**难度**：⭐⭐⭐⭐

**示例**：为配置项添加验证注解
```scala
// common/src/main/scala/org/apache/celeborn/common/internal/config/ConfigEntry.scala

case class ConfigEntryWithValidation[T](
  key: String,
  defaultValue: T,
  validator: T => Boolean,
  errorMessage: String
) extends ConfigEntry[T] {
  
  override def readFrom(reader: ConfigReader): T = {
    val value = super.readFrom(reader)
    if (!validator(value)) {
      throw new IllegalArgumentException(
        s"Invalid value for $key: $value. $errorMessage"
      )
    }
    value
  }
}

// 使用示例
val WORKER_HEARTBEAT_TIMEOUT = ConfigEntryWithValidation[Long](
  key = "celeborn.worker.heartbeat.timeout",
  defaultValue = 120000L,
  validator = _ > 0,
  errorMessage = "Timeout must be positive"
)
```

---

### 3.3 技术债务清理任务

这些任务对项目质量提升很大，且相对独立：

| 任务 | 难度 | 影响 |
|------|------|------|
| 添加缺失的 @Override 注解 | ⭐ | 代码规范 |
| 移除未使用的导入 | ⭐ | 代码整洁 |
| 统一日志格式 | ⭐⭐ | 可观测性 |
| 提取魔法数字为常量 | ⭐⭐ | 可维护性 |
| 补充 JavaDoc/ScalaDoc | ⭐⭐ | 文档质量 |
| 修复编译器警告 | ⭐⭐ | 代码质量 |
| 优化异常处理 | ⭐⭐⭐ | 稳定性 |

---

## 4. 学习资源

### 4.1 必读文档

1. **项目文档**
   - `README.md` - 项目概览
   - `ARCHITECTURE_ANALYSIS.md` - 架构分析
   - `AGENTS.md` - 项目指南

2. **代码规范**
   - `.scalafmt.conf` - Scala 代码格式
   - `CONTRIBUTING.md` - 贡献指南

3. **设计文档**
   - `docs/configuration/*.md` - 配置文档
   - `docs/monitoring.md` - 监控指南

### 4.2 代码阅读路线

```
第一阶段：理解基础 (1-2 周)
├── common/src/main/scala/org/apache/celeborn/common/
│   ├── CelebornConf.scala          # 配置系统
│   ├── rpc/                        # RPC 框架
│   └── util/Utils.scala            # 工具类
└── common/src/main/proto/          # 协议定义

第二阶段：理解客户端 (2-3 周)
├── client/src/main/scala/
│   ├── LifecycleManager.scala      # 生命周期管理
│   └── commit/                     # Commit 处理
└── client/src/main/java/
    └── ShuffleClientImpl.java      # 客户端实现

第三阶段：理解服务端 (3-4 周)
├── master/src/main/scala/          # Master 实现
├── worker/src/main/scala/          # Worker 实现
└── service/src/main/scala/         # 通用服务
```

### 4.3 调试技巧

```bash
# 1. 本地启动 Master
./sbin/start-master.sh

# 2. 本地启动 Worker
./sbin/start-worker.sh

# 3. 查看日志
tail -f logs/celeborn-master.out
tail -f logs/celeborn-worker.out

# 4. 使用 CLI 工具查看状态
./bin/celeborn-cli master status
./bin/celeborn-cli worker list

# 5. 运行单个测试进行调试
./build/mvn test -pl common -Dtest=UtilsSuite#testBytesToString

# 6. 启用远程调试
export CELEBORN_MASTER_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
```

---

## 5. 常见问题

### Q1: 编译失败怎么办？

**常见问题及解决**：
```bash
# 问题 1: Scala 版本不匹配
# 解决：指定 Scala 版本编译
./build/mvn clean package -Dscala.binary.version=2.12 -DskipTests

# 问题 2: 内存不足
# 解决：增加 Maven 内存
export MAVEN_OPTS="-Xmx4g -XX:MaxMetaspaceSize=512m"

# 问题 3: 依赖下载失败
# 解决：清除缓存重试
rm -rf ~/.m2/repository/org/apache/celeborn
./build/mvn clean package -DskipTests
```

### Q2: 如何找到可以贡献的任务？

**推荐途径**：
1. **Jira 筛选**：
   - 访问 https://issues.apache.org/jira/projects/CELEBORN
   - 过滤器：`status = Open AND labels in (newbie, good-first-issue)`

2. **GitHub Issues**：
   - 查看标记为 `good first issue` 的问题

3. **代码检查**：
   ```bash
   # 查找 TODO 标记
   grep -r "TODO" --include="*.scala" --include="*.java" . | head -20
   
   # 查找 FIXME 标记
   grep -r "FIXME" --include="*.scala" --include="*.java" . | head -20
   ```

### Q3: PR 被拒绝的常见原因？

| 原因 | 解决方案 |
|------|---------|
| 代码格式不正确 | 运行 `./dev/reformat` |
| 缺少测试 | 补充单元测试 |
| 提交信息不规范 | 遵循格式 `[CELEBORN-XXXX] 标题` |
| 与主干冲突 | 执行 `git rebase origin/main` |
| 功能不符合设计 | 先开 Jira Issue 讨论 |

### Q4: 如何与社区交流？

**渠道**：
1. **Slack**：https://join.slack.com/t/apachecelebor-kw08030/
   - #general - 一般讨论
   - #development - 开发讨论
   - #users - 用户问题

2. **邮件列表**：
   - dev@celeborn.apache.org - 开发讨论
   - 订阅：发送空邮件到 dev-subscribe@celeborn.apache.org

3. **会议**：
   - 社区双周会（查看邮件列表通知）
   - 新特性设计评审会

### Q5: 如何快速理解一个模块？

**步骤**：
1. 阅读模块的 `README.md` 或设计文档
2. 查看单元测试，理解使用场景
3. 画类图，理清依赖关系
4. 使用 IDE 的调试功能单步执行
5. 添加日志，观察运行时行为

---

## 6. 成功案例

### 案例 1: 新手的第一份贡献

**背景**：小张是 Java 开发者，想参与开源

**贡献路径**：
1. **第 1 周**：阅读文档，搭建环境
2. **第 2 周**：修复文档中的 3 个拼写错误（PR #1）
3. **第 3-4 周**：为 Utils 类补充 10 个单元测试（PR #2）
4. **第 5-6 周**：重构重复代码，提取公共方法（PR #3）
5. **第 7-8 周**：修复一个 NPE Bug（PR #4）

**收获**：
- 熟悉了代码审查流程
- 学习了 Scala 和分布式系统知识
- 获得 Committer 的认可和指导

---

## 7. 下一步

当你完成前 3-5 个贡献后，建议：

1. **选择一个感兴趣的方向深入学习**：
   - 存储引擎优化
   - 网络通信改进
   - 调度算法优化
   - 可观测性增强

2. **参与社区活动**：
   - 参加线上讨论会
   - 撰写技术博客分享经验
   - 帮助其他新手入门

3. **申请成为 Committer**：
   - 持续高质量的贡献
   - 参与代码审查
   - 帮助维护项目

---

**祝你在 Apache Celeborn 社区贡献愉快！🎉**

如有问题，欢迎随时在 Slack 或邮件列表中提问。
