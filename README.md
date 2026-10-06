# Spring Boot 整合 Kafka

这是一个 Spring Boot 示例，演示字符串消息、JSON 对象消息、指定分区发送，以及
多个消费者组/消费者监听 Kafka 主题。项目是单 Maven 模块，不包含 Kafka 或
ZooKeeper 的容器编排。

## 技术版本

- Java 17 (LTS)
- Spring Boot `4.1.1`
- Spring Kafka（版本由 Spring Boot 管理）
- Maven 3.6.3 或更新版本（仓库未提供 Maven Wrapper）

Spring Boot 4.1.1 当前要求 Java 17 或更高，并兼容至 Java 26。项目选用共同兼容的
Java 17 LTS 基线；Eclipse Temurin 的支持路线图列示 Java 17 LTS 仍受支持至少到
2027 年 10 月。版本依据（查阅于 2026-10-04）：
[Spring Boot 系统要求](https://docs.spring.io/spring-boot/system-requirements.html)
和 [Eclipse Adoptium 支持路线图](https://adoptium.net/support/)。

2026-10-05 分别以 Java `17.0.9`、`25.0.4.1` 完成 `clean package`，
两个 tests（含真实 broker integration）均通过，CI 使用 Java 17／25 matrix。
编译目标仍是 Java 17。JDK 23+ 默认禁用隐式 annotation processing，因此 compiler
已显式配置现有 Boot-managed Lombok processor，未提高最低 Java 版本。

## 功能与结构

| 路径 | 作用 |
| --- | --- |
| `producer/KafkaCustomProducer.java` | 使用具类型的 `KafkaTemplate` 异步发送消息并记录回调结果 |
| `controller/SendMsgController.java` | 提供三个触发消息发送的 HTTP 接口 |
| `consumer/KafkaSimpleConsumer.java` | 消费普通字符串消息 |
| `consumer/KafkaBeanConsumer.java` | 将 JSON 消息反序列化为 `Programmer` |
| `consumer/KafkaGroupConsumer.java` | 演示指定分区和多个消费者组 |
| `config/KafkaConfig.java` | 声明 10 分区、复制因子为 2 的分组主题 |
| `constant/Topic.java` | 定义示例使用的三个主题名 |

主题如下：

| 主题 | 用途 |
| --- | --- |
| `spring.boot.kafka.simple` | 普通字符串消息 |
| `spring.boot.kafka.bean` | JSON 对象消息 |
| `spring.boot.kafka.newGroup` | 指定分区和消费者组示例 |

## 前置条件

1. 安装 JDK 17 和 Maven。
2. 准备可访问的 Kafka 集群。
3. 默认连接 `localhost:9092`。若 broker 位于其他位置，设置以逗号分隔的环境
   变量 `KAFKA_BOOTSTRAP_SERVERS`，例如：

   ```bash
   export KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092
   ```
4. `spring.boot.kafka.newGroup` 在源码中要求 10 个分区、复制因子 2，因此自动
   创建时至少需要 2 个 broker。单 broker 环境可先手工创建同名主题，使用 10 个
   分区和复制因子 1；已存在的主题不会被重复创建。

另外两个主题依赖 broker 的自动创建设置；如果集群关闭了自动创建，请手工创建
`spring.boot.kafka.simple` 和 `spring.boot.kafka.bean`。

## 构建、测试与运行

```bash
# 编译并执行测试
mvn clean test

# 打包
mvn clean package

# 运行
mvn spring-boot:run

# 或运行打包结果
java -jar target/spring-boot-kafka-0.0.1-SNAPSHOT.jar
```

`mvn test` 同时执行 Jackson JSON 单元测试与真实 producer/consumer 整合测试。
整合测试使用已有 `spring-kafka-test` 的 Kafka testkit 启动两个 in-process KRaft
broker 与一个 controller，固定使用 released production metadata，避免默认
experimental metadata／多 controller quorum 导致的启动与关闭故障。
10 个分区及复制因子 2，保留 production 的主题/复制设置；不需要 Docker、
外部 Kafka 或 ZooKeeper。测试覆写 bootstrap address 与 listener concurrency，
实际调用三个 HTTP controller，再等待真实 listener 收到消息。
它验证字符串内容、真实 bean listener 成功解码、对象字段以及 0–3 分区的
key/value 和两个消费者组（包括显式绑定相同分区的两个 listener）。

```bash
# 最小整合 gate；broker 不可启动/消费超时会失败，不会跳过
mvn -B --no-transfer-progress -Dtest=KafkaMessagingIntegrationTest test

# 完整测试与可执行 JAR gate（CI 也执行此命令）
mvn -B --no-transfer-progress clean package
```

Surefire 报告与 embedded broker 暂存资料都位于 `target/`；broker 在 Spring
context 关闭后停止；fixture 的 shutdown 异常会导致测试失败，而不是只记 warning。
嵌入式测试需要允许绑定 loopback TCP ports，不验证外部
集群的认证、网络或部署。运行应用仍需前述可访问的 Kafka 集群。

应用监听端口由 `application.yml` 设置为 `19091`。

## 调用示例

```bash
# 发送普通字符串
curl http://localhost:19091/sendSimple

# 序列化 Programmer 为 JSON 后发送
curl http://localhost:19091/sendBean

# 分别向分组主题的 0、1、2、3 分区发送消息
curl http://localhost:19091/sendGroup
```

接口用于触发发送，响应体为空；发送结果和消费结果输出在应用控制台。

## 消费行为

- `simpleGroup` 消费普通消息主题。
- `beanGroup` 消费对象消息主题。
- `group1` 中的监听器显式绑定分区 0/1 或 2/3。
- `group2` 监听整个分组主题。
- `application.yml` 设置监听并发数为 5、自动提交 offset，并从 `earliest` 开始
  读取没有有效 offset 的分区。

`consumer1-1` 与 `consumer1-3` 被显式绑定到相同分区，因此示例中两者都可能收到
这些分区的消息；这不是 Kafka 自动分区分配的典型同组互斥方式。

## 配置与运维注意事项

- 生产者使用字符串序列化；对象示例使用 Spring Boot 管理的 Jackson 3 转换 JSON，
  不另行引入 JSON parser。
- 生产者 `acks=1`、`retries=0`，仅适合演示，不代表生产环境可靠性配置。
- HTTP GET 接口会产生消息，属于演示设计，不应直接作为生产 API 约定。
- 修改 broker 地址、主题、副本数或消费策略时，以
  `application.yml`、`KafkaConfig.java` 和 `Topic.java` 的当前值为准。

## Repository hygiene

Maven `target/`、Java `.class` 与常见 IDE metadata 均由 `.gitignore` 排除，不应
提交。构建前后可用 `git status --short` 确认工作树只包含刻意修改的源码。
