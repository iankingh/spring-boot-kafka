# Spring Boot 整合 Kafka

这是一个 Spring Boot 2.1 示例，演示字符串消息、JSON 对象消息、指定分区发送，以及
多个消费者组/消费者监听 Kafka 主题。项目是单 Maven 模块，不包含 Kafka 或
ZooKeeper 的容器编排。

## 技术版本

- Java 8
- Spring Boot `2.1.1.RELEASE`
- Spring Kafka（版本由 Spring Boot 管理）
- Maven（仓库未提供 Maven Wrapper）

## 功能与结构

| 路径 | 作用 |
| --- | --- |
| `Producer/KafKaCustomrProducer.java` | 使用 `KafkaTemplate` 异步发送消息并输出回调结果 |
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

1. 安装 JDK 8 和 Maven。
2. 准备可访问的 Kafka 集群。
3. 修改 `src/main/resources/application.yml` 中的
   `spring.kafka.bootstrap-servers`。仓库当前值 `192.168.3.196:9092` 是历史
   示例地址，不是通用本机默认值。
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

测试类只是 Spring 上下文测试，并未使用嵌入式 Kafka。由于应用上下文会加载 Kafka
管理配置，执行测试和启动应用前都应提供可访问的 broker。

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

- 生产者使用字符串序列化；对象示例由 Fastjson 先转换为 JSON 字符串。
- 生产者 `acks=1`、`retries=0`，仅适合演示，不代表生产环境可靠性配置。
- HTTP GET 接口会产生消息，属于演示设计，不应直接作为生产 API 约定。
- 修改 broker 地址、主题、副本数或消费策略时，以
  `application.yml`、`KafkaConfig.java` 和 `Topic.java` 的当前值为准。
