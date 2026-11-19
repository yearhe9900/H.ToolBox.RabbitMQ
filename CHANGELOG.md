# Changelog

本文件记录 **H.ToolBox.RabbitMQ** 的所有重要变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本 2.0.0](https://semver.org/lang/zh-CN/)。

> 说明：当前为 `mq_dev` 开发分支的 alpha 阶段，下列变更均基于 `1.0.4-alpha05`
> 版本线，尚未发布到 NuGet。文档与实现不一致之处已在对应批次修正。

## [Unreleased]

> 批次 3 已完成，尚未提升包版本；发布时把本节内容归入新版本号。

### Fixed
- **连接池信号量不变量**：`SweepIdleChannels` 回收空闲通道时不再重复 `Release()` 信号量许可
  （通道归还已由 `Return` 释放一次），避免许可数随每次扫描单调递增直至
  `SemaphoreFullException`、池容量上限失效；回收失败时保留通道而非丢弃。
- **消费者健康判定永真**：`RabbitConsumerRegister` 以按实例的健康状态字典取代
  `healthy == total` 聚合计数——原实现在任务异常时同步递减两个计数器，等式恒成立，
  导致故障消费者永不重启。日志事件按 `consumerTag` 落到具体实例；未知日志类型不再抛异常。
- **重启令牌泄漏**：抽取 `BuildCts` 统一创建/替换取消令牌源，`ReStartAsync` 重建 CTS 时
  补上 `StopConsumers` 取消回调并释放旧 CTS（旧实现两者皆缺失）。
- **健康检查全量重启**：`TransportCheckProcessor` 改用 `ReStartAsync()` 默认 `force:false`，
  由注册器内部二次校验健康状态，避免把已恢复的消费者一并拆掉重连造成抖动。

### Added
- **连接池可测试性**：`ConnectionChannelPool` 新增可注入连接工厂的内部构造函数，并暴露
  `SweepIdleChannelsNow()` 与诊断属性 `AvailablePermits` / `PooledChannelCount`，
  无需真实 Broker 即可单测信号量不变量。
- 新增信号量不变量、回收后再租借、以及消费者健康聚合（单实例异常即不健康）等回归用例。

## [1.0.4-alpha05] - 2026-11-10

### Added
- 引入 `Testcontainers.RabbitMq` 集成测试基建（`RabbitMqFixture` / `ProducerConsumerHarness` /
  `DockerFactAttribute`）：无 Docker 环境自动跳过，集成测试源码可通过
  `-p:EnableIntegrationTests=false` 彻底排除。
- 新增 `DeadLetterTopology`：按集群级 `RabbitMQOptions` 统一声明死信交换机 / 队列 / 绑定，
  并构建队列声明参数。
- 新增 `AddRabbitMqConsumer<T>()` 与 `AddConfigConsumerService<T>()` 注册入口，
  按 `(ServiceType, ImplementationType)` 判重，避免后续实现被吞掉。
- 新增异常类型 `PublishNackedException`（broker `basic.nack`）与 `PublishReturnedException`
  （`mandatory` 消息 `basic.return`）。

### Changed
- **（破坏性）消费失败策略**：`IRabbitMqConsumerClient.RejectAsync` 新增 `requeue` 参数且默认 `false`；
  `RabbitMqBasicConsumer.BasicReject`、`ConsumerService` 两处 `requeue` 同步改为 `false`，
  统一接入 `requeue:false + DLX` 死信拓扑，杜绝毒消息无限重投打满 CPU / 日志。
- **发布确认链路**：`ConnectionChannelPool` 创建通道时同步开启
  `PublisherConfirmationTrackingEnabled`；`RabbitMqTransport` 改为 `await BasicPublishAsync`
  + `catch PublishException`，按 RabbitMQ.Client 7.x 语义区分 broker `nack`（指数退避重试）
  与 `basic.return`（不重试），移除 `confirmTask` 死代码。
- **死信拓扑唯一真源**：生产者（`RabbitMqTransport`）、消费者（`RabbitMqConsumerClient`）、
  简化消费 API（`ConsumerService`）三处队列声明共用 `DeadLetterTopology`，
  修正生产者 `_durable=false` 与消费侧默认 `Durable=true` 的冲突，避免 `PRECONDITION_FAILED`。
- **多实现类发现**：`ConsumerServiceSelector` 增加程序集扫描兜底并只解析 DI 已注册实例；
  `MethodMatcherCache` 自动消费者名改用 `FullName`、重复名 fail-fast、每次全量重建缓存清残留；
  `RabbitConsumerRegister` 启动期直接绑定 `MethodScanResult`，移除 `consumerTag` 字符串回退查找。
- **声明缓存**：`CachedDeclareChannel` 改为声明成功后才写缓存、锁按 channel 共享、缓存创建原子化，
  并新增 `QueueBindAsync`。
- **投递元信息**：`TransportMessage` 新增 `Redelivered` / `MessageId` / `Exchange`，
  由 `RabbitMqBasicConsumer` 填充。

### Fixed
- 发布确认形同虚设（`PublisherConfirmationTrackingEnabled` 被关闭成 `false`）。
- 消费者方法签名不合法时静默 `CommitAsync` 导致消息丢失，改为拒绝进死信队列。
- 多个类库的 `IToolBoxRabbitMQ` / `IConfigConsumer` 实现只能扫描到最早注入的一个。
- `CachedDeclareChannel` 声明失败仍写缓存，可能导致消息发往不存在的队列。
- `ServiceCollectionExtensions` 多次调用 `AddRabbitMQ` 时重复注册基础设施、集群配置不合并、
  重复 `ConfigId` 未报错。

## [1.0.4-alpha04] 及更早
- 见 Git 历史与 `README.md` / `ARCHITECTURE.md`。
