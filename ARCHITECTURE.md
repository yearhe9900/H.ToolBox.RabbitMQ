# H.ToolBox.RabbitMQ 架构设计

## 概述

H.ToolBox.RabbitMQ 是一个基于 .NET 的 RabbitMQ 消费者/生产者工具库，采用 **特性标记 + 自动扫描** 和 **配置驱动** 两种模式，简化 RabbitMQ 的集成工作。

---

## 核心架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           应用层 (Application)                              │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────┐  │
│  │   IToolBoxRabbitMQ   │  │    IConfigConsumer   │  │  IConsumerService│  │
│  │   (特性驱动消费者)    │  │   (配置驱动消费者)    │  │   (简化API)      │  │
│  └──────────┬───────────┘  └──────────┬───────────┘  └────────┬─────────┘  │
└─────────────┼─────────────────────────┼───────────────────────┼─────────────┘
              │                         │                       │
┌─────────────┼─────────────────────────┼───────────────────────┼─────────────┐
│             ▼                         ▼                       ▼             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    ConsumerServiceSelector                           │   │
│  │              (消费者服务选择器 - 扫描与解析)                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      MethodMatcherCache                              │   │
│  │                  (方法匹配缓存 - ConsumerName映射)                   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     RabbitConsumerRegister                           │   │
│  │                (消费者注册器 - 统一管理所有消费者)                    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                           核心层 (Core)                                     │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        Bootstrapper                                  │   │
│  │                   (启动器 - BackgroundService)                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   TransportCheckProcessor                            │   │
│  │                    (健康检查处理器)                                   │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
              │
┌─────────────┼───────────────────────────────────────────────────────────────┐
│             ▼                                                               │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │              RabbitMqConsumerClientFactory                            │   │
│  │                 (消费者客户端工厂)                                    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   ConnectionChannelPoolManager                       │   │
│  │                   (连接通道池管理器 - 多集群)                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                           连接层 (Connection)                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                     │
│  │  Pool(default)│  │ Pool(cluster2)│  │ Pool(cluster3)│  ...              │
│  │  192.168.1.100│  │ 192.168.1.200│  │ 192.168.1.300│                     │
│  └──────────────┘  └──────────────┘  └──────────────┘                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 组件详解

### 1. 消费者扫描层

#### ConsumerServiceSelector

**职责**: 扫描所有消费者实现类，提取消费者方法信息

**支持两种扫描模式**:

1. **IToolBoxRabbitMQ 扫描** (特性驱动)
   - 扫描所有 `IToolBoxRabbitMQ` 实现类
   - 提取带有 `[RabbitMQConsumer]` 特性的方法
   - 从特性中读取配置参数

2. **IConfigConsumer 扫描** (配置驱动)
   - 扫描所有 `IConfigConsumer` 实现类
   - 从 `ConfigConsumerOptions` 配置中查找匹配的类名和方法名
   - 动态创建 `RabbitMQConsumerAttribute` 实例

**代码位置**: `H.ToolBox.RabbitMQ/Consumer/ConsumerServiceSelector.cs`

#### MethodMatcherCache

**职责**: 缓存消费者名称到方法扫描结果的映射

**特性**:
- `ConcurrentDictionary<string, MethodScanResult>` 存储，每次启动全量重建并原子替换，清理上批次残留
- 启动时校验 ConsumerName 全局唯一性，重复则 fail-fast 抛异常（避免同名消费者互相覆盖）
- 未显式指定时自动生成 ConsumerName（完整类型名.方法名），避免跨类库同名类撞名

**代码位置**: `H.ToolBox.RabbitMQ/Model/MethodMatcherCache.cs`

---

### 2. 消费者注册层

#### RabbitConsumerRegister

**职责**: 统一管理所有集群的消费者注册和生命周期

**核心流程**:
```
ExecuteAsync()
  ├── TrySetExecutor() → 获取所有消费者方法扫描结果
  ├── 遍历每个方法:
  │   ├── CompileDelegate() → 编译委托 (Expression Tree)
  │   ├── 根据 ConfigId 确定连接集群
  │   ├── 根据 ListenerCount 创建 N 个并发消费者
  │   └── 每个消费者:
  │       ├── CreateAsync(configId) → 创建客户端
  │       ├── RegisterMessageProcessor() → 注册回调
  │       └── ListeningAsync() → 开始监听
  └── 健康监控计数器
```

**消息处理回调**:
- 通过 consumerTag 查找 MethodMatcherCache 中的执行器
- 优先使用编译后的委托 (高性能)
- 方法返回 `true` → CommitAsync (确认)
- 方法返回 `false` → RejectAsync (拒绝)

**代码位置**: `H.ToolBox.RabbitMQ/Consumer/RabbitConsumerRegister.cs`

---

### 3. 启动与生命周期层

#### Bootstrapper

**职责**: 作为 BackgroundService 管理消费者启动和停止

**生命周期**:
- `StartAsync`: 解析所有 `IRabbitConsumerRegister` 并启动
- `StopAsync`: 停止所有注册器，释放资源

**代码位置**: `H.ToolBox.RabbitMQ/Bootstrapper.cs`

#### TransportCheckProcessor

**职责**: 定时检查消费者健康状态

**特性**:
- 可配置检查间隔 (默认30秒，`<=0` 不开启)
- 发现不健康时自动调用 `ReStartAsync(force: true)` 重启

**代码位置**: `H.ToolBox.RabbitMQ/TransportCheckProcessor.cs`

---

### 4. 连接池层

#### ConnectionChannelPoolManager

**职责**: 管理多集群连接池，按 ConfigId 路由

**特性**:
- `ConcurrentDictionary<string, ConnectionChannelPool>` 存储
- 按 ConfigId 查找对应的连接池
- 支持动态注册新集群
- **三组连接池**: 每个集群维护三组独立连接池，提供连接级隔离：
  - **消费者池** (GetPool): 供 RabbitMQConsumer 使用
  - **生产者池** (GetProducerPool): 供生产者使用
  - **ConsumerService 池** (GetConsumerServicePool): 供 IConsumerService 使用，支持 ConsumerDispatchConcurrency 设置
- 注意：`Rent` / `Return` 与扩容/回收只对生产者池生效；消费侧通过 `GetConnectionAsync()` 创建长生命周期通道，不走通道租借

**代码位置**: `H.ToolBox.RabbitMQ/Channel/ConnectionChannelPoolManager.cs`

#### ConnectionChannelPool

**职责**: 管理单集群的 RabbitMQ 连接和通道复用

**核心机制**:
- **多连接支持**: `ConnectionCount` (默认2) 条 TCP 连接
- **Round-Robin 分配**: 通道均匀分摊到各连接
- **异步租借**: `SemaphoreSlim` + `ConcurrentQueue`
- **动态扩容**: 信号量满时 CAS 无锁翻倍扩容
- **空闲回收**: `IdleTimeout` 超时自动回收
- **声明缓存**: `CachedDeclareChannel` 减少网络往返

**配置项**:
| 配置项 | 默认值 | 说明 |
|---|---|---|
| `PoolSize` | 15 | 初始并发通道数量 |
| `AbsoluteMaxPoolSize` | 128 | 动态扩展上限 |
| `MinPoolSize` | 0 | 最小保留通道数 |
| `IdleTimeout` | 60秒 | 空闲超时时间 |
| `ConnectionCount` | 2 | TCP 连接数 |

**代码位置**: `H.ToolBox.RabbitMQ/Channel/ConnectionChannelPool.cs`

---

### 5. 消费者客户端层

#### RabbitMqConsumerClientFactory

**职责**: 创建并初始化消费者客户端实例

**特性**:
- 通过 `IConnectionChannelPoolManager` 按 ConfigId 选择连接池
- 上层代码无需感知底层集群差异

**代码位置**: `H.ToolBox.RabbitMQ/Consumer/RabbitMqConsumerClientFactory.cs`

#### RabbitMqConsumerClient

**职责**: 负责连接、声明队列/交换机、QoS、持续监听消息

**核心流程**:
```
ListeningAsync()
  ├── 获取连接通道
  ├── 声明队列/交换机
  ├── 绑定交换机 (交换机模式)
  ├── 设置 QoS
  ├── 创建消费者
  └── 持续监听消息
```

**代码位置**: `H.ToolBox.RabbitMQ/Consumer/RabbitMqConsumerClient.cs`

---

### 6. 生产者层

#### RabbitMqTransport

**职责**: 按 ConfigId 路由到对应集群发送消息

**发送方式**:
1. **队列模式**: `SendAsync(configId, queueName, message)`
2. **交换机模式**: `SendAsync(configId, exchange, routingKey, message)`
3. **底层方式**: `SendAsync(TransportMessage)`

**特性**:
- 自动 JSON 序列化
- 支持多集群路由
- 发布确认模式
- 故障重试机制

**代码位置**: `H.ToolBox.RabbitMQ/Transport/RabbitMqTransport.cs`

---

### 7. 简化 API 层

#### IConsumerService / ConsumerService

**职责**: 提供简化的消费 API（兼容性消费者 API）

**支持模式**:
1. **队列消费**: `ListenAsync<T>(configId, queueName, callback)`
2. **交换机消费**: `ListenByExchangeAsync<T>(configId, exchange, exchangeType, routingKey, queueName, callback)`

**特性**:
- 泛型支持，自动反序列化
- 通过 configId 获取 channel
- 支持取消令牌
- **独立连接池**: 使用 ConsumerService 专用连接池，与 RabbitMQConsumer 隔离
- **ConsumerDispatchConcurrency 支持**: 可通过 `RabbitMQOptions.ConsumerDispatchConcurrency` 配置并发分发度

**并发机制说明**:
- IConsumerService 使用 RabbitMQ.Client 原生的 `ConsumerDispatchConcurrency` 进行并发分发
- RabbitMQConsumer 使用 `RabbitMqBasicConsumer` 内部的 `Task.Run` 并发机制
- 两种机制互斥，因此使用独立的连接池隔离

**代码位置**: `H.ToolBox.RabbitMQ/Consumer/ConsumerService.cs`

---

## 数据流

### 消费者启动流程

```
Program.cs
  ├── AddSingleton<IToolBoxRabbitMQ, MyService>()     // 特性驱动
  ├── AddSingleton<IConfigConsumer, ConfigService>()   // 配置驱动
  ├── AddRabbitMQ(configs => { ... })                  // 注册服务
  │   ├── Singleton: ConsumerServiceSelector
  │   ├── Singleton: MethodMatcherCache
  │   ├── Singleton: RabbitConsumerRegister
  │   ├── Singleton: Bootstrapper (HostedService)
  │   └── Singleton: TransportCheckProcessor (HostedService)
  └── AddConfigConsumer(configuration)                 // 加载配置

Host Start
  └── Bootstrapper.ExecuteAsync()
      └── RabbitConsumerRegister.StartAsync()
          ├── ConsumerServiceSelector.SelectCandidates()
          │   ├── 扫描 IToolBoxRabbitMQ 实现 (特性驱动)
          │   └── 扫描 IConfigConsumer 实现 (配置驱动)
          ├── MethodMatcherCache.TrySetExecutor() 缓存
          └── 对每个消费者方法:
              ├── CompileDelegate() (Expression Tree)
              ├── CreateAsync(configId) → 创建客户端
              └── ListeningAsync() → 开始监听
```

### 消息处理流程

```
消息到达
  └── RabbitMqConsumerClient
      └── 消息回调
          ├── 通过 consumerTag 查找 MethodMatcherCache
          ├── 获取编译后的委托
          ├── 调用委托(message, routingKey)
          │   ├── true → CommitAsync (确认)
          │   └── false → RejectAsync (拒绝)
          └── 记录日志
```

### 生产者发送流程

```
应用代码
  └── IRabbitMqTransport.SendAsync()
      ├── 根据 ConfigId 获取连接池
      ├── 从连接池租借通道
      ├── 序列化消息
      ├── 发送消息
      │   ├── 队列模式: BasicPublishAsync(queueName)
      │   └── 交换机模式: BasicPublishAsync(exchange, routingKey)
      └── 归还通道到连接池
```

---

## 设计模式

### 1. 标记接口模式 (Marker Interface)

- `IToolBoxRabbitMQ`: 特性驱动消费者标记
- `IConfigConsumer`: 配置驱动消费者标记

**优势**: 无侵入性，实现类无需实现任何方法

### 2. 策略模式 (Strategy Pattern)

- `ConsumerServiceSelector` 支持两种扫描策略
- 根据接口类型选择不同的扫描逻辑

**优势**: 易于扩展新的扫描策略

### 3. 工厂模式 (Factory Pattern)

- `RabbitMqConsumerClientFactory` 创建消费者客户端
- `IConnectionChannelPoolManager` 管理连接池

**优势**: 解耦创建逻辑，支持多集群

### 4. 单例模式 (Singleton Pattern)

- 所有核心服务注册为 Singleton
- 通过 DI 容器管理生命周期

**优势**: 资源复用，性能优化

### 5. 观察者模式 (Observer Pattern)

- `Bootstrapper` 作为 HostedService 监听应用生命周期
- `TransportCheckProcessor` 定时检查健康状态

**优势**: 自动管理，及时响应状态变化

### 6. 缓存模式 (Caching Pattern)

- `MethodMatcherCache` 缓存扫描结果
- `CachedDeclareChannel` 缓存声明操作

**优势**: 避免重复计算，提升性能

---

## 扩展点

### 1. 自定义序列化器

```csharp
// 实现 IMessageSerializer 接口
public class CustomSerializer : IMessageSerializer
{
    // ...
}

// 注册（必须在 AddRabbitMQ 之前）
builder.Services.AddSingleton<IMessageSerializer, CustomSerializer>();
```

### 2. 自定义消费者客户端

```csharp
// 实现 IRabbitMqConsumerClientFactory 接口
public class CustomClientFactory : IRabbitMqConsumerClientFactory
{
    // ...
}

// 注册
builder.Services.AddSingleton<IRabbitMqConsumerClientFactory, CustomClientFactory>();
```

### 3. 自定义连接池管理器

```csharp
// 实现 IConnectionChannelPoolManager 接口
public class CustomPoolManager : IConnectionChannelPoolManager
{
    // ...
}

// 注册
builder.Services.AddSingleton<IConnectionChannelPoolManager, CustomPoolManager>();
```

---

## 性能优化

### 1. Expression Tree 编译

- 使用 `ExpressionTree` 编译委托，避免反射开销
- 编译后的委托缓存到 `MethodMatcherCache`

### 2. 通道池复用

- `SemaphoreSlim` 异步租借，避免锁竞争
- `ConcurrentQueue` 存储空闲通道
- 动态扩容，高峰自动扩展

### 3. 多连接分摊

- `ConnectionCount` 条 TCP 连接
- Round-Robin 均匀分摊通道
- 避免单连接 AMQP 帧串行阻塞

### 4. 声明缓存

- `CachedDeclareChannel` 缓存声明操作
- 同一 exchange/queue 只声明一次
- 减少网络往返

### 5. 死信队列（DLX）

- 消费失败统一 `requeue:false`，由 `DeadLetterTopology` 声明 DLX / DLQ / 绑定
- 拓扑参数由集群级 `RabbitMQOptions` 唯一定义，生产者与消费者共用，避免 `PRECONDITION_FAILED`
- 可配置：`EnableDeadLetter` / `DeadLetterExchange` / `DeadLetterExchangeType` / `DeadLetterQueueSuffix`

### 6. 批量确认（规划中，尚未实现）

- `AckBatcher` 累积确认（后续版本）
- `AckBatchSize` 达到阈值批量确认
- `AckFlushIntervalMs` 定时刷新兜底
- 当前版本仍为逐条确认，上述参数暂不生效

---

## 容错机制

### 1. 故障重试

- 发布瞬时故障自动重试
- 指数退避 (100/200/400ms...上限 2s)
- 可配置重试次数 (`MaxRetries`)

### 2. 连接级故障转移

- 某条连接异常时自动切换
- 通道创建时自动重建

### 3. 健康检查

- 定时检查消费者健康状态
- 不健康时自动重启

### 4. 优雅停机

- `StopAsync` 等待所有消费者完成
- 释放所有资源

---

## 配置管理

### 集群配置 (RabbitMQOptions)

```json
{
  "RabbitMQConfig": [
    {
      "ConfigId": "default",
      "HostName": ["192.168.1.100"],
      "Port": 5672,
      "UserName": "guest",
      "Password": "guest",
      "VirtualHost": "/",
      "PublishConfirms": true,
      "PoolSize": 15,
      "AbsoluteMaxPoolSize": 128,
      "MinPoolSize": 2,
      "IdleTimeout": "00:01:00",
      "ConnectionCount": 2,
      "MaxRetries": 3,
      "ConsumerThreadCount": 1,
      "DefaultQueueType": "Classic"
    }
  ]
}
```

### 消费者配置 (ConfigConsumerOptions)

```json
{
  "ConfigConsumer": {
    "Consumers": [
      {
        "ConfigId": "default",
        "QueueName": "order.queue",
        "ClassName": "MyNamespace.MyConsumer",
        "MethodName": "HandleOrder",
        "ConsumerName": "order.consumer",
        "AutoAck": false,
        "Durable": true,
        "ListenerCount": 1,
        "PrefetchCount": 100
      }
    ]
  }
}
```

---

## 依赖关系

```
H.ToolBox.RabbitMQ (核心库)
  ├── RabbitMQ.Client (7.2.1)
  ├── Microsoft.Extensions.Hosting.Abstractions
  ├── Microsoft.Extensions.Logging
  └── Microsoft.Extensions.Configuration.Binder
```

---

## 版本演进

### 当前版本: 1.0.4-alpha05（以 `H.ToolBox.RabbitMQ.csproj` 为准，未发布到 NuGet）

**已实现功能**:
- ✅ 特性驱动消费者 / 配置驱动消费者
- ✅ 多集群支持（多次 `AddRabbitMQ` 合并配置；`AddRabbitMqConsumer<T>()` 显式注册）
- ✅ 连接通道池复用 / 多连接分摊 / 动态扩容（生产者侧）
- ✅ Publisher Confirms 与 `nack` / `basic.return` 区分处理
- ✅ 死信队列（DLX）：消费失败 `requeue:false` + 毒消息隔离
- ✅ 按实例的健康检查与自动重启
- ✅ 简化 API
- ✅ Testcontainers 集成测试基建（无 Docker 自动跳过）

**下一版本计划（尚未实现）**:
- 🔄 批量累积确认 (AckBatcher)
- 🔄 监控指标暴露

---

## 许可证

MIT License

## 作者

**何志鹏**
