# H.ToolBox.RabbitMQ

基于 .NET 的 RabbitMQ 消费者/生产者工具库，通过 **特性标记 + 自动扫描** 的方式，让你用最少的代码完成 RabbitMQ 消费者和生产者的集成。支持 **多集群连接**，通过 `ConfigId` 一键切换不同的 MQ 实例。

---

## 项目结构

```
H.ToolBox.RabbitMQ/
├── H.ToolBox.RabbitMQ/                # 核心库
│   ├── Channel/                        # 连接通道池（多集群管理）
│   ├── Consumer/                       # 消费者管理（扫描、注册、客户端）
│   ├── Model/                          # 数据模型（配置、缓存、扫描结果）
│   └── Transport/                      # 消息传输（生产者）
├── H.ToolBox.RabbitMQ.Application/    # 示例消费者实现
└── H.ToolBox.RabbitMQ.WorkerService/  # 示例 Worker 服务
```

### 核心组件

| 层 | 组件 | 说明 |
|---|---|---|
| **连接池** | `ConnectionChannelPool` | 管理单集群的 RabbitMQ 连接和通道复用，SemaphoreSlim 异步租借，支持空闲回收和声明缓存 |
| **连接池管理** | `ConnectionChannelPoolManager` | 管理多集群连接池，按 `ConfigId` 路由 |
| **消费者扫描** | `ConsumerServiceSelector` | 反射扫描 `IToolBoxRabbitMQ` 实现类中的 `[RabbitMQConsumer]` 标记方法 |
| **方法缓存** | `MethodMatcherCache` | 缓存 ConsumerName → 方法映射，启动时校验唯一性 |
| **消费者注册** | `RabbitConsumerRegister` | 单实例统一管理所有集群的消费者，按各自 `ConfigId` 自动路由 |
| **消费者客户端** | `RabbitMqConsumerClient` | 负责连接、声明队列/交换机、QoS、持续监听消息 |
| **生产者** | `RabbitMqTransport` | 按 `ConfigId` 路由到对应集群发送消息 |
| **消费者服务** | `IConsumerService` / `ConsumerService` | 简化消费 API，支持 `ListenAsync`（队列消费）和 `ListenByExchangeAsync`（路由消费），通过 `configId` 获取 channel |
| **启动器** | `Bootstrapper` | `BackgroundService`，应用启动时自动初始化所有消费者 |
| **健康检查** | `TransportCheckProcessor` | 可配置间隔时间的健康检查（默认30秒，`<=0` 不开启） |

---

## 设计思路

### 核心理念：特性标记 + 自动扫描

不需要手动创建消费者实例、声明队列、绑定交换机。只需：

1. 实现 `IToolBoxRabbitMQ` 标记接口
2. 在消费方法上标记 `[RabbitMQConsumer]` 特性
3. 一行代码注册服务

框架自动完成：扫描 → 缓存 → 创建客户端 → 声明资源 → 持续监听 → 反射调用。

### 多集群架构

```
┌─────────────────────────────────────────────────┐
│               ConnectionChannelPoolManager       │
│  ┌──────────────┐    ┌──────────────┐           │
│  │ Pool(default)│    │ Pool(cluster2)│  ...      │
│  │ 192.168.1.100│    │ 192.168.1.200│           │
│  │ Port: 5672   │    │ Port: 5673   │           │
│  └──────┬───────┘    └──────┬───────┘           │
└─────────┼────────────────────┼──────────────────┘
          │                    │
    ┌─────┴─────┐        ┌────┴──────┐
    │ Consumer A │        │ Consumer B │
    │ ConfigId=  │        │ ConfigId=  │
    │ "default"  │        │ "cluster2" │
    └───────────┘        └───────────┘
```

- **消费者端**：每个消费者通过 `[RabbitMQConsumer(ConfigId = "...")]` 指定连接的集群
- **生产者端**：通过 `TransportMessage.ConfigId` 指定发送到哪个集群
- **连接池**：`ConnectionChannelPoolManager` 统一管理，按 `ConfigId` 路由

---

## 快速上手

### 1. 安装配置

`appsettings.json`：

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
      "IdleTimeout": "00:01:00"
    },
    {
      "ConfigId": "cluster2",
      "HostName": ["192.168.1.200"],
      "Port": 5672,
      "UserName": "guest",
      "Password": "guest"
    }
  ]
}
```

### 2. 注册服务

```csharp
// 注册消费者类
builder.Services.AddSingleton<IToolBoxRabbitMQ, MyService>();

// 注册 RabbitMQ 多集群
builder.Services.AddRabbitMQ(configs =>
{
    var clusterConfigs = builder.Configuration
        .GetSection("RabbitMQConfig").Get<List<RabbitMQOptions>>();
    if (clusterConfigs != null)
    {
        foreach (var config in clusterConfigs)
            configs.Add(config);
    }
});
```

单集群兼容模式：

```csharp
builder.Services.AddRabbitMQ(options =>
{
    builder.Configuration.GetSection("RabbitMQConfig").Bind(options);
});
```

健康检查配置（可选，默认 30 秒，`<=0` 不开启）：

```csharp
// 自定义 60 秒健康检查间隔
builder.Services.AddRabbitMQ(configs => { ... }, checkIntervalSeconds: 60);

// 不开启健康检查
builder.Services.AddRabbitMQ(configs => { ... }, checkIntervalSeconds: 0);
```

通道池配置（在 `RabbitMQOptions` 中设置）：

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `PoolSize` | 15 | 初始并发通道数量 |
| `AbsoluteMaxPoolSize` | 128 | 动态扩展上限，信号量满时自动翻倍扩容，设为0或负数则禁止动态扩展 |
| `MinPoolSize` | 0 | 空闲回收时至少保留的通道数 |
| `IdleTimeout` | 60 秒 | 通道空闲超时时间，`TimeSpan.Zero` 禁用回收 |

### 3. 创建消费者

```csharp
public class MyService : IToolBoxRabbitMQ
{
    // 默认集群消费者
    [RabbitMQConsumer(QueueName = "order.queue", ConsumerName = "order.consumer")]
    public async Task HandleOrder(string source, string routingKey)
    {
        Console.WriteLine($"收到订单消息: {source}");
    }

    // cluster2 集群消费者
    [RabbitMQConsumer(ConfigId = "cluster2", QueueName = "log.queue", ConsumerName = "log.consumer")]
    public async Task HandleLog(string source, string routingKey)
    {
        Console.WriteLine($"收到日志消息: {source}");
    }

    // 交换机模式消费者
    [RabbitMQConsumer(
        QueueName = "notify.queue",
        Exchange = "notify.exchange",
        RoutingKey = "notify.#",
        ExchangeType = ExchangeTypeConst.Topic,
        ConsumerName = "notify.consumer")]
    public async Task HandleNotify(string source, string routingKey)
    {
        Console.WriteLine($"收到通知消息: {source}");
    }
}
```

### 4. 发送消息

`IRabbitMqTransport` 提供三种发送方式：

```csharp
public class MyProducer
{
    private readonly IRabbitMqTransport _transport;

    public MyProducer(IRabbitMqTransport transport)
    {
        _transport = transport;
    }

    public async Task SendAsync()
    {
        // 方式一：队列模式便捷方法（泛型，自动 JSON 序列化）
        await _transport.SendAsync("default", "order.queue", new { Id = 1, Name = "test" });

        // 方式二：交换机模式便捷方法（泛型，自动 JSON 序列化）
        await _transport.SendAsync("default", "notify.exchange", "notify.order", new { Id = 1, Content = "hello" });

        // 方式三：底层 TransportMessage 方法（手动控制）
        var body = Encoding.UTF8.GetBytes("Hello World");
        await _transport.SendAsync(new TransportMessage(body)
        {
            QueueName = "order.queue",
            RoutingKey = "order.queue",
            Durable = true
        });

        // 发送到 cluster2 集群
        await _transport.SendAsync("cluster2", "log.queue", new { Level = "Info", Message = "test" });
    }
}
```

### 5. 使用 IConsumerService 直接消费（简化 API）

除了通过 `[RabbitMQConsumer]` 特性自动扫描注册消费者外，还可以直接注入 `IConsumerService` 进行更灵活的消费：

```csharp
public class ConsumerWorker : BackgroundService
{
    private readonly IConsumerService _consumerService;

    public ConsumerWorker(IConsumerService consumerService)
    {
        _consumerService = consumerService;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queueTask = Task.Run(() => ListenQueue(stoppingToken), stoppingToken);
        var exchangeTask = Task.Run(() => ListenExchange(stoppingToken), stoppingToken);
        return Task.WhenAny(queueTask, exchangeTask);
    }

    // 队列消费
    private async Task ListenQueue(CancellationToken ct)
    {
        await _consumerService.ListenAsync<string>(
            configId: "default",
            queueName: "order.queue",
            allwaysRunActionTask: async (body, bodyString, routingKey) =>
            {
                Console.WriteLine($"[队列消费] Body: {body}, RoutingKey: {routingKey}");
                return true; // true=Ack, false=Reject并重新入队
            },
            prefetchCount: 10,
            cancellationToken: ct);
    }

    // 路由消费（交换机模式）
    private async Task ListenExchange(CancellationToken ct)
    {
        await _consumerService.ListenByExchangeAsync<string>(
            configId: "default",
            exchange: "my.exchange",
            exchangeType: "topic",
            routingKey: "order.#",
            queueName: "my.queue",
            allwaysRunActionTask: async (body, bodyString, routingKey) =>
            {
                Console.WriteLine($"[路由消费] Body: {body}, RoutingKey: {routingKey}");
                return true;
            },
            prefetchCount: 10,
            cancellationToken: ct);
    }
}
```

**`allwaysRunActionTask` 回调参数说明：**

| 参数 | 类型 | 说明 |
|---|---|---|
| 第 1 个参数 | `T` | 消息体反序列化后的对象（`string` 类型返回 UTF-8 字符串，`byte[]` 返回原始字节，其他类型 JSON 反序列化） |
| 第 2 个参数 | `string` | 消息体原始字符串（`ea.Body` 的 UTF-8 编码） |
| 第 3 个参数 | `string` | 路由键（RoutingKey） |
| 返回值 | `Task<bool>` | `true` 确认消息（Ack），`false` 拒绝消息（Reject）并重新入队 |

**可选参数：**

| 参数 | 默认值 | 说明 |
|---|---|---|
| `prefetchCount` | 50 | QoS 预取消息数量 |
| `autoAck` | false | 是否自动确认 |
| `durable` | true | 队列/交换机是否持久化 |
| `exclusive` | false | 是否独占队列 |
| `autoDelete` | false | 是否自动删除 |
| `consumerTag` | "" | 消费者标签（空则由 RabbitMQ 自动生成） |
| `ackBatchSize` | 100 | 批量确认累积条数，达到即 flush；`<=0` 退化为逐条确认 |
| `ackFlushIntervalMs` | 10 | 批量确认定时刷新间隔（毫秒），低吞吐兜底；`<=0` 退化为逐条确认 |

> **说明**：`ackBatchSize` / `ackFlushIntervalMs` 自下一版本迭代（见 `PLAN.md`）起生效，声明式消费通过 `[RabbitMQConsumer(AckBatchSize, AckFlushIntervalMs)]` 配置，命令式消费通过 `IConsumerService` 同名可选参数配置。

---

## 设计优点

### 🔌 零代码消费注册
实现 `IToolBoxRabbitMQ` 接口 + 方法上标记 `[RabbitMQConsumer]` 特性，框架自动扫描注册，无需手写一行连接、声明、监听代码。

### 🌐 多集群原生支持
通过 `ConfigId` 一个属性，即可将不同消费者绑定到不同 MQ 集群，生产者也能按 `ConfigId` 发送到任意集群。配置以数组形式管理，新增集群只需添加一个配置项。

### 🔧 单集群向后兼容
原有的 `AddRabbitMQ(options => { ... })` 单集群注册方式完全兼容，不破坏现有代码。

### 🏊 连接通道池复用
`ConnectionChannelPool` 使用 `SemaphoreSlim` + `ConcurrentQueue` 实现异步通道租借与复用，避免频繁创建销毁通道。`CachedDeclareChannel` 对同一 exchange/queue 的声明操作只执行一次，减少发布时网络往返。支持 `PoolSize`（初始通道数）、`AbsoluteMaxPoolSize`（动态扩展上限，信号量满时 CAS 无锁翻倍扩容）、`MinPoolSize`（最小保留数）、`IdleTimeout`（空闲超时回收）配置，高峰自动扩容，闲时自动收缩。`OperationCanceledException` 直接向上传播，不被包装为发布失败异常。

### 📡 自动声明与绑定
消费者启动时自动声明队列/交换机并绑定（交换机模式），设置 QoS 预取数量，开发者无需关心基础设施初始化。

### ⚡ 并发消费控制
通过 `ListenerCount` 属性启动多个消费者实例并行消费同一队列；通过 `PrefetchCount` 控制消息预取数量，防止消费者被压垮；通过 `ConsumerConcurrent` 控制单个消费者实例内的消息并发处理级别（0=顺序处理，>0=信号量控制并行数）。

### 🛡️ ConsumerName 唯一性校验
启动时自动校验所有消费者的 `ConsumerName` 全局唯一，重复则立即抛出异常并提示冲突的方法信息，避免运行时消息路由混乱。

### 🔄 生命周期管理
`Bootstrapper` 作为 `BackgroundService` 统一管理所有消费者的启动、停止和资源释放，支持优雅停机和健康检查。

### 🏭 工厂模式创建客户端
`RabbitMqConsumerClientFactory` 通过 `IConnectionChannelPoolManager` 按 ConfigId 自动选择对应集群的连接池创建客户端，上层代码无需感知底层集群差异。

### 📨 消息持久化与确认机制
默认开启消息持久化（`DeliveryModes.Persistent`）和手动确认（`AutoAck = false`），确保消息不丢失。处理成功后自动 Ack，处理失败自动 Reject 并重新入队。

### 📦 批量累积确认（AckBatcher）
消费确认（ACK）由新增的 `AckBatcher` 组件统一调度（下一版本迭代，详见 `PLAN.md`）：在单个 channel 上维护「已确认连续 deliveryTag 区间」，当累积完成的消息达到 `AckBatchSize`（默认 100）或距上次刷新超过 `AckFlushIntervalMs`（默认 10ms）任一条件触发时，发送一次 `BasicAckAsync(maxTag, multiple:true)` 累积确认，将高吞吐场景下的 AMQP 确认帧数量降低 1~2 个数量级。并发乱序完成（`ConsumerConcurrent > 1`）时，依据完成顺序补齐连续前缀，避免 `multiple:true` 误确认未处理消息；Reject 前会先 flush 连续 ACK，再单独拒绝（multiple:false）。`<=0` 时退化为逐条确认，便于灰度。

### 🔗 多主机集群支持
`HostName` 支持配置多个地址，实现 RabbitMQ 集群的高可用和负载均衡。

### 📊 丰富的日志回调
`RabbitMqBasicConsumer` 监听消费者注册、取消、关闭等生命周期事件，通过回调委托通知上层，便于监控和问题排查。

---

## 许可证

MIT License

## 作者

**何志鹏**
