using RabbitMQ.Client;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// RabbitMQ配置选项类
    /// </summary>
    public class RabbitMQOptions
    {
        /// <summary>
        /// 集群配置标识，用于区分不同的 RabbitMQ 集群连接
        /// 默认值为 "default"，当连接多个集群时，每个集群应设置不同的 ConfigId
        /// </summary>
        public string ConfigId { get; set; } = "default";

        /// <summary>
        /// RabbitMQ服务器地址
        /// </summary>
        public List<string> HostName { get; set; } = new();

        /// <summary>
        /// RabbitMQ服务器端口
        /// </summary>
        public int Port { get; set; } = 5672;

        /// <summary>
        /// RabbitMQ用户名
        /// </summary>
        public string UserName { get; set; } = "guest";

        /// <summary>
        /// RabbitMQ密码
        /// 默认值 "guest" 匹配 RabbitMQ 默认凭据（guest/guest），
        /// 生产环境请务必修改为强密码
        /// </summary>
        public string Password { get; set; } = "guest";

        /// <summary>
        /// 虚拟主机
        /// </summary>
        public string VirtualHost { get; set; } = "/";

        /// <summary>
        /// 客户端名称
        /// </summary>
        public string ClientProvidedName { get; set; } = "H.ToolBox.RabbitMQ.Client";

        /// <summary>
        /// 是否启用发布确认模式
        /// </summary>
        /// <remarks>
        /// 启用后，生产者投递消息时Broker会返回应答，以确认消息是否正常发送到交换机。
        /// 建议在生产环境中启用此选项以确保消息可靠性。
        /// </remarks>
        public bool PublishConfirms { get; set; } = true;

        /// <summary>
        /// 通道池初始大小，控制初始并发通道数量，默认值为15
        /// </summary>
        public int PoolSize { get; set; } = 15;

        /// <summary>
        /// 通道池绝对上限，动态扩展时不超过此值，默认值为128。
        /// 设置为0或负数时，自动取 PoolSize 值（即禁止动态扩展）。
        /// </summary>
        public int AbsoluteMaxPoolSize { get; set; } = 128;

        /// <summary>
        /// 通道池最小保留数量，空闲回收时不会低于此值，默认值为0（不保留）
        /// </summary>
        public int MinPoolSize { get; set; } = 0;

        /// <summary>
        /// 通道空闲超时时间，超过此时间未被使用的通道将被自动回收，默认60秒
        /// 设置为 TimeSpan.Zero 可禁用空闲回收
        /// </summary>
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(60);

        /// <summary>
        /// 连接池大小（TCP 连接数量），默认值为 2。
        /// 单个 RabbitMQ 连接上的所有 AMQP 帧由同一条 TCP 通道串行处理，
        /// 在推送压力较大时容易成为瓶颈；配置多条连接可将通道与发布请求均匀分摊到多条 TCP 连接上，
        /// 显著提升高吞吐场景下的发布性能并降低相互阻塞。
        /// 取值建议：高吞吐生产者 2~4；普通场景 2 即可；设置为 1 将回退为原有的单连接行为。
        /// 内部会强制约束在 [1, 64] 区间。
        /// </summary>
        public int ConnectionCount { get; set; } = 2;

        /// <summary>
        /// 发布失败时的瞬时故障重试次数，默认值为 3。
        /// 仅针对连接/通道中断等可恢复的瞬时错误（如 AlreadyClosedException、
        /// OperationInterruptedException、Broker 不可达、底层 IO/网络异常、超时）生效；
        /// 参数校验失败、消息格式错误等不可恢复错误不会重试。
        /// 重试采用指数退避（100ms、200ms、400ms …，上限 2s）。
        /// 设置为 0 可禁用重试。
        /// </summary>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// 消费者线程数，控制并发消费的并行度
        /// </summary>
        public int ConsumerThreadCount { get; set; } = 1;

        /// <summary>
        /// 消费者分发并发度，控制单个消费者通道上同时分发的消息数量。
        /// 默认值为 1（串行分发）。设置为大于 1 时，客户端会同时向 ReceivedAsync 回调
        /// 分发多条消息，配合 prefetchCount 可显著提升慢消费者的吞吐量。
        /// 建议值：prefetchCount / 2 或与业务处理并发度一致。
        /// 
        /// 注意：此设置仅对 IConsumerService（兼容性消费者 API）有效。
        /// RabbitMQConsumer 使用 RabbitMqBasicConsumer 内部的 Task.Run 并发机制，
        /// 其连接池的 ConsumerDispatchConcurrency 固定为 1，避免并发机制冲突。
        /// </summary>
        public ushort ConsumerDispatchConcurrency { get; set; } = 1;

        /// <summary>
        /// 默认队列类型，控制新声明队列的类型。
        /// Classic 为经典队列（默认），Quorum 为仲裁队列（推荐集群模式使用）。
        /// 可在 <see cref="RabbitMQConsumerAttribute.QueueType"/> 中针对单个消费者覆盖此设置。
        /// </summary>
        public QueueType DefaultQueueType { get; set; } = QueueType.Classic;

        /// <summary>
        /// 仲裁队列初始组大小（副本数），仅当队列类型为 Quorum 时生效。
        /// 设置为 null 表示使用 RabbitMQ 服务器默认值（通常为 3）。
        /// 建议设置为奇数（如 3、5），且不超过集群节点数。
        /// </summary>
        public int? QuorumGroupSize { get; set; }

        /// <summary>
        /// 是否启用死信（DLX）拓扑，默认 true。
        /// 启用后所有队列声明都会附带 <c>x-dead-letter-exchange</c> 与
        /// <c>x-dead-letter-routing-key</c>，消费失败的消息以 <c>requeue:false</c>
        /// 拒绝后进入死信队列，避免毒消息无限循环重投。
        /// </summary>
        /// <remarks>
        /// 死信参数是队列级属性：生产者与消费者声明同一队列时必须使用完全一致的参数，
        /// 否则 RabbitMQ 会返回 PRECONDITION_FAILED。因此该配置按 ConfigId 集群级统一，
        /// 刻意不提供单消费者覆盖入口。
        /// </remarks>
        public bool EnableDeadLetter { get; set; } = true;

        /// <summary>
        /// 死信交换机名称，默认 "H.ToolBox.dlx"。
        /// 由所有声明队列的路径统一声明为 durable 的 direct 交换机。
        /// </summary>
        public string DeadLetterExchange { get; set; } = "H.ToolBox.dlx";

        /// <summary>
        /// 死信交换机类型，默认 "direct"。
        /// 使用 direct 交换机配合队列名作为 routing key，保证每个业务队列有独立的死信队列。
        /// </summary>
        public string DeadLetterExchangeType { get; set; } = "direct";

        /// <summary>
        /// 死信队列名后缀，默认 ".dlq"。
        /// 业务队列 "order.queue" 对应的死信队列名为 "order.queue.dlq"。
        /// </summary>
        public string DeadLetterQueueSuffix { get; set; } = ".dlq";

        /// <summary>
        /// 连接工厂自定义配置回调
        /// 允许对 <see cref="ConnectionFactory"/> 进行额外的个性化设置（如SSL、超时时间等）
        /// </summary>
        public Action<ConnectionFactory>? ConnectionFactoryOptions { get; set; }
    }
}