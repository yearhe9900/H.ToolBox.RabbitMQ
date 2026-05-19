using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ
{
    /// <summary>
    /// RabbitMQ消费者特性，用于标记方法为RabbitMQ消息消费者
    /// 应用此特性的方法将被自动扫描并注册为消费者
    /// </summary>
    /// <remarks>
    /// 使用方式：在实现 <see cref="IToolBoxRabbitMQ"/> 接口的类中，对需要消费消息的方法添加此特性。
    /// <code>
    /// [RabbitMQConsumer(QueueName = "my.queue", ConsumerName = "my.consumer")]
    /// public void HandleMessage(string source, string routingKey) { ... }
    /// </code>
    /// </remarks>
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public class RabbitMQConsumerAttribute : Attribute
    {
        /// <summary>
        /// 获取或设置集群配置标识（ConfigId），用于指定此消费者连接到哪个 RabbitMQ 集群
        /// 默认值为 "default"，对应 <see cref="Model.RabbitMQOptions.ConfigId"/> 中配置的集群标识
        /// 当需要将不同方法绑定到不同集群时，通过此属性进行区分
        /// </summary>
        public string ConfigId { get; set; } = "default";

        /// <summary>
        /// 获取或设置要消费的队列名称
        /// </summary>
        public string? QueueName { get; set; }

        /// <summary>
        /// 获取或设置消费者名称，用于标识和日志记录
        /// 默认值为随机生成的 GUID 字符串
        /// </summary>
        public string? ConsumerName { get; set; } = Guid.NewGuid().ToString();

        /// <summary>
        /// 获取或设置交换机名称，用于交换机模式的消息路由
        /// 当设置了 Exchange 和 RoutingKey 时，将使用交换机模式进行消息消费
        /// </summary>
        public string? Exchange { get; set; }

        /// <summary>
        /// 获取或设置交换机类型（如 direct、fanout、topic、headers），默认为直连交换机
        /// 可使用 <see cref="ExchangeTypeConst"/> 中定义的常量
        /// </summary>
        public string? ExchangeType { get; set; }

        /// <summary>
        /// 获取或设置路由键，用于消息从交换机到队列的路由匹配
        /// 当指定了 Exchange 和 RoutingKey 时，将使用交换机模式
        /// </summary>
        public string? RoutingKey { get; set; }

        /// <summary>
        /// 获取或设置队列和交换机是否持久化，默认为 true
        /// 持久化的队列和交换机在 RabbitMQ 重启后不会丢失
        /// </summary>
        public bool Durable { get; set; } = true;

        /// <summary>
        /// 获取或设置是否自动确认消息，默认为 false
        /// 当设置为 false 时，消息处理成功后需要手动调用 CommitAsync 进行确认；
        /// 当设置为 true 时，消息在发送给消费者后立即被确认，无需手动确认
        /// </summary>
        public bool AutoAck { get; set; } = false;

        /// <summary>
        /// 获取或设置预取消息数量（QoS），默认为 50
        /// 控制消费者在未确认消息的情况下，RabbitMQ 服务器最多向其推送的消息数量，
        /// 用于防止消费者被大量消息压垮
        /// </summary>
        public ushort PrefetchCount { get; set; } = 50;

        /// <summary>
        /// 获取或设置消费者监听器数量（并发消费者实例数），默认为 1
        /// 设置大于 1 的值可以启动多个消费者实例并行消费同一队列中的消息，
        /// 提高消息处理的吞吐量
        /// </summary>
        public int ListenerCount { get; set; } = 1;

        /// <summary>
        /// 获取或设置消费者监听超时时间（秒），默认为 30 秒
        /// 该超时时间用于控制消费者等待消息的轮询间隔
        /// </summary>
        public int WaitOneTimeOut { get; set; } = 30;

        /// <summary>
        /// 获取或设置消费者并发处理级别，默认为 1
        /// 当值为 0 时，消息在当前上下文中顺序处理；
        /// 当值大于 0 时，使用信号量控制并发数量，消息在独立的 Task 中并行处理
        /// </summary>
        public byte ConsumerConcurrent { get; set; } = 1;
    }
}