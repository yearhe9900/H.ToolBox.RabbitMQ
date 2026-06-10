using System.Collections.Generic;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 消费者监听配置选项
    /// 封装消费者启动监听时所需的所有配置参数，包括队列、交换机、QoS 等设置
    /// </summary>
    /// <remarks>
    /// 该类通常由 <see cref="RabbitConsumerRegister"/> 在启动消费者时根据
    /// <see cref="RabbitMQConsumerAttribute"/> 的配置自动构造，无需手动创建。
    /// </remarks>
    public class ListeningOptions
    {
        /// <summary>
        /// 获取或设置要消费的队列名称
        /// 在队列模式下为必填项；在交换机模式下也需要指定，用于声明要绑定到交换机的队列
        /// </summary>
        public string? QueueName { get; set; }

        /// <summary>
        /// 获取或设置消费者名称，用于在 RabbitMQ 管理界面中标识消费者
        /// 如果为空，RabbitMQ 会自动生成一个消费者标签
        /// </summary>
        public string? ConsumerName { get; set; }

        /// <summary>
        /// 获取或设置交换机名称
        /// 当指定了 Exchange 和 RoutingKey 时，将使用交换机模式进行消息消费
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
        /// 当设置为 false 时，消息处理成功后需要手动确认（Ack）；
        /// 当设置为 true 时，消息在发送给消费者后立即被确认
        /// </summary>
        public bool AutoAck { get; set; } = false;

        /// <summary>
        /// 获取或设置预取消息数量（QoS），默认为 50
        /// 控制消费者在未确认消息的情况下，RabbitMQ 服务器最多向其推送的消息数量，
        /// 用于防止消费者被大量消息压垮
        /// </summary>
        public ushort PrefetchCount { get; set; } = 50;

        /// <summary>
        /// 获取或设置消费者并发处理级别，默认为 0（顺序处理）
        /// 当值为 0 时，消息在当前上下文中顺序处理；
        /// 当值大于 0 时，使用信号量控制并发数量，消息在独立的 Task 中并行处理
        /// </summary>
        public byte ConsumerConcurrent { get; set; } = 0;

        /// <summary>
        /// 获取或设置队列类型，默认为 Classic（经典队列）
        /// 设置为 Quorum 可使用仲裁队列，适用于集群模式下需要高可用的场景
        /// </summary>
        public QueueType QueueType { get; set; } = QueueType.Classic;

        /// <summary>
        /// 获取或设置仲裁队列初始组大小，仅当 QueueType 为 Quorum 时生效
        /// null 表示使用全局配置或服务器默认值
        /// </summary>
        public int? QuorumGroupSize { get; set; }
    }
}
