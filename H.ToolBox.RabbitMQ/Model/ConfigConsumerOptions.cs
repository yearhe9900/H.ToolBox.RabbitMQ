using System.Collections.Generic;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 配置驱动的消费者配置选项
    /// </summary>
    public class ConfigConsumerOptions
    {
        /// <summary>
        /// 消费者配置列表
        /// </summary>
        public List<ConfigConsumerItem> Consumers { get; set; } = new List<ConfigConsumerItem>();
    }

    /// <summary>
    /// 单个消费者配置项
    /// </summary>
    public class ConfigConsumerItem
    {
        /// <summary>
        /// 集群配置标识，对应 RabbitMQOptions.ConfigId
        /// </summary>
        public string ConfigId { get; set; } = "default";

        /// <summary>
        /// 要消费的队列名称
        /// </summary>
        public string QueueName { get; set; } = string.Empty;

        /// <summary>
        /// 消费者类的完整类型名（命名空间.类名）
        /// </summary>
        public string ClassName { get; set; } = string.Empty;

        /// <summary>
        /// 消费者方法名
        /// </summary>
        public string MethodName { get; set; } = string.Empty;

        /// <summary>
        /// 消费者名称，用于标识和日志记录
        /// 默认为 null，启动时自动生成为 "类名.方法名" 格式
        /// </summary>
        public string? ConsumerName { get; set; }

        /// <summary>
        /// 交换机名称，用于交换机模式的消息路由
        /// </summary>
        public string? Exchange { get; set; }

        /// <summary>
        /// 交换机类型（如 direct、fanout、topic、headers）
        /// </summary>
        public string? ExchangeType { get; set; }

        /// <summary>
        /// 路由键，用于消息从交换机到队列的路由匹配
        /// </summary>
        public string? RoutingKey { get; set; }

        /// <summary>
        /// 队列和交换机是否持久化，默认为 true
        /// </summary>
        public bool Durable { get; set; } = true;

        /// <summary>
        /// 是否自动确认消息，默认为 false
        /// </summary>
        public bool AutoAck { get; set; } = false;

        /// <summary>
        /// 预取消息数量（QoS），默认为 50
        /// </summary>
        public ushort PrefetchCount { get; set; } = 50;

        /// <summary>
        /// 消费者监听器数量（并发消费者实例数），默认为 1
        /// </summary>
        public int ListenerCount { get; set; } = 1;

        /// <summary>
        /// 消费者监听超时时间（秒），默认为 30 秒
        /// </summary>
        public int WaitOneTimeOut { get; set; } = 30;

        /// <summary>
        /// 消费者并发处理级别，默认为 1
        /// </summary>
        public byte ConsumerConcurrent { get; set; } = 1;

        /// <summary>
        /// 队列类型，默认为 Classic（经典队列）
        /// </summary>
        public QueueType QueueType { get; set; } = QueueType.Classic;

        /// <summary>
        /// 仲裁队列初始组大小（副本数），仅当 QueueType 为 Quorum 时生效
        /// </summary>
        public int QuorumGroupSize { get; set; }

        /// <summary>
        /// 队列是否为独占队列，默认为 false
        /// </summary>
        public bool Exclusive { get; set; } = false;

        /// <summary>
        /// 队列是否自动删除，默认为 false
        /// </summary>
        public bool AutoDelete { get; set; } = false;
    }
}
