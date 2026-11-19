using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Transport
{
    /// <summary>
    /// RabbitMQ传输消息类，封装消息的基本信息和内容
    /// </summary>
    public struct TransportMessage
    {
        /// <summary>
        /// 初始化传输消息实例
        /// </summary>
        /// <param name="body">消息体内容</param>
        public TransportMessage(ReadOnlyMemory<byte> body)
        {
            Body = body;
            QueueType = QueueType.Classic;
            QuorumGroupSize = null;
            // net6.0 默认语言版本为 C# 10，struct 构造函数要求显式赋值所有自动属性
            MessageId = null;
        }

        /// <summary>
        /// 获取或设置集群配置标识（ConfigId），用于指定消息发送到哪个 RabbitMQ 集群
        /// 默认值为 "default"，对应 <see cref="Model.RabbitMQOptions.ConfigId"/> 中配置的集群标识
        /// </summary>
        public string ConfigId { get; set; } = "default";

        /// <summary>
        /// 获取或设置交换机名称
        /// </summary>
        public string Exchange { get; set; } = string.Empty;

        /// <summary>
        /// 获取或设置交换机类型，默认为直连交换机（direct）
        /// </summary>
        public string ExchangeType { get; set; } = ExchangeTypeConst.Direct;

        /// <summary>
        /// 获取或设置路由键，用于消息路由到指定队列
        /// </summary>
        public string RoutingKey { get; set; } = string.Empty;

        /// <summary>
        /// 获取或设置队列名称
        /// </summary>
        public string QueueName { get; set; } = string.Empty;

        /// <summary>
        /// 获取或设置队列是否持久化（可选）
        /// </summary>
        public bool? Durable { get; set; } = true;

        /// <summary>
        /// 获取或设置队列类型，默认为 Classic（经典队列）
        /// 设置为 Quorum 可使用仲裁队列
        /// </summary>
        public QueueType QueueType { get; set; } = QueueType.Classic;

        /// <summary>
        /// 获取或设置仲裁队列初始组大小，仅当 QueueType 为 Quorum 时生效
        /// null 表示使用全局配置或服务器默认值
        /// </summary>
        public int? QuorumGroupSize { get; set; }

        /// <summary>
        /// 获取或设置队列是否为独占队列，默认为 false
        /// </summary>
        public bool Exclusive { get; set; } = false;

        /// <summary>
        /// 获取或设置队列/交换机是否自动删除，默认为 false
        /// </summary>
        public bool AutoDelete { get; set; } = false;

        /// <summary>
        /// 获取或设置消息无法路由时是否返回给发布者，默认为 false。
        /// 设置为 true 时，如果没有队列绑定匹配 routingKey，Broker 会将消息退回发布者。
        /// </summary>
        public bool Mandatory { get; set; } = false;

        /// <summary>
        /// 获取消息体的原始字节内容
        /// </summary>
        public ReadOnlyMemory<byte> Body { get; }

        /// <summary>
        /// 获取或设置消息体的字符串内容（UTF-8 编码）
        /// 由消费者客户端在接收消息时自动从 <see cref="Body"/> 解码设置，
        /// 用于传递给业务方法处理
        /// </summary>
        public string BodyContent { get; set; } = string.Empty;

        /// <summary>
        /// 获取或设置消息是否为重新投递（由消费侧从 basic.deliver 的 redelivered 标志填充）。
        /// 业务侧可据此区分首次投递与重投（例如从死信队列重新发布后的消息），
        /// 从而决定是否跳过副作用不可重复的操作。
        /// </summary>
        public bool Redelivered { get; set; } = false;

        /// <summary>
        /// 获取或设置消息唯一标识（由消费侧从 <c>message-id</c> 属性填充，可能为 null）。
        /// 用于日志追踪与幂等判断。
        /// </summary>
        public string? MessageId { get; set; }
    }
}