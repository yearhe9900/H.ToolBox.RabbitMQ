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
        /// </summary>
        public string Password { get; set; } = string.Empty;

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
        /// 消费者线程数，控制并发消费的并行度
        /// </summary>
        public int ConsumerThreadCount { get; set; } = 1;

        /// <summary>
        /// 连接工厂自定义配置回调
        /// 允许对 <see cref="ConnectionFactory"/> 进行额外的个性化设置（如SSL、超时时间等）
        /// </summary>
        public Action<ConnectionFactory>? ConnectionFactoryOptions { get; set; }
    }
}