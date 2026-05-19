namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ消费者客户端工厂接口
    /// 负责创建和初始化 <see cref="IRabbitMqConsumerClient"/> 实例
    /// </summary>
    public interface IRabbitMqConsumerClientFactory
    {
        /// <summary>
        /// 异步创建并初始化一个RabbitMQ消费者客户端实例（使用默认集群）
        /// </summary>
        /// <returns>已连接并准备就绪的消费者客户端</returns>
        Task<IRabbitMqConsumerClient> CreateAsync();

        /// <summary>
        /// 异步创建并初始化一个RabbitMQ消费者客户端实例（指定集群 ConfigId）
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <returns>已连接并准备就绪的消费者客户端</returns>
        Task<IRabbitMqConsumerClient> CreateAsync(string configId);
    }
}
