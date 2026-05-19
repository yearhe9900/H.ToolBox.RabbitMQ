using H.ToolBox.RabbitMQ.Channel;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ消费者客户端工厂
    /// 负责创建 <see cref="RabbitMqConsumerClient"/> 实例并建立初始连接，支持多集群
    /// </summary>
    public class RabbitMqConsumerClientFactory : IRabbitMqConsumerClientFactory
    {
        private readonly IConnectionChannelPoolManager _poolManager;

        /// <summary>
        /// 初始化消费者客户端工厂实例
        /// </summary>
        /// <param name="poolManager">连接通道池管理器</param>
        public RabbitMqConsumerClientFactory(IConnectionChannelPoolManager poolManager)
        {
            _poolManager = poolManager;
        }

        /// <summary>
        /// 异步创建并初始化一个RabbitMQ消费者客户端实例（使用默认集群 "default"）
        /// </summary>
        /// <returns>已连接并准备就绪的消费者客户端</returns>
        public async Task<IRabbitMqConsumerClient> CreateAsync()
        {
            return await CreateAsync("default");
        }

        /// <summary>
        /// 异步创建并初始化一个RabbitMQ消费者客户端实例（指定集群 ConfigId）
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <returns>已连接并准备就绪的消费者客户端</returns>
        public async Task<IRabbitMqConsumerClient> CreateAsync(string configId)
        {
            var pool = _poolManager.GetPool(configId);
            var client = new RabbitMqConsumerClient(pool);
            await client.ConnectAsync();
            return client;
        }
    }
}