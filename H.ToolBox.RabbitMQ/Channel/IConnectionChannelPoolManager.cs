using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 连接通道池管理器接口
    /// 负责管理多个 RabbitMQ 集群的连接通道池，通过 ConfigId 区分不同集群
    /// </summary>
    public interface IConnectionChannelPoolManager
    {
        /// <summary>
        /// 根据 ConfigId 获取对应的连接通道池（消费者专用）
        /// </summary>
        /// <param name="configId">集群配置标识，默认为 "default"</param>
        /// <returns>对应集群的连接通道池</returns>
        /// <exception cref="KeyOutOfRangeException">当指定的 ConfigId 未注册时抛出</exception>
        IConnectionChannelPool GetPool(string configId = "default");

        /// <summary>
        /// 根据 ConfigId 获取对应的生产者专用连接通道池
        /// 生产者池与消费者池使用独立的 TCP 连接，避免连接级背压（Flow Control）相互影响，
        /// 解决生产者高速发送时通过共享连接压制消费者 ACK 吞吐量的问题
        /// </summary>
        /// <param name="configId">集群配置标识，默认为 "default"</param>
        /// <returns>对应集群的生产者专用连接通道池</returns>
        /// <exception cref="KeyNotFoundException">当指定的 ConfigId 未注册时抛出</exception>
        IConnectionChannelPool GetProducerPool(string configId = "default");

        /// <summary>
        /// 根据 ConfigId 获取对应的 RabbitMQ 配置选项
        /// </summary>
        /// <param name="configId">集群配置标识，默认为 "default"</param>
        /// <returns>对应集群的配置选项</returns>
        /// <exception cref="KeyNotFoundException">当指定的 ConfigId 未注册时抛出</exception>
        RabbitMQOptions GetOptions(string configId = "default");

        /// <summary>
        /// 预热所有集群的连接池，预先创建所有配置的 TCP 连接
        /// 避免启动时集中创建连接导致的瞬时压力，提高系统启动稳定性
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步预热操作的任务</returns>
        Task WarmupAllAsync(CancellationToken cancellationToken = default);
    }
}