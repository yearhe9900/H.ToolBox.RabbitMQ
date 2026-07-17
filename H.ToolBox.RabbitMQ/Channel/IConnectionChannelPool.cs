using RabbitMQ.Client;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 连接通道池接口
    /// </summary>
    /// <remarks>老版本的MQ客户端会出现大量作废channel无法释放的问题，这次采用通道池的概念，解决这个问题</remarks>
    public interface IConnectionChannelPool
    {
        /// <summary>
        /// 是否启用了发布确认模式
        /// </summary>
        bool IsPublishConfirms { get; }

        /// <summary>
        /// 异步获取RabbitMQ连接实例
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>RabbitMQ连接对象</returns>
        Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// 从通道池中租借一个可用的RabbitMQ通道
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>可用的RabbitMQ通道</returns>
        Task<IChannel> Rent(CancellationToken cancellationToken = default);

        /// <summary>
        /// 归还通道到通道池中以供复用
        /// </summary>
        /// <param name="context">要归还的RabbitMQ通道</param>
        /// <returns>归还是否成功（true表示成功回收到池中，false表示通道被释放）</returns>
        bool Return(IChannel context);

        /// <summary>
        /// 预热连接池，预先创建所有配置的 TCP 连接
        /// 避免启动时集中创建连接导致的瞬时压力，提高系统启动稳定性
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步预热操作的任务</returns>
        Task WarmupAsync(CancellationToken cancellationToken = default);
    }
}
