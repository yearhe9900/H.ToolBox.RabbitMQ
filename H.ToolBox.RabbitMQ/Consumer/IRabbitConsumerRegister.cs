namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ 消费者注册器接口
    /// 定义消费者注册、健康检查和生命周期管理的标准契约
    /// </summary>
    /// <remarks>
    /// 消费者注册器是消费者管理层的核心组件，负责：
    /// <list type="bullet">
    ///   <item><description>通过 <see cref="IConsumerServiceSelector"/> 扫描并发现所有消费者方法</description></item>
    ///   <item><description>通过 <see cref="IRabbitMqConsumerClientFactory"/> 为每个消费者创建客户端实例</description></item>
    ///   <item><description>管理消费者的启动、停止、重启等生命周期</description></item>
    ///   <item><description>监控消费者健康状态</description></item>
    /// </list>
    /// </remarks>
    public interface IRabbitConsumerRegister : IDisposable
    {
        /// <summary>
        /// 检查消费者是否处于健康状态
        /// </summary>
        /// <returns>如果消费者健康返回 true，否则返回 false</returns>
        bool IsHealthy();

        /// <summary>
        /// 异步重启消费者注册
        /// 当消费者处于不健康状态或强制重启时，先停止现有消费者再重新启动
        /// </summary>
        /// <param name="force">
        /// 是否强制重启。为 true 时无论健康状态都执行重启；
        /// 为 false 时仅在消费者不健康时才重启
        /// </param>
        /// <returns>表示异步重启操作的任务</returns>
        ValueTask ReStartAsync(bool force = false);

        /// <summary>
        /// 异步启动消费者注册
        /// 扫描所有消费者方法并创建对应的消费者客户端进行消息监听
        /// </summary>
        /// <param name="stoppingToken">停止令牌，用于取消操作</param>
        /// <returns>表示异步启动操作的任务</returns>
        ValueTask StartAsync(CancellationToken stoppingToken);
    }
}
