namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// MQ 日志类型枚举
    /// 定义消费者生命周期中可能发生的状态变化事件类型
    /// </summary>
    /// <remarks>
    /// 该枚举由 <see cref="LogMessageEventArgs.LogType"/> 使用，
    /// 用于在 <see cref="Consumer.RabbitConsumerRegister"/> 中区分不同的日志事件并采取相应的处理策略。
    /// </remarks>
    public enum MqLogType
    {
        /// <summary>
        /// 消费者被取消（服务器端发起）
        /// 当 RabbitMQ 服务器主动取消消费者时触发（例如队列被删除），
        /// 此状态下消费者将被标记为不健康
        /// </summary>
        ConsumerCancelled,

        /// <summary>
        /// 消费者已注册（连接成功）
        /// 当消费者成功注册到 RabbitMQ 服务器并开始消费消息时触发，
        /// 此状态下消费者将被标记为健康
        /// </summary>
        ConsumerRegistered,

        /// <summary>
        /// 消费者已取消注册（客户端发起）
        /// 当客户端主动取消消费者注册时触发，
        /// 此状态仅记录日志，不影响健康状态
        /// </summary>
        ConsumerUnregistered,

        /// <summary>
        /// 消费者通道已关闭
        /// 当 RabbitMQ 通道因异常或主动关闭时触发，
        /// 此状态下消费者将被标记为不健康
        /// </summary>
        ConsumerShutdown,
    }
}
