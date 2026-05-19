namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// MQ 日志事件参数
    /// 封装消费者状态变化事件的详细信息，用于在消费者生命周期各阶段传递日志和状态信息
    /// </summary>
    /// <remarks>
    /// 该事件参数由 <see cref="Consumer.RabbitMqBasicConsumer"/> 在消费者状态发生变化时创建，
    /// 并通过回调委托传递给 <see cref="Consumer.RabbitConsumerRegister"/> 进行日志记录和健康状态管理。
    /// </remarks>
    public class LogMessageEventArgs : EventArgs
    {
        /// <summary>
        /// 获取或设置日志事件的原因或详细描述
        /// 通常包含消费者标签（consumerTag）、错误信息或其他上下文信息
        /// </summary>
        public string? Reason { get; set; }

        /// <summary>
        /// 获取或设置日志事件类型
        /// 标识消费者状态变化的具体类型（如注册、取消、关闭等）
        /// </summary>
        public MqLogType LogType { get; set; }
    }
}
