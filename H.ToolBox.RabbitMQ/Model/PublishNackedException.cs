using System;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// Broker 对本次发布返回 basic.nack 时抛出的异常。
    /// 表示 Broker 未能接收该消息，属于可恢复错误，发布侧会按 MaxRetries 进行指数退避重试。
    /// </summary>
    public class PublishNackedException : Exception
    {
        /// <summary>
        /// 使用指定的错误消息初始化异常实例
        /// </summary>
        public PublishNackedException(string message) : base(message)
        {
        }

        /// <summary>
        /// 使用指定的错误消息和内部异常初始化异常实例
        /// </summary>
        public PublishNackedException(string message, Exception? innerException) : base(message, innerException)
        {
        }
    }
}
