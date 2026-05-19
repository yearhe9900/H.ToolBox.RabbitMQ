using System;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 消息发布失败异常
    /// 当RabbitMQ消息发布操作失败时抛出此异常
    /// </summary>
    public class PublisherSentFailedException : Exception
    {
        /// <summary>
        /// 使用指定的错误消息初始化异常实例
        /// </summary>
        /// <param name="message">描述错误的消息</param>
        public PublisherSentFailedException(string message) : base(message)
        {
        }

        /// <summary>
        /// 使用指定的错误消息和内部异常初始化异常实例
        /// </summary>
        /// <param name="message">描述错误的消息</param>
        /// <param name="ex">导致当前异常的内部异常</param>
        public PublisherSentFailedException(string message, Exception? ex) : base(message, ex)
        {
        }
    }
}
