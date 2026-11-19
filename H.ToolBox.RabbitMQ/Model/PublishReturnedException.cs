using System;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// mandatory 消息被 Broker 以 basic.return 退回（没有队列匹配路由键）时抛出的异常。
    /// 属于拓扑/路由配置错误，重试不会改变结果，因此不参与发布重试。
    /// </summary>
    public class PublishReturnedException : Exception
    {
        /// <summary>
        /// 使用指定的错误消息初始化异常实例
        /// </summary>
        public PublishReturnedException(string message) : base(message)
        {
        }

        /// <summary>
        /// 使用指定的错误消息和内部异常初始化异常实例
        /// </summary>
        public PublishReturnedException(string message, Exception? innerException) : base(message, innerException)
        {
        }
    }
}
