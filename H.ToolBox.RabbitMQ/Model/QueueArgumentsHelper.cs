using System.Collections.Generic;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 队列声明参数构建辅助类
    /// 根据队列类型生成对应的 RabbitMQ 队列声明 arguments 字典
    /// </summary>
    public static class QueueArgumentsHelper
    {
        /// <summary>
        /// 根据队列类型和仲裁组大小构建队列声明参数
        /// </summary>
        /// <param name="queueType">队列类型</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小（仅 Quorum 类型有效），null 表示使用 RabbitMQ 默认值</param>
        /// <returns>队列声明参数字典，Classic 类型返回 null</returns>
        public static IDictionary<string, object?>? BuildArguments(QueueType queueType, int? quorumGroupSize = null)
        {
            if (queueType == QueueType.Classic)
                return null;

            var arguments = new Dictionary<string, object?>
            {
                { "x-queue-type", "quorum" }
            };

            if (quorumGroupSize.HasValue && quorumGroupSize.Value > 0)
            {
                arguments["x-quorum-initial-group-size"] = quorumGroupSize.Value;
            }

            return arguments;
        }

        /// <summary>
        /// 获取队列类型对应的 x-queue-type 字符串值
        /// </summary>
        /// <param name="queueType">队列类型</param>
        /// <returns>RabbitMQ 队列类型字符串</returns>
        public static string GetQueueTypeString(QueueType queueType)
        {
            return queueType switch
            {
                QueueType.Quorum => "quorum",
                _ => "classic"
            };
        }
    }
}
