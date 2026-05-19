using System.Reflection;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 方法扫描结果类，封装消费者方法扫描过程中发现的方法信息
    /// </summary>
    public class MethodScanResult
    {
        /// <summary>
        /// 获取或设置方法所属的服务实例对象
        /// </summary>
        public object? Instance { get; set; }

        /// <summary>
        /// 获取或设置扫描到的方法信息
        /// </summary>
        public MethodInfo? MethodInfo { get; set; }

        /// <summary>
        /// 获取或设置方法上的消费者特性配置
        /// </summary>
        public RabbitMQConsumerAttribute? ConsumerAttribute { get; set; }
    }
}