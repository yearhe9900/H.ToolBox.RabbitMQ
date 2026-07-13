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

        /// <summary>
        /// 获取或设置编译后的委托，用于替代反射调用，提升消息处理性能
        /// 委签名为 Func&lt;string, string, Task&lt;bool&gt;&gt;，统一处理各种返回类型
        /// </summary>
        public Func<string, string, Task<bool>>? CompiledDelegate { get; set; }
    }
}