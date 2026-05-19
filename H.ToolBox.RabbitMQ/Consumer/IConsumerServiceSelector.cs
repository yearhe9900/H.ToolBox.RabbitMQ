using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// 消费者服务选择器接口
    /// 定义扫描和选择消费者方法的标准契约
    /// </summary>
    public interface IConsumerServiceSelector
    {
        /// <summary>
        /// 扫描并返回所有消费者方法的候选列表
        /// </summary>
        /// <returns>包含消费者方法信息的只读列表</returns>
        IReadOnlyList<MethodScanResult> SelectCandidates();
    }
}