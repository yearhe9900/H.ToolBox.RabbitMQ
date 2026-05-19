namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 连接通道池管理器接口
    /// 负责管理多个 RabbitMQ 集群的连接通道池，通过 ConfigId 区分不同集群
    /// </summary>
    public interface IConnectionChannelPoolManager
    {
        /// <summary>
        /// 根据 ConfigId 获取对应的连接通道池
        /// </summary>
        /// <param name="configId">集群配置标识，默认为 "default"</param>
        /// <returns>对应集群的连接通道池</returns>
        /// <exception cref="KeyOutOfRangeException">当指定的 ConfigId 未注册时抛出</exception>
        IConnectionChannelPool GetPool(string configId = "default");
    }
}