namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// RabbitMQ 队列类型枚举
    /// </summary>
    public enum QueueType
    {
        /// <summary>
        /// 经典队列（默认），适用于单节点或不需要高可用的场景
        /// </summary>
        Classic,

        /// <summary>
        /// 仲裁队列，基于 Raft 共识协议，适用于集群模式下需要高可用和数据安全的场景
        /// 需要 RabbitMQ 3.8.0+
        /// </summary>
        Quorum
    }
}
