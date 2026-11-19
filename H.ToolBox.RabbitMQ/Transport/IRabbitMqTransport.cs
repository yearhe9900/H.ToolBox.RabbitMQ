using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Transport
{
    /// <summary>
    /// RabbitMQ 消息传输接口，定义消息发送的标准契约。
    /// 支持两种发送模式：
    ///   1. 队列模式 —— 仅指定队列名，消息通过默认交换机路由到目标队列；
    ///   2. 交换机模式 —— 指定交换机名称 + 路由键，消息经由指定交换机进行路由。
    /// </summary>
    public interface IRabbitMqTransport
    {
        /// <summary>
        /// 发送一条消息到 RabbitMQ（底层通用方法）。
        /// 根据 <see cref="TransportMessage"/> 中设置的属性自动判断使用队列模式或交换机模式。
        /// 通过 <see cref="TransportMessage.ConfigId"/> 指定目标集群，默认为 "default"。
        /// </summary>
        /// <param name="message">传输消息对象，包含消息体、路由信息及集群标识</param>
        /// <param name="cancellationToken">用于取消操作的令牌</param>
        /// <returns>操作结果，成功返回 <see cref="OperateResult.Success"/>，失败时包含异常和错误详情</returns>
        Task<OperateResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default);

        /// <summary>
        /// 以队列模式发送消息（泛型便捷方法）。
        /// 内部将消息对象序列化为 JSON 字节后，通过默认交换机路由到指定队列。
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="configId">集群配置标识，对应 <see cref="Model.RabbitMQOptions.ConfigId"/></param>
        /// <param name="queueName">目标队列名称，同时作为路由键使用</param>
        /// <param name="msg">待发送的消息对象，将被 JSON 序列化</param>
        /// <param name="durable">队列是否持久化，默认为 true，持久化队列在 RabbitMQ 重启后不会丢失</param>
        /// <param name="queueType">队列类型，默认为 Classic（经典队列）</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小，仅当 queueType 为 Quorum 时生效</param>
        /// <param name="cancellationToken">用于取消操作的令牌</param>
        /// <returns>操作结果</returns>
        Task<OperateResult> SendAsync<T>(string configId, string queueName, T msg, bool? durable = true, QueueType queueType = QueueType.Classic, int? quorumGroupSize = null, CancellationToken cancellationToken = default) where T : class;

        /// <summary>
        /// 以交换机模式发送消息（泛型便捷方法）。
        /// 内部将消息对象序列化为 JSON 字节后，通过指定交换机和路由键进行路由。
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="configId">集群配置标识，对应 <see cref="Model.RabbitMQOptions.ConfigId"/></param>
        /// <param name="exchange">交换机名称</param>
        /// <param name="routingKey">路由键，交换机根据此键将消息路由到匹配的队列</param>
        /// <param name="msg">待发送的消息对象，将被 JSON 序列化</param>
        /// <param name="durable">交换机和队列是否持久化，默认为 true</param>
        /// <param name="exchangeType">
        /// 交换机类型，默认为 Topic（主题交换机）。
        /// 可选值参见 <see cref="ExchangeTypeConst"/>：Direct、Fanout、Headers、Topic
        /// </param>
        /// <param name="mandatory">消息无法路由时是否返回给发布者，默认为 false</param>
        /// <param name="cancellationToken">用于取消操作的令牌</param>
        /// <returns>操作结果</returns>
        Task<OperateResult> SendAsync<T>(string configId, string exchange, string routingKey, T msg, bool? durable = true, string exchangeType = ExchangeTypeConst.Topic, bool mandatory = false,  CancellationToken cancellationToken = default) where T : class;
    }
}
