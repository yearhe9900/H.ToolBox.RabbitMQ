using H.ToolBox.RabbitMQ.Model;
using RabbitMQ.Client;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// 消费者服务接口
    /// 提供简化的消息消费 API，支持队列消费和路由（交换机）消费两种模式
    /// </summary>
    /// <remarks>
    /// 通过 <see cref="IConnectionChannelPool"/> 获取 RabbitMQ 通道，
    /// 使用 <c>AsyncEventingBasicConsumer</c> 的 <c>ReceivedAsync</c> 事件驱动方式消费消息。
    /// 回调委托 <c>Func<T, string, string, Task<bool>></c> 的参数分别为：
    /// <list type="number">
    ///   <item><description>T: 消息体内容（泛型，支持 string/byte[]/自定义类型 JSON 反序列化）</description></item>
    ///   <item><description>string: 消息体原始字符串（ea.Body 的 UTF-8 编码字符串）</description></item>
    ///   <item><description>string: 路由键（RoutingKey）</description></item>
    ///   <item><description>返回 Task<bool>: true 表示确认消息（Ack），false 表示拒绝消息（Reject）并重新入队</description></item>
    /// </list>
    /// </remarks>
    public interface IConsumerService : IAsyncDisposable
    {
        /// <summary>
        /// 队列消费：监听指定队列并消费消息
        /// 通过 <see cref="IConnectionChannelPool"/> 获取通道，声明队列并注册消费者
        /// </summary>
        /// <typeparam name="T">消息体的反序列化目标类型</typeparam>
        /// <param name="configId">集群配置标识，用于从 <see cref="Channel.IConnectionChannelPoolManager"/> 获取对应的连接通道池</param>
        /// <param name="queueName">要消费的队列名称</param>
        /// <param name="allwaysRunActionTask">
        /// 消息处理回调委托，参数分别为消息体(T)、消息体原始字符串(ea.Body UTF-8)、路由键(RoutingKey)，
        /// 返回 true 确认消息，false 拒绝消息并重新入队
        /// </param>
        /// <param name="prefetchCount">预取消息数量（QoS），控制消费者在未确认消息的情况下最多接收的消息数量，默认为 50</param>
        /// <param name="autoAck">是否自动确认消息，默认为 false（手动确认）</param>
        /// <param name="durable">队列是否持久化，默认为 true</param>
        /// <param name="exclusive">队列是否为独占队列，默认为 false</param>
        /// <param name="autoDelete">队列是否自动删除（当所有消费者断开后），默认为 false</param>
        /// <param name="consumerTag">消费者标签，用于标识消费者，默认为空（由 RabbitMQ 自动生成）</param>
        /// <param name="queueType">队列类型，默认为 Classic（经典队列）</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小，仅当 queueType 为 Quorum 时生效</param>
        /// <param name="cancellationToken">取消令牌，用于停止消费</param>
        /// <returns>表示异步消费操作的任务（此任务会持续运行直到取消）</returns>
        Task ListenAsync<T>(
            string configId,
            string queueName,
            Func<T, string, string, Task<bool>> allwaysRunActionTask,
            ushort prefetchCount = 50,
            bool autoAck = false,
            bool durable = true,
            bool exclusive = false,
            bool autoDelete = false,
            string consumerTag = "",
            QueueType queueType = QueueType.Classic,
            int? quorumGroupSize = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// 路由消费：通过交换机和路由键监听消息
        /// 通过 <see cref="IConnectionChannelPool"/> 获取通道，声明交换机、队列并绑定路由关系后注册消费者
        /// </summary>
        /// <typeparam name="T">消息体的反序列化目标类型</typeparam>
        /// <param name="configId">集群配置标识，用于从 <see cref="Channel.IConnectionChannelPoolManager"/> 获取对应的连接通道池</param>
        /// <param name="exchange">交换机名称</param>
        /// <param name="exchangeType">交换机类型（direct、fanout、topic、headers），可使用 <see cref="ExchangeTypeConst"/> 中的常量</param>
        /// <param name="routingKey">路由键，用于消息从交换机到队列的路由匹配</param>
        /// <param name="queueName">要绑定到交换机的队列名称</param>
        /// <param name="allwaysRunActionTask">
        /// 消息处理回调委托，参数分别为消息体(T)、消息体原始字符串(ea.Body UTF-8)、路由键(RoutingKey)，
        /// 返回 true 确认消息，false 拒绝消息并重新入队
        /// </param>
        /// <param name="prefetchCount">预取消息数量（QoS），控制消费者在未确认消息的情况下最多接收的消息数量，默认为 50</param>
        /// <param name="autoAck">是否自动确认消息，默认为 false（手动确认）</param>
        /// <param name="durable">队列和交换机是否持久化，默认为 true</param>
        /// <param name="exclusive">队列是否为独占队列，默认为 false</param>
        /// <param name="autoDelete">队列是否自动删除（当所有消费者断开后），默认为 false</param>
        /// <param name="consumerTag">消费者标签，用于标识消费者，默认为空（由 RabbitMQ 自动生成）</param>
        /// <param name="queueType">队列类型，默认为 Classic（经典队列）</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小，仅当 queueType 为 Quorum 时生效</param>
        /// <param name="cancellationToken">取消令牌，用于停止消费</param>
        /// <returns>表示异步消费操作的任务（此任务会持续运行直到取消）</returns>
        Task ListenByExchangeAsync<T>(
            string configId,
            string exchange,
            string exchangeType,
            string routingKey,
            string queueName,
            Func<T, string, string, Task<bool>> allwaysRunActionTask,
            ushort prefetchCount = 50,
            bool autoAck = false,
            bool durable = true,
            bool exclusive = false,
            bool autoDelete = false,
            string consumerTag = "",
            QueueType queueType = QueueType.Classic,
            int? quorumGroupSize = null,
            CancellationToken cancellationToken = default);
    }
}