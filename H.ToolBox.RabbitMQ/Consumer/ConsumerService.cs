using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// 消费者服务实现
    /// 提供队列消费和路由（交换机）消费两种模式，通过 <see cref="IConnectionChannelPool"/> 获取通道，
    /// 使用 <see cref="AsyncEventingBasicConsumer"/> 的 <c>ReceivedAsync</c> 事件驱动方式消费消息
    /// </summary>
    public class ConsumerService : IConsumerService
    {
        /// <summary>
        /// 连接通道池管理器，用于根据 ConfigId 获取对应集群的连接通道池
        /// </summary>
        private readonly IConnectionChannelPoolManager _poolManager;

        /// <summary>
        /// 消息序列化器
        /// </summary>
        private readonly IMessageSerializer _serializer;

        private readonly ILogger<ConsumerService> _logger;

        /// <summary>
        /// 当前持有的 RabbitMQ 通道列表，用于资源释放时关闭所有通道
        /// </summary>
        private readonly List<IChannel> _channels = new();

        /// <summary>
        /// 信号量，用于控制通道列表操作的线程安全
        /// </summary>
        private readonly SemaphoreSlim _semaphore = new(1, 1);

        /// <summary>
        /// 标识是否已释放资源
        /// </summary>
        private volatile bool _disposed;

        /// <summary>
        /// 初始化消费者服务实例
        /// </summary>
        /// <param name="poolManager">连接通道池管理器</param>
        /// <param name="serializer">消息序列化器</param>
        public ConsumerService(IConnectionChannelPoolManager poolManager, IMessageSerializer serializer, ILogger<ConsumerService> logger)
        {
            _poolManager = poolManager;
            _serializer = serializer;
            _logger = logger;
        }

        /// <summary>
        /// 队列消费：监听指定队列并消费消息
        /// 通过 <see cref="IConnectionChannelPool"/> 获取通道，声明队列并注册消费者
        /// </summary>
        /// <typeparam name="T">消息体的反序列化目标类型</typeparam>
        /// <param name="configId">集群配置标识</param>
        /// <param name="queueName">要消费的队列名称</param>
        /// <param name="allwaysRunActionTask">
        /// 消息处理回调委托，参数分别为消息体(T)、消息体原始字符串(ea.Body UTF-8)、路由键(RoutingKey)，
        /// 返回 true 确认消息，false 以 requeue:false 拒绝消息使其进入死信队列
        /// </param>
        /// <param name="prefetchCount">预取消息数量（QoS），默认为 50</param>
        /// <param name="autoAck">是否自动确认消息，默认为 false</param>
        /// <param name="durable">队列是否持久化，默认为 true</param>
        /// <param name="exclusive">队列是否为独占队列，默认为 false</param>
        /// <param name="autoDelete">队列是否自动删除，默认为 false</param>
        /// <param name="consumerTag">消费者标签，默认为空</param>
        /// <param name="queueType">队列类型，默认为 Classic（经典队列）</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小，仅当 queueType 为 Quorum 时生效</param>
        /// <param name="cancellationToken">取消令牌，用于停止消费</param>
        /// <returns>表示异步消费操作的任务（此任务会持续运行直到取消）</returns>
        public async Task ListenAsync<T>(
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
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(queueName))
                throw new ArgumentException("队列名称不能为空。", nameof(queueName));

            // 使用 ConsumerService 专用连接池，支持 ConsumerDispatchConcurrency 设置
            var pool = _poolManager.GetConsumerServicePool(configId);
            var connection = await pool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            await TrackChannelAsync(channel).ConfigureAwait(false);

            // 声明死信拓扑并构建业务队列参数：与生产者共用同一份集群配置，保证队列参数一致
            var clusterOptions = _poolManager.GetOptions(configId);
            await DeadLetterTopology.EnsureAsync(
                channel, queueName, queueType, quorumGroupSize, clusterOptions, cancellationToken).ConfigureAwait(false);
            var queueArguments = DeadLetterTopology.BuildQueueArguments(
                queueName, queueType, quorumGroupSize, clusterOptions);

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: durable,
                exclusive: exclusive,
                autoDelete: autoDelete,
                arguments: queueArguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 设置 QoS
            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: prefetchCount,
                global: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 创建异步事件消费者并注册 ReceivedAsync 回调
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (sender, ea) =>
            {
                var bodyString = Encoding.UTF8.GetString(ea.Body.Span);
                T body;
                try
                {
                    body = _serializer.Deserialize<T>(ea.Body.Span);
                }
                catch (Exception ex)
                {
                    // 反序列化失败：消息格式不可恢复，拒绝并不再重入队列，避免无限重试
                    _logger.LogError(ex, "消息反序列化失败，消息将被丢弃。Queue: {Queue}", queueName);
                    if (!autoAck)
                    {
                        await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    }
                    return;
                }
                var routingKey = ea.RoutingKey ?? string.Empty;

                try
                {
                    var success = await allwaysRunActionTask(body, bodyString, routingKey).ConfigureAwait(false);
                    if (!autoAck)
                    {
                        if (success)
                        {
                            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                        }
                        else
                        {
                            // 业务处理失败：不再无限重投，直接死信
                            await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "消息处理失败，Queue: {Queue}, DeliveryTag: {DeliveryTag}", queueName, ea.DeliveryTag);
                    if (!autoAck)
                    {
                        await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    }
                }
            };

            // 启动消费
            await channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: autoAck,
                consumerTag: consumerTag ?? string.Empty,
                consumer: consumer,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 持续阻塞等待，直到取消令牌被触发
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // 正常取消，忽略异常
            }
        }

        /// <summary>
        /// 路由消费：通过交换机和路由键监听消息
        /// 通过 <see cref="IConnectionChannelPool"/> 获取通道，声明交换机、队列并绑定路由关系后注册消费者
        /// </summary>
        /// <typeparam name="T">消息体的反序列化目标类型</typeparam>
        /// <param name="configId">集群配置标识</param>
        /// <param name="exchange">交换机名称</param>
        /// <param name="exchangeType">交换机类型（direct、fanout、topic、headers）</param>
        /// <param name="routingKey">路由键</param>
        /// <param name="queueName">要绑定到交换机的队列名称</param>
        /// <param name="allwaysRunActionTask">
        /// 消息处理回调委托，参数分别为消息体(T)、消息体原始字符串(ea.Body UTF-8)、路由键(RoutingKey)，
        /// 返回 true 确认消息，false 以 requeue:false 拒绝消息使其进入死信队列
        /// </param>
        /// <param name="prefetchCount">预取消息数量（QoS），默认为 50</param>
        /// <param name="autoAck">是否自动确认消息，默认为 false</param>
        /// <param name="durable">队列和交换机是否持久化，默认为 true</param>
        /// <param name="exclusive">队列是否为独占队列，默认为 false</param>
        /// <param name="autoDelete">队列是否自动删除，默认为 false</param>
        /// <param name="consumerTag">消费者标签，默认为空</param>
        /// <param name="queueType">队列类型，默认为 Classic（经典队列）</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小，仅当 queueType 为 Quorum 时生效</param>
        /// <param name="cancellationToken">取消令牌，用于停止消费</param>
        /// <returns>表示异步消费操作的任务（此任务会持续运行直到取消）</returns>
        public async Task ListenByExchangeAsync<T>(
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
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(exchange))
                throw new ArgumentException("交换机名称不能为空。", nameof(exchange));
            if (string.IsNullOrWhiteSpace(queueName))
                throw new ArgumentException("队列名称不能为空。", nameof(queueName));

            // 使用 ConsumerService 专用连接池，支持 ConsumerDispatchConcurrency 设置
            var pool = _poolManager.GetConsumerServicePool(configId);
            var connection = await pool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            await TrackChannelAsync(channel).ConfigureAwait(false);

            // 声明交换机
            await channel.ExchangeDeclareAsync(
                exchange: exchange,
                type: exchangeType ?? "direct",
                durable: durable,
                autoDelete: autoDelete,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 声明死信拓扑并构建业务队列参数：与生产者共用同一份集群配置，保证队列参数一致
            var clusterOptions = _poolManager.GetOptions(configId);
            await DeadLetterTopology.EnsureAsync(
                channel, queueName, queueType, quorumGroupSize, clusterOptions, cancellationToken).ConfigureAwait(false);
            var queueArguments = DeadLetterTopology.BuildQueueArguments(
                queueName, queueType, quorumGroupSize, clusterOptions);

            await channel.QueueDeclareAsync(
                queue: queueName,
                durable: durable,
                exclusive: exclusive,
                autoDelete: autoDelete,
                arguments: queueArguments,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 绑定队列到交换机
            await channel.QueueBindAsync(
                queue: queueName,
                exchange: exchange,
                routingKey: routingKey ?? string.Empty,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 设置 QoS
            await channel.BasicQosAsync(
                prefetchSize: 0,
                prefetchCount: prefetchCount,
                global: false,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 创建异步事件消费者并注册 ReceivedAsync 回调
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (sender, ea) =>
            {
                var bodyString = Encoding.UTF8.GetString(ea.Body.Span);
                T body;
                try
                {
                    body = _serializer.Deserialize<T>(ea.Body.Span);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "消息反序列化失败，消息将被丢弃。Queue: {Queue}", queueName);
                    if (!autoAck)
                    {
                        await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    }
                    return;
                }
                var sourceRoutingKey = ea.RoutingKey ?? string.Empty;

                try
                {
                    var success = await allwaysRunActionTask(body, bodyString, sourceRoutingKey).ConfigureAwait(false);
                    if (!autoAck)
                    {
                        if (success)
                        {
                            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                        }
                        else
                        {
                            // 业务处理失败：不再无限重投，直接死信
                            await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "消息处理失败，Exchange: {Exchange}, Queue: {Queue}, DeliveryTag: {DeliveryTag}", exchange, queueName, ea.DeliveryTag);
                    if (!autoAck)
                    {
                        await channel.BasicRejectAsync(ea.DeliveryTag, requeue: false, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                    }
                }
            };

            // 启动消费
            await channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: autoAck,
                consumerTag: consumerTag ?? string.Empty,
                consumer: consumer,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 持续阻塞等待，直到取消令牌被触发
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // 正常取消，忽略异常
            }
        }

        /// <summary>
        /// 追踪通道实例，用于资源释放时统一关闭
        /// </summary>
        /// <param name="channel">要追踪的通道实例</param>
        private async Task TrackChannelAsync(IChannel channel)
        {
            await _semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                _channels.Add(channel);
            }
            finally
            {
                _semaphore.Release();
            }
        }

        /// <summary>
        /// 异步释放资源，关闭并清理所有已追踪的通道
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            await _semaphore.WaitAsync().ConfigureAwait(false);
            try
            {
                foreach (var channel in _channels)
                {
                    try
                    {
                        if (channel.IsOpen)
                        {
                            await channel.CloseAsync().ConfigureAwait(false);
                        }
                        await channel.DisposeAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        // 释放过程中忽略异常，确保所有通道都被尝试关闭
                    }
                }
                _channels.Clear();
            }
            finally
            {
                _semaphore.Release();
            }

            _semaphore.Dispose();
        }
    }
}