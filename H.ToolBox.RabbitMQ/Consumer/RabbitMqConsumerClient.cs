using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Transport;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ 消费者客户端
    /// 负责与 RabbitMQ 服务器建立连接、声明队列/交换机、设置 QoS 参数，
    /// 并通过异步事件驱动方式持续监听和消费消息
    /// </summary>
    /// <remarks>
    /// 核心职责：
    /// <list type="bullet">
    ///   <item><description>通过 <see cref="IConnectionChannelPool"/> 获取 RabbitMQ 连接并创建通道</description></item>
    ///   <item><description>根据配置声明队列（队列模式）或交换机（交换机模式）</description></item>
    ///   <item><description>设置 QoS（预取消息数量）以控制消费速率</description></item>
    ///   <item><description>注册 <see cref="RabbitMqBasicConsumer"/> 并开始持续监听消息</description></item>
    ///   <item><description>提供消息确认（CommitAsync）和拒绝（RejectAsync）功能</description></item>
    /// </list>
    /// </remarks>
    public class RabbitMqConsumerClient : IRabbitMqConsumerClient
    {
        /// <summary>
        /// 信号量，用于控制并发访问，确保通道创建的线程安全
        /// 初始化和最大并发数都设置为1，实现互斥访问
        /// </summary>
        private readonly SemaphoreSlim _semaphore = new(1, 1);

        /// <summary>
        /// 连接通道池，用于获取和管理 RabbitMQ 连接
        /// </summary>
        private readonly IConnectionChannelPool _connectionChannelPool;

        /// <summary>
        /// 日志记录器，用于记录连接重试等信息
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// 当前集群的配置，用于构建与生产者一致的死信拓扑参数
        /// </summary>
        private readonly RabbitMQOptions _options;

        /// <summary>
        /// 当前使用的 RabbitMQ 通道实例
        /// </summary>
        private IChannel? _channel;

        /// <summary>
        /// RabbitMQ 异步基础消费者实例，用于接收消息并进行确认/拒绝操作
        /// </summary>
        private RabbitMqBasicConsumer? _consumer = null;

        /// <summary>
        /// 初始化 RabbitMQ 消费者客户端实例
        /// </summary>
        /// <param name="connectionChannelPool">连接通道池，用于获取 RabbitMQ 连接和创建通道</param>
        /// <param name="logger">日志记录器</param>
        /// <param name="options">集群配置；为 null 时使用默认配置（仍会启用默认死信拓扑）</param>
        public RabbitMqConsumerClient(IConnectionChannelPool connectionChannelPool, ILogger logger, RabbitMQOptions? options = null)
        {
            _connectionChannelPool = connectionChannelPool;
            _logger = logger;
            _options = options ?? new RabbitMQOptions();
        }

        /// <summary>
        /// 获取或设置日志回调委托
        /// 当消费者状态发生变化（注册、取消、关闭等）时由 <see cref="RabbitMqBasicConsumer"/> 触发
        /// </summary>
        public Action<LogMessageEventArgs>? OnLogCallback { get; set; }

        /// <summary>
        /// 获取或设置消息接收回调委托
        /// 当消费者收到消息时由 <see cref="RabbitMqBasicConsumer"/> 触发，
        /// 参数分别为：消息对象、投递标签（deliveryTag）、消费者标签（consumerTag）
        /// </summary>
        public Func<TransportMessage, object?, string, Task>? OnMessageCallback { get; set; }

        /// <summary>
        /// 确认消息已成功处理
        /// 将确认操作委托给内部的 <see cref="RabbitMqBasicConsumer"/> 执行
        /// </summary>
        /// <param name="sender">消息投递标签（deliveryTag）</param>
        /// <returns>表示异步确认操作的任务</returns>
        public async Task CommitAsync(object? sender)
        {
            if (_consumer is null) throw new InvalidOperationException("消费者未初始化或已释放。");
            if (sender is null) throw new ArgumentNullException(nameof(sender));
            await _consumer.BasicAck((ulong)sender).ConfigureAwait(false);
        }

        /// <summary>
        /// 拒绝消息。默认 requeue:false，消息进入死信队列，避免毒消息无限循环重投。
        /// 将拒绝操作委托给内部的 <see cref="RabbitMqBasicConsumer"/> 执行
        /// </summary>
        /// <param name="sender">消息投递标签（deliveryTag）</param>
        /// <param name="requeue">是否重新入队；默认 false</param>
        /// <returns>表示异步拒绝操作的任务</returns>
        public async Task RejectAsync(object? sender, bool requeue = false)
        {
            if (_consumer is null) throw new InvalidOperationException("消费者未初始化或已释放。");
            if (sender is null) throw new ArgumentNullException(nameof(sender));
            await _consumer.BasicReject((ulong)sender, requeue).ConfigureAwait(false);
        }

        /// <summary>
        /// 启动消息监听，持续消费指定队列中的消息
        /// 该方法会阻塞当前任务直到取消令牌被触发
        /// </summary>
        /// <param name="timeout">监听超时时间，控制轮询等待间隔</param>
        /// <param name="cancellationToken">取消令牌，用于停止监听</param>
        /// <param name="options">监听配置选项，包含队列名、交换机、QoS 等参数</param>
        /// <returns>表示异步监听操作的任务</returns>
        /// <exception cref="ArgumentException">当队列模式下未指定 QueueName 时抛出</exception>
        /// <remarks>
        /// 监听流程：
        /// <list type="number">
        ///   <item><description>验证队列名称不为空</description></item>
        ///   <item><description>建立连接并创建通道</description></item>
        ///   <item><description>根据配置判断队列模式或交换机模式</description></item>
        ///   <item><description>声明队列或交换机</description></item>
        ///   <item><description>设置 QoS 预取数量</description></item>
        ///   <item><description>注册消费者并开始监听</description></item>
        ///   <item><description>进入阻塞等待直到取消</description></item>
        /// </list>
        /// </remarks>
        public async Task ListeningAsync(TimeSpan timeout, CancellationToken cancellationToken, ListeningOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.QueueName))
            {
                throw new ArgumentException("队列模式下必须指定 QueueName，不能为空。");
            }

            // 建立连接并创建通道
            await ConnectAsync().ConfigureAwait(false);

            // 确定预取消息数量，最小值为 1
            ushort prefetch = options.PrefetchCount > 0 ? options.PrefetchCount : (ushort)1;

            // 判断是否为 Exchange 模式（配置了 Exchange 则为 Exchange 模式，否则为队列模式）
            bool isExchangeMode = !string.IsNullOrWhiteSpace(options.Exchange);

            var queueName = options.QueueName;
            var consumerConcurrent = options.ConsumerConcurrent ;

            // 先声明死信拓扑，再用同一份 RabbitMQOptions 构建业务队列声明参数。
            // 生产者也会声明同一队列，参数必须完全一致，否则会触发 PRECONDITION_FAILED。
            await DeadLetterTopology.EnsureAsync(
                _channel!, options.QueueName!, options.QueueType, options.QuorumGroupSize,
                _options, cancellationToken).ConfigureAwait(false);

            var queueArguments = DeadLetterTopology.BuildQueueArguments(
                options.QueueName!, options.QueueType, options.QuorumGroupSize, _options);

            if (isExchangeMode)
            {
                // Exchange 模式：声明交换机、声明队列、绑定队列到交换机
                var exchange = options.Exchange ?? string.Empty;
                var exchangeType = options.ExchangeType ?? "direct";
                var routingKey = options.RoutingKey!;

                await _channel!.ExchangeDeclareAsync(exchange, exchangeType, options.Durable, options.AutoDelete, null).ConfigureAwait(false);

                var queueDeclareOk = await _channel!.QueueDeclareAsync(
                    options.QueueName ?? string.Empty,
                    options.Durable,
                    options.Exclusive,
                    options.AutoDelete,
                    queueArguments).ConfigureAwait(false);
                queueName = queueDeclareOk.QueueName;

                await _channel!.QueueBindAsync(queueName, exchange, routingKey, arguments: null).ConfigureAwait(false);
            }
            else
            {
                // 队列模式：声明队列，捕获返回值以获取实际队列名称
                // （QueueName 为空时 RabbitMQ 会生成匿名队列名）
                var queueDeclareOk = await _channel!.QueueDeclareAsync(
                    options.QueueName ?? string.Empty,
                    options.Durable,
                    options.Exclusive,
                    options.AutoDelete,
                    queueArguments).ConfigureAwait(false);
                queueName = queueDeclareOk.QueueName;
            }

            // 设置 QoS：控制消费者在未确认消息的情况下最多接收的消息数量
            await _channel!.BasicQosAsync(prefetchSize: 0, prefetchCount: prefetch, global: false, cancellationToken).ConfigureAwait(false);

            // 创建异步基础消费者，传入消息回调和日志回调
            _consumer = new RabbitMqBasicConsumer(_channel, consumerConcurrent, OnMessageCallback!, OnLogCallback!, options.AutoAck);

            // 注册消费者并开始消费消息
            await _channel!.BasicConsumeAsync(queueName, options.AutoAck, options.ConsumerName ?? string.Empty, _consumer, cancellationToken).ConfigureAwait(false);

            // 持续阻塞等待，直到取消令牌被触发
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(timeout, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 瞬时故障重试次数，针对连接/通道创建过程中的可恢复错误
        /// </summary>
        private const int MaxConnectRetries = 3;

        /// <summary>
        /// 异步连接到 RabbitMQ 服务器并创建通道
        /// 如果通道不存在或已关闭，则创建新的通道
        /// 使用信号量确保通道创建过程的线程安全，并支持瞬时故障重试
        /// </summary>
        /// <returns>表示异步连接操作的任务</returns>
        public async Task ConnectAsync()
        {
            // 等待获取信号量，确保同一时间只有一个线程可以执行通道创建逻辑
            await _semaphore.WaitAsync().ConfigureAwait(false);

            try
            {
                // 检查通道是否需要重新创建：
                // 1. 通道为空（首次创建）
                // 2. 通道已关闭（需要重建）
                if (_channel == null || _channel.IsClosed)
                {
                    Exception? lastException = null;

                    for (var attempt = 0; attempt <= MaxConnectRetries; attempt++)
                    {
                        try
                        {
                            // 从连接池中异步获取一个可用的 RabbitMQ 连接
                            var connection = await _connectionChannelPool.GetConnectionAsync().ConfigureAwait(false);
                            // 通过连接创建新的通道
                            _channel = await connection.CreateChannelAsync().ConfigureAwait(false);
                            return; // 成功，直接返回
                        }
                        catch (OperationCanceledException)
                        {
                            // 调用方主动取消，直接抛出
                            throw;
                        }
                        catch (Exception ex)
                        {
                            lastException = ex;

                            // 仅对瞬时错误重试
                            if (attempt >= MaxConnectRetries || !IsTransientAmqpException(ex))
                            {
                                break;
                            }

                            // 指数退避后重试（100ms、200ms、400ms …，上限 2s）
                            var backoffMs = (int)Math.Min(2000, Math.Pow(2, attempt) * 100);
                            _logger.LogWarning(ex, "消费者通道创建遇到瞬时故障，{Delay}ms 后进行第 {Next} 次重试（共 {Max} 次）。", backoffMs, attempt + 1, MaxConnectRetries);
                            await Task.Delay(backoffMs).ConfigureAwait(false);
                        }
                    }

                    // 重试耗尽，抛出最后一次异常
                    throw lastException ?? new InvalidOperationException("消费者通道创建失败，原因未知。");
                }
            }
            finally
            {
                // 释放信号量，允许其他线程访问
                _semaphore.Release();
            }
        }

        /// <summary>
        /// 判断异常是否为可恢复的瞬时 AMQP 错误
        /// </summary>
        private static bool IsTransientAmqpException(Exception ex)
        {
            return ex is AlreadyClosedException
                || ex is OperationInterruptedException
                || ex is BrokerUnreachableException
                || ex is IOException
                || ex is SocketException
                || ex is TimeoutException;
        }

        /// <summary>
        /// 异步释放资源，关闭并清理通道和信号量
        /// </summary>
        /// <returns>值任务</returns>
        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_channel != null)
                {
                    await _channel.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // 通道释放失败不应阻止信号量清理
            }
            _semaphore.Dispose();
        }
    }
}