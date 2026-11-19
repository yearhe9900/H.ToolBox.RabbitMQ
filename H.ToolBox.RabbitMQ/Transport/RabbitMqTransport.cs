using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.IO;
using System.Net.Sockets;

namespace H.ToolBox.RabbitMQ.Transport
{
    /// <summary>
    /// RabbitMQ 消息传输实现类，负责将消息发布到 RabbitMQ。
    /// 支持多集群（通过 <see cref="IConnectionChannelPoolManager"/> 管理不同集群的连接通道池），
    /// 支持队列模式和交换机模式两种发送方式。
    /// </summary>
    public class RabbitMqTransport : IRabbitMqTransport
    {
        /// <summary>
        /// 默认持久化标志，当消息未显式指定 Durable 时使用此默认值。
        /// 必须与消费侧默认 Durable=true 保持一致，否则同一队列的两次声明会因参数不一致
        /// 触发 RabbitMQ PRECONDITION_FAILED 并关闭通道。
        /// </summary>
        private readonly bool _durable = true;

        /// <summary>
        /// 共享的消息属性实例，所有发送操作复用以减少对象分配
        /// DeliveryMode.Persistent 表示消息持久化到磁盘
        /// </summary>
        private static readonly BasicProperties s_basicProperties = new() { DeliveryMode = DeliveryModes.Persistent };

        /// <summary>
        /// 连接通道池管理器，负责维护和分发不同集群的连接通道池
        /// </summary>
        private readonly IConnectionChannelPoolManager _poolManager;

        /// <summary>
        /// 日志记录器
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// 消息序列化器
        /// </summary>
        private readonly IMessageSerializer _serializer;

        /// <summary>
        /// 初始化 RabbitMQ 传输实例
        /// </summary>
        /// <param name="logger">日志记录器，用于记录发送过程中的警告和错误信息</param>
        /// <param name="poolManager">连接通道池管理器，提供按集群标识获取通道的能力</param>
        /// <param name="serializer">消息序列化器，用于将消息对象序列化为字节数组</param>
        /// <param name="options">RabbitMQ 配置选项（保留以兼容既有构造签名）</param>
        public RabbitMqTransport(ILogger<RabbitMqTransport> logger, IConnectionChannelPoolManager poolManager, IMessageSerializer serializer, IOptions<RabbitMQOptions> options)
        {
            _logger = logger;
            _poolManager = poolManager;
            _serializer = serializer;
        }

        /// <summary>
        /// 发送消息到 RabbitMQ（核心方法，支持队列模式和交换机模式）。
        /// 处理流程：
        ///   1. 验证消息参数的合法性；
        ///   2. 根据 ConfigId 获取对应的连接通道池；
        ///   3. 从池中租借通道，根据模式声明队列或交换机；
        ///   4. 设置消息持久化属性并发布消息；
        ///   5. 开启 publisher confirmation tracking 时，await 发布即等待 broker 确认；
        ///      broker nack 或 mandatory 消息被 basic.return 时客户端抛 PublishException；
        ///   6. nack 按瞬时故障重试，basic.return 属于拓扑错误不重试；
        ///   7. 无论成功或失败，都将通道归还到池中。
        /// </summary>
        /// <param name="message">传输消息对象，必须包含有效的路由信息和非空消息体</param>
        /// <param name="cancellationToken">用于取消操作的令牌</param>
        /// <returns>操作结果，成功返回 <see cref="OperateResult.Success"/>，失败时包含异常和错误详情</returns>
        public async Task<OperateResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default)
        {
            // 验证消息参数：至少满足队列模式（QueueName 非空）或交换机模式（Exchange + RoutingKey 非空）之一
            bool hasQueueMode = !string.IsNullOrEmpty(message.QueueName);
            bool hasExchangeMode = !string.IsNullOrEmpty(message.Exchange) && !string.IsNullOrEmpty(message.RoutingKey);

            if ((!hasQueueMode && !hasExchangeMode) || message.Body.IsEmpty)
            {
                // 参数无效时返回失败结果，使用 Windows HRESULT 错误码 0x80070057（E_INVALIDARG）
                uint hexValue = 0x80070057;
                int intValue = unchecked((int)hexValue);
                var ex = new Exception("未指定队列名或交换机+路由键，或消息内容为空")
                {
                    HResult = intValue
                };

                return OperateResult.Failed(ex, new OperateError
                {
                    Code = ex.HResult.ToString(),
                    Description = ex.Message
                });
            }

            cancellationToken.ThrowIfCancellationRequested();

            // 根据 ConfigId 获取对应集群的生产者专用连接通道池
            // 使用独立连接，避免生产者高速发送时的连接级背压影响消费者 ACK
            var connectionChannelPool = _poolManager.GetProducerPool(message.ConfigId);

            // 读取该集群配置：瞬时故障重试次数与死信拓扑参数。
            // 死信参数必须与消费侧使用同一个 RabbitMQOptions 实例，否则同名队列声明会冲突。
            var clusterOptions = _poolManager.GetOptions(message.ConfigId);
            var maxRetries = Math.Max(0, clusterOptions.MaxRetries);

            Exception? lastException = null;

            for (var attempt = 0; attempt <= maxRetries; attempt++)
            {
                IChannel? channel = null;
                try
                {
                    // 从连接通道池中租借一个可用通道
                    channel = await connectionChannelPool.Rent(cancellationToken).ConfigureAwait(false);

                    // 用 CachedDeclareChannel 包装，对同一个 exchange/queue 的声明操作自动跳过
                    var cached = new CachedDeclareChannel(channel);

                    if (hasExchangeMode)
                    {
                        // 交换机模式：首次声明后缓存，后续同参数调用直接跳过
                        await cached.ExchangeDeclareAsync(message.Exchange, message.ExchangeType, message.Durable ?? _durable, message.AutoDelete, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        // 队列模式：先声明死信拓扑（DLX / DLQ / 绑定），再构建业务队列声明参数。
                        // 生产者与消费者必须使用完全一致的 arguments，否则 RabbitMQ 会返回 PRECONDITION_FAILED。
                        await DeadLetterTopology.EnsureAsync(
                            cached, message.QueueName, message.QueueType, message.QuorumGroupSize,
                            clusterOptions, cancellationToken).ConfigureAwait(false);

                        var queueArguments = DeadLetterTopology.BuildQueueArguments(
                            message.QueueName, message.QueueType, message.QuorumGroupSize, clusterOptions);

                        // 队列模式：首次声明后缓存，后续同参数调用直接跳过
                        await cached.QueueDeclareAsync(message.QueueName, message.Durable ?? _durable, message.Exclusive, message.AutoDelete, message.QueueType, queueArguments, cancellationToken).ConfigureAwait(false);

                        // 队列模式下若未指定 RoutingKey，则使用队列名作为 RoutingKey
                        // 默认交换机会将 RoutingKey 与队列名完全匹配的消息路由到对应队列
                        if (string.IsNullOrEmpty(message.RoutingKey))
                        {
                            message.RoutingKey = message.QueueName;
                        }
                    }

                    var props = s_basicProperties;

                    // RabbitMQ.Client 7.x：通道开启 publisherConfirmationTrackingEnabled 后，
                    // 此 await 会等待 broker 的 ack；nack / basic.return 时抛 PublishException。
                    // 若未开启 confirms，则仅写入 socket 后立即返回，保持旧行为。
                    await channel.BasicPublishAsync(
                        exchange: message.Exchange,
                        routingKey: message.RoutingKey,
                        mandatory: message.Mandatory,
                        basicProperties: props,
                        body: message.Body,
                        cancellationToken: cancellationToken
                        ).ConfigureAwait(false);

                    // 发布与确认均已完成，通道可安全归还给其他任务复用
                    connectionChannelPool.Return(channel);
                    channel = null; // 避免下方异常处理中重复归还

                    return OperateResult.Success;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // 只有当调用方主动取消时才抛出，其他情况（如连接超时）应作为失败处理
                    throw;
                }
                catch (PublishException pex)
                {
                    // confirmation tracking 开启时，客户端在 nack / basic.return 时抛出此异常。
                    // IsReturn=true 表示消息未路由（拓扑配置错误），重试无意义；否则是 broker nack，按瞬时故障重试。
                    lastException = pex.IsReturn
                        ? new PublishReturnedException(
                            $"mandatory 消息无法路由到任何队列（basic.return）。Exchange: {message.Exchange}, RoutingKey: {message.RoutingKey}", pex)
                        : new PublishNackedException("Broker 明确拒绝（basic.nack）了本次发布，消息未被接受。", pex);

                    // 确认失败不代表通道损坏，仍需归还，否则池会泄漏许可
                    if (channel != null)
                    {
                        try { connectionChannelPool.Return(channel); } catch { /* 忽略 */ }
                        channel = null;
                    }

                    if (pex.IsReturn || attempt >= maxRetries)
                    {
                        _logger.LogError(lastException, "发布确认失败且不可重试，队列: {QueueName}, ConfigId: {ConfigId}",
                            message.QueueName, message.ConfigId);
                        break;
                    }

                    var backoffMs = (int)Math.Min(2000, Math.Pow(2, attempt) * 100);
                    _logger.LogWarning(lastException, "发布确认失败，{Delay}ms 后进行第 {Next} 次重试（共 {Max} 次）。",
                        backoffMs, attempt + 1, maxRetries);
                    await Task.Delay(backoffMs, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    lastException = ex;

                    // 归还/释放当前通道：异常通道不会被复用，Return 内部会对已关闭通道做 Dispose
                    if (channel != null)
                    {
                        // 通道状态不一致（IsOpen 仍为 true 但其底层连接已关闭）时，先强制关闭再归还
                        if (ex is AlreadyClosedException && channel.IsOpen)
                        {
                            _logger.LogWarning("检测到通道状态不一致：通道显示为已打开，但其底层连接已关闭。正在强制关闭该通道。");
                            try { await channel.DisposeAsync().ConfigureAwait(false); } catch { /* 忽略 */ }
                        }

                        try { connectionChannelPool.Return(channel); } catch { /* 忽略 */ }
                        channel = null; // 避免下方异常处理中重复归还
                    }

                    // 仅对瞬时 AMQP 错误且在重试次数内才重试，避免无意义的无效重试
                    if (attempt >= maxRetries || !IsTransientAmqpException(ex))
                    {
                        break;
                    }

                    // 指数退避后重试（100ms、200ms、400ms …，上限 2s）
                    var backoffMs = (int)Math.Min(2000, Math.Pow(2, attempt) * 100);
                    _logger.LogWarning(ex, "消息发布遇到瞬时故障，{Delay}ms 后进行第 {Next} 次重试（共 {Max} 次）。", backoffMs, attempt + 1, maxRetries);
                    await Task.Delay(backoffMs, cancellationToken).ConfigureAwait(false);
                }
            }

            _logger.LogError(lastException, "消息发布失败（已达重试上限或不可恢复），队列: {QueueName}, ConfigId: {ConfigId}", message.QueueName, message.ConfigId);

            var finalException = lastException ?? new InvalidOperationException("消息发布失败，原因未知。");
            var finalWrapperEx = new PublisherSentFailedException(finalException.Message, finalException);
            var errors = new OperateError
            {
                Code = finalException.HResult.ToString(),
                Description = finalException.Message
            };

            return OperateResult.Failed(finalWrapperEx, errors);
        }

        /// <summary>
        /// 判断异常是否为可恢复的瞬时 AMQP 错误。
        /// 连接/通道在途被关闭、AMQP 操作被中断、Broker 不可达、底层 IO/网络异常、超时，
        /// 以及 Broker 明确 nack（通常是瞬时内部错误）均属于可重试范畴；
        /// 参数校验失败、消息格式错误、mandatory 消息被退回等不可恢复错误应直接失败。
        /// </summary>
        private static bool IsTransientAmqpException(Exception ex)
        {
            return ex is AlreadyClosedException
                || ex is OperationInterruptedException
                || ex is BrokerUnreachableException
                || ex is IOException
                || ex is SocketException
                || ex is TimeoutException
                || ex is PublishNackedException;
        }

        /// <summary>
        /// 以队列模式发送消息（泛型便捷方法）。
        /// 将消息对象序列化为 JSON 后，通过默认交换机路由到指定队列。
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="configId">集群配置标识，对应 <see cref="RabbitMQOptions.ConfigId"/></param>
        /// <param name="queueName">目标队列名称，同时作为路由键</param>
        /// <param name="msg">待发送的消息对象</param>
        /// <param name="durable">队列是否持久化，默认为 true</param>
        /// <param name="queueType">队列类型，默认为 Classic（经典队列）</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小，仅当 queueType 为 Quorum 时生效</param>
        /// <param name="cancellationToken">用于取消操作的令牌</param>
        /// <returns>操作结果</returns>
        public async Task<OperateResult> SendAsync<T>(string configId, string queueName, T msg, bool? durable = true, QueueType queueType = QueueType.Classic, int? quorumGroupSize = null, CancellationToken cancellationToken = default) where T : class
        {
            ReadOnlyMemory<byte> body = _serializer.Serialize(msg);

            var message = new TransportMessage(body)
            {
                ConfigId = configId,
                QueueName = queueName,
                Durable = durable,
                QueueType = queueType,
                QuorumGroupSize = quorumGroupSize,
            };

            return await SendAsync(message, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// 以交换机模式发送消息（泛型便捷方法）。
        /// 将消息对象序列化为 JSON 后，通过指定交换机和路由键进行路由。
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="configId">集群配置标识，对应 <see cref="RabbitMQOptions.ConfigId"/></param>
        /// <param name="exchange">交换机名称</param>
        /// <param name="routingKey">路由键</param>
        /// <param name="msg">待发送的消息对象</param>
        /// <param name="durable">交换机和队列是否持久化，默认为 true</param>
        /// <param name="exchangeType">交换机类型，默认为 Topic（主题交换机），可选值参见 <see cref="ExchangeTypeConst"/></param>
        /// <param name="mandatory">消息无法路由时是否返回给发布者，默认为 false</param>
        /// <param name="cancellationToken">用于取消操作的令牌</param>
        /// <returns>操作结果</returns>
        public async Task<OperateResult> SendAsync<T>(string configId, string exchange, string routingKey, T msg, bool? durable = true, string exchangeType = ExchangeTypeConst.Topic, bool mandatory = false, CancellationToken cancellationToken = default) where T : class
        {
            ReadOnlyMemory<byte> body = _serializer.Serialize(msg);
            var message = new TransportMessage(body)
            {
                ConfigId = configId,
                Exchange = exchange,
                RoutingKey = routingKey,
                Durable = durable,
                ExchangeType = exchangeType,
                Mandatory = mandatory,
            };
            return await SendAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }
}
