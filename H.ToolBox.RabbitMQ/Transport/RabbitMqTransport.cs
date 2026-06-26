using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

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
        /// 默认持久化标志，当消息未显式指定 Durable 时使用此默认值
        /// </summary>
        private readonly bool _durable = false;

        /// <summary>
        /// 共享的消息属性实例，所有发送操作复用以减少对象分配
        /// DeliveryMode.Persistent 表示消息持久化到磁盘
        /// </summary>
        private static readonly BasicProperties s_basicProperties = new() { DeliveryMode = DeliveryModes.Persistent };

        /// <summary>
        /// 连接通道池管理器，负责维护和分发不同集群的连接通道
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
        /// 发布确认超时时间
        /// </summary>
        private readonly TimeSpan _publishConfirmsTimeout;

        /// <summary>
        /// 初始化 RabbitMQ 传输实例
        /// </summary>
        /// <param name="logger">日志记录器，用于记录发送过程中的警告和错误信息</param>
        /// <param name="poolManager">连接通道池管理器，提供按集群标识获取通道的能力</param>
        /// <param name="serializer">消息序列化器，用于将消息对象序列化为字节数组</param>
        /// <param name="options">RabbitMQ 配置选项</param>
        public RabbitMqTransport(ILogger<RabbitMqTransport> logger, IConnectionChannelPoolManager poolManager, IMessageSerializer serializer, IOptions<RabbitMQOptions> options)
        {
            _logger = logger;
            _poolManager = poolManager;
            _serializer = serializer;
            _publishConfirmsTimeout = options.Value.PublishConfirmsTimeout;
        }

        /// <summary>
        /// 发送消息到 RabbitMQ（核心方法，支持队列模式和交换机模式）。
        /// 处理流程：
        ///   1. 验证消息参数的合法性；
        ///   2. 根据 ConfigId 获取对应的连接通道池；
        ///   3. 从池中租借通道，根据模式声明队列或交换机；
        ///   4. 设置消息持久化属性并发布消息；
        ///   5. 若启用了 Publisher Confirm，等待 Broker 确认；
        ///   6. 发布失败时根据配置进行重试（指数退避）；
        ///   7. 无论成功或失败，都将通道归还到池中。
        /// </summary>
        /// <param name="message">传输消息对象，必须包含有效的路由信息和非空消息体</param>
        /// <returns>操作结果，成功返回 <see cref="OperateResult.Success"/>，失败时包含异常和错误详情</returns>
        public async Task<OperateResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default)
        {
            // 验证消息参数：至少满足队列模式（QueueName 非空）或交换机模式（Exchange + RoutingKey 非空）之一
            bool hasQueueMode = !string.IsNullOrEmpty(message.QueueName);
            bool hasExchangeMode = !string.IsNullOrEmpty(message.Exchange) && !string.IsNullOrEmpty(message.RoutingKey);

            if ((!hasQueueMode && !hasExchangeMode) || message.Body.IsEmpty)
            {
                _logger.LogDebug("[SendAsync] 参数验证失败: QueueName={QueueName}, Exchange={Exchange}, RoutingKey={RoutingKey}, BodyEmpty={BodyEmpty}",
                    message.QueueName, message.Exchange, message.RoutingKey, message.Body.IsEmpty);

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

            // 根据 ConfigId 获取对应集群的连接通道池
            var connectionChannelPool = _poolManager.GetPool(message.ConfigId);
            _logger.LogDebug("[SendAsync] 获取连接通道池: ConfigId={ConfigId}, IsPublishConfirms={IsPublishConfirms}",
                message.ConfigId, connectionChannelPool.IsPublishConfirms);

            var maxRetries = message.MaxRetries ?? 0;
            var retryCount = 0;

            while (true)
            {
                IChannel? channel = null;
                try
                {
                    // 从连接通道池中租借一个可用通道
                    channel = await connectionChannelPool.Rent(cancellationToken).ConfigureAwait(false);
                    _logger.LogDebug("[SendAsync] 租借通道成功: ChannelNumber={ChannelNumber}, IsOpen={IsOpen}",
                        channel.ChannelNumber, channel.IsOpen);

                    // 用 CachedDeclareChannel 包装，对同一个 exchange/queue 的声明操作自动跳过
                    var cached = new CachedDeclareChannel(channel);

                    if (hasExchangeMode)
                    {
                        // 交换机模式：首次声明后缓存，后续同参数调用直接跳过
                        await cached.ExchangeDeclareAsync(message.Exchange, message.ExchangeType, message.Durable ?? _durable, message.AutoDelete, cancellationToken).ConfigureAwait(false);
                        _logger.LogDebug("[SendAsync] 交换机声明完成: Exchange={Exchange}, Type={ExchangeType}, Durable={Durable}",
                            message.Exchange, message.ExchangeType, message.Durable ?? _durable);
                    }
                    else
                    {
                        // 队列模式：根据队列类型构建声明参数
                        var queueArguments = QueueArgumentsHelper.BuildArguments(message.QueueType, message.QuorumGroupSize);

                        // 队列模式：首次声明后缓存，后续同参数调用直接跳过
                        await cached.QueueDeclareAsync(message.QueueName, message.Durable ?? _durable, message.Exclusive, message.AutoDelete, message.QueueType, queueArguments, cancellationToken).ConfigureAwait(false);
                        _logger.LogDebug("[SendAsync] 队列声明完成: QueueName={QueueName}, Durable={Durable}, QueueType={QueueType}",
                            message.QueueName, message.Durable ?? _durable, message.QueueType);

                        // 队列模式下若未指定 RoutingKey，则使用队列名作为 RoutingKey
                        // 默认交换机会将 RoutingKey 与队列名完全匹配的消息路由到对应队列
                        if (string.IsNullOrEmpty(message.RoutingKey))
                        {
                            message.RoutingKey = message.QueueName;
                        }
                    }

                    var props = s_basicProperties;

                    // 发布消息
                    // mandatory: true 时，消息无法路由则退回发布者；false 时直接丢弃
                    PublishConfirmsTracker? tracker = null;
                    try
                    {
                        if (connectionChannelPool.IsPublishConfirms)
                        {
                            // 7.x 移除了 WaitForConfirmsAsync，改用事件 + 并发字典 + 轮询模拟
                            // 1. 获取当前 deliveryTag 并追踪
                            // 2. 注册 BasicAcks/BasicNacks 事件接收确认
                            // 3. 发布消息
                            // 4. 轮询等待确认
                            tracker = new PublishConfirmsTracker(_logger, _publishConfirmsTimeout);
                            tracker.RegisterChannel(cached.Channel);

                            var deliveryTag = await cached.Channel.GetNextPublishSequenceNumberAsync(cancellationToken).ConfigureAwait(false);
                            tracker.Track(deliveryTag);
                            _logger.LogDebug("[SendAsync] PublishConfirms 追踪已设置: DeliveryTag={DeliveryTag}, Timeout={Timeout}s",
                                deliveryTag, _publishConfirmsTimeout.TotalSeconds);
                        }

                        _logger.LogDebug("[SendAsync] 开始发布消息: Exchange={Exchange}, RoutingKey={RoutingKey}, Mandatory={Mandatory}, BodySize={BodySize}",
                            message.Exchange, message.RoutingKey, message.Mandatory, message.Body.Length);

                        await cached.BasicPublishAsync(
                            exchange: message.Exchange,
                            routingKey: message.RoutingKey,
                            mandatory: message.Mandatory,
                            basicProperties: props,
                            body: message.Body,
                            cancellationToken: cancellationToken
                            ).ConfigureAwait(false);

                        _logger.LogDebug("[SendAsync] 消息发布完成，开始等待确认");

                        if (tracker != null)
                        {
                            await tracker.WaitForAllConfirmsAsync(cancellationToken).ConfigureAwait(false);
                            _logger.LogDebug("[SendAsync] Broker 确认完成");
                        }

                        _logger.LogDebug("[SendAsync] 发送成功: ConfigId={ConfigId}, QueueName={QueueName}, Exchange={Exchange}, RoutingKey={RoutingKey}",
                            message.ConfigId, message.QueueName, message.Exchange, message.RoutingKey);

                        return OperateResult.Success;
                    }
                    finally
                    {
                        tracker?.Dispose();
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogDebug("[SendAsync] 操作被取消: ConfigId={ConfigId}", message.ConfigId);
                    // 取消是正常的控制流，不需要包装，直接向上传播
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "[SendAsync] 发布异常: ConfigId={ConfigId}, RetryCount={RetryCount}/{MaxRetries}, Exception={ExceptionType}",
                        message.ConfigId, retryCount, maxRetries, ex.GetType().Name);

                    // 通道已关闭但状态标记不一致（IsOpen 仍为 true）时，强制释放该通道以避免资源泄漏
                    if (ex is AlreadyClosedException && channel?.IsOpen == true)
                    {
                        _logger.LogWarning("检测到通道状态不一致：通道显示为已打开，但其底层连接已关闭。正在强制关闭该通道。");
                        await channel.DisposeAsync().ConfigureAwait(false);
                    }

                    // 归还当前通道（异常通道不会被复用，Return 内部会 Dispose）
                    if (channel != null)
                    {
                        try
                        {
                            connectionChannelPool.Return(channel);
                            channel = null; // 避免 finally 中重复归还
                        }
                        catch (Exception returnEx)
                        {
                            _logger.LogWarning(returnEx, "归还异常通道到池时发生异常");
                        }
                    }

                    // 判断是否需要重试
                    if (retryCount < maxRetries && !cancellationToken.IsCancellationRequested)
                    {
                        retryCount++;
                        var delay = TimeSpan.FromMilliseconds(Math.Pow(2, retryCount) * 100);
                        _logger.LogWarning(ex, "消息发布失败，正在进行第 {RetryCount}/{MaxRetries} 次重试，等待 {Delay}ms",
                            retryCount, maxRetries, delay.TotalMilliseconds);

                        try
                        {
                            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // 延迟期间被取消，包装最后一次异常返回
                            var wrapperEx = new PublisherSentFailedException(ex.Message, ex);
                            return OperateResult.Failed(wrapperEx, new OperateError
                            {
                                Code = ex.HResult.ToString(),
                                Description = ex.Message
                            });
                        }

                        continue; // 重新循环，租借新通道重试
                    }

                    _logger.LogError(ex, "消息发布失败（已重试 {RetryCount} 次），队列: {QueueName}, ConfigId: {ConfigId}, 错误: {ErrorMessage}",
                        retryCount, message.QueueName, message.ConfigId, ex.Message);

                    var finalWrapperEx = new PublisherSentFailedException(ex.Message, ex);
                    var errors = new OperateError
                    {
                        Code = ex.HResult.ToString(),
                        Description = ex.Message
                    };

                    return OperateResult.Failed(finalWrapperEx, errors);
                }
                finally
                {
                    // 无论发布成功或失败，都确保通道被归还到连接池，防止通道泄漏
                    if (channel != null)
                    {
                        try
                        {
                            connectionChannelPool.Return(channel);
                            _logger.LogDebug("[SendAsync] 通道已归还: ChannelNumber={ChannelNumber}", channel.ChannelNumber);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "归还通道到池时发生异常");
                        }
                    }
                }
            }
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
        /// <param name="maxRetries">发布失败时的最大重试次数，默认为 null（不重试）</param>
        /// <returns>操作结果</returns>
        public async Task<OperateResult> SendAsync<T>(string configId, string exchange, string routingKey, T msg, bool? durable = true, string exchangeType = ExchangeTypeConst.Topic, bool mandatory = false, int? maxRetries = null, CancellationToken cancellationToken = default) where T : class
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
                MaxRetries = maxRetries
            };
            return await SendAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }
}
