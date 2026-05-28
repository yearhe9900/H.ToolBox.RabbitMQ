using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

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
        /// 连接通道池管理器，负责维护和分发不同集群的连接通道
        /// </summary>
        private readonly IConnectionChannelPoolManager _poolManager;

        /// <summary>
        /// 日志记录器
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// 初始化 RabbitMQ 传输实例
        /// </summary>
        /// <param name="logger">日志记录器，用于记录发送过程中的警告和错误信息</param>
        /// <param name="poolManager">连接通道池管理器，提供按集群标识获取通道的能力</param>
        public RabbitMqTransport(ILogger<RabbitMqTransport> logger, IConnectionChannelPoolManager poolManager)
        {
            _logger = logger;
            _poolManager = poolManager;
        }

        /// <summary>
        /// 发送消息到 RabbitMQ（核心方法，支持队列模式和交换机模式）。
        /// 处理流程：
        ///   1. 验证消息参数的合法性；
        ///   2. 根据 ConfigId 获取对应的连接通道池；
        ///   3. 从池中租借通道，根据模式声明队列或交换机；
        ///   4. 设置消息持久化属性并发布消息；
        ///   5. 无论成功或失败，都将通道归还到池中。
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

            IChannel? channel = null;
            try
            {
                // 从连接通道池中租借一个可用通道
                channel = await connectionChannelPool.Rent(cancellationToken);

                // 用 CachedDeclareChannel 包装，对同一个 exchange/queue 的声明操作自动跳过
                var cached = new CachedDeclareChannel(channel);

                if (hasExchangeMode)
                {
                    // 交换机模式：首次声明后缓存，后续同参数调用直接跳过
                    await cached.ExchangeDeclareAsync(message.Exchange, message.ExchangeType, message.Durable ?? _durable, cancellationToken);
                }
                else
                {
                    // 队列模式：首次声明后缓存，后续同参数调用直接跳过
                    await cached.QueueDeclareAsync(message.QueueName, message.Durable ?? _durable, cancellationToken);

                    // 队列模式下若未指定 RoutingKey，则使用队列名作为 RoutingKey
                    // 默认交换机会将 RoutingKey 与队列名完全匹配的消息路由到对应队列
                    if (string.IsNullOrEmpty(message.RoutingKey))
                    {
                        message.RoutingKey = message.QueueName;
                    }
                }

                // 设置消息属性：DeliveryMode.Persistent 表示消息会被持久化到磁盘，RabbitMQ 重启后不丢失
                var props = new BasicProperties
                {
                    DeliveryMode = DeliveryModes.Persistent,
                };

                // 发布消息
                // mandatory: false 表示消息无法路由时直接丢弃，不返回给发布者
                await cached.BasicPublishAsync(
                    exchange: message.Exchange,
                    routingKey: message.RoutingKey,
                    mandatory: false,
                    basicProperties: props,
                    body: message.Body,
                    cancellationToken: cancellationToken);

                return OperateResult.Success;
            }
            catch (OperationCanceledException)
            {
                // 取消是正常的控制流，不需要包装，直接向上传播
                throw;
            }
            catch (Exception ex)
            {
                // 通道已关闭但状态标记不一致（IsOpen 仍为 true）时，强制释放该通道以避免资源泄漏
                if (ex is AlreadyClosedException && channel?.IsOpen == true)
                {
                    _logger.LogWarning("检测到通道状态不一致：通道显示为已打开，但其底层连接已关闭。正在强制关闭该通道。");
                    await channel.DisposeAsync();
                }

                _logger.LogError(ex, "消息发布失败，队列: {QueueName}, ConfigId: {ConfigId}, 错误: {ErrorMessage}",
                    message.QueueName, message.ConfigId, ex.Message);

                var wrapperEx = new PublisherSentFailedException(ex.Message, ex);
                var errors = new OperateError
                {
                    Code = ex.HResult.ToString(),
                    Description = ex.Message
                };

                return OperateResult.Failed(wrapperEx, errors);
            }
            finally
            {
                // 无论发布成功或失败，都确保通道被归还到连接池，防止通道泄漏
                if (channel != null)
                {
                    try
                    {
                        connectionChannelPool.Return(channel);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "归还通道到池时发生异常");
                    }
                }
            }
        }

        /// <summary>
        /// 全局共享的 JSON 序列化选项，使用 UnsafeRelaxedJsonEscaping 避免对中文等非 ASCII 字符进行转义
        /// </summary>
        private static readonly JsonSerializerOptions s_jsonOptions = new JsonSerializerOptions
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        /// <summary>
        /// 以队列模式发送消息（泛型便捷方法）。
        /// 将消息对象序列化为 JSON 后，通过默认交换机路由到指定队列。
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="configId">集群配置标识，对应 <see cref="RabbitMQOptions.ConfigId"/></param>
        /// <param name="queueName">目标队列名称，同时作为路由键</param>
        /// <param name="msg">待发送的消息对象</param>
        /// <param name="durable">队列是否持久化，默认为 true</param>
        /// <returns>操作结果</returns>
        public async Task<OperateResult> SendAsync<T>(string configId, string queueName, T msg, bool? durable = true, CancellationToken cancellationToken = default) where T : class
        {
            ReadOnlyMemory<byte> body = CreateMessageBody(msg);

            var message = new TransportMessage(body)
            {
                ConfigId = configId,
                QueueName = queueName,
                Durable = durable,
            };

            return await SendAsync(message, cancellationToken);
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
        /// <returns>操作结果</returns>
        public async Task<OperateResult> SendAsync<T>(string configId, string exchange, string routingKey, T msg, bool? durable = true, string exchangeType = ExchangeTypeConst.Topic, CancellationToken cancellationToken = default) where T : class
        {
            ReadOnlyMemory<byte> body = CreateMessageBody(msg);
            var message = new TransportMessage(body)
            {
                ConfigId = configId,
                Exchange = exchange,
                RoutingKey = routingKey,
                Durable = durable,
                ExchangeType = exchangeType
            };
            return await SendAsync(message, cancellationToken);
        }

        /// <summary>
        /// 将消息对象序列化为 UTF-8 编码的 JSON 字节数组
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="msg">待序列化的消息对象</param>
        /// <returns>序列化后的字节内存块</returns>
        private static ReadOnlyMemory<byte> CreateMessageBody<T>(T msg) where T : class
        {
            string json = JsonSerializer.Serialize(msg, s_jsonOptions);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            return new ReadOnlyMemory<byte>(bytes);
        }
    }
}
