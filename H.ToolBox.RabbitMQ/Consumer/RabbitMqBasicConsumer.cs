using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Transport;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ 异步基础消费者
    /// 继承自 <see cref="AsyncDefaultBasicConsumer"/>，提供消息接收、确认（Ack）和拒绝（Reject）功能
    /// </summary>
    /// <remarks>
    /// 该类是消息消费的底层组件，负责：
    /// <list type="bullet">
    ///   <item><description>接收 RabbitMQ 服务器推送的消息</description></item>
    ///   <item><description>将消息体解码并封装为 <see cref="TransportMessage"/> 对象</description></item>
    ///   <item><description>通过回调委托将消息传递给上层业务逻辑处理</description></item>
    ///   <item><description>支持并发控制（通过 Task.Run 实现并发处理，并发度由 BasicQos(prefetchCount) 控制）</description></item>
    ///   <item><description>提供手动确认（Ack）和拒绝（Reject）机制</description></item>
    ///   <item><description>监听消费者生命周期事件（注册、取消、关闭等）并通知日志回调</description></item>
    /// </list>
    /// </remarks>
    public class RabbitMqBasicConsumer : AsyncDefaultBasicConsumer
    {
        /// <summary>
        /// 消息接收回调委托
        /// 当消费者收到消息时被调用，参数分别为：消息对象、投递标签、消费者标签
        /// </summary>
        private readonly Func<TransportMessage, object?, string, Task> _msgCallback;

        /// <summary>
        /// 日志回调委托
        /// 当消费者状态发生变化时被调用，用于记录日志和更新健康状态
        /// </summary>
        private readonly Action<LogMessageEventArgs> _logCallback;

        /// <summary>
        /// 标识是否使用 Task.Run 进行并发消息处理
        /// 当 concurrent > 0 时为 true，消息将在独立的线程池线程中处理，
        /// 实际并发度由 BasicQos(prefetchCount) 控制
        /// </summary>
        private readonly bool _usingTaskRun;

        /// <summary>
        /// 标识是否为自动确认模式
        /// 为 true 时消息发送给消费者后立即确认，无需手动调用 BasicAck
        /// </summary>
        private readonly bool _isAutoAck;

        /// <summary>
        /// 初始化 RabbitMQ 异步基础消费者实例
        /// </summary>
        /// <param name="channel">RabbitMQ 通道，用于与服务器通信</param>
        /// <param name="concurrent">
        /// 并发模式。为 0 时消息顺序处理；大于 0 时消息在独立的 Task 中并行处理，
        /// 实际并发度由 BasicQos(prefetchCount) 控制
        /// </param>
        /// <param name="msgCallback">消息接收回调，收到消息时被调用</param>
        /// <param name="logCallback">日志回调，消费者状态变化时被调用</param>
        /// <param name="isAutoAck">是否为自动确认模式，默认为 false（手动确认）</param>
        public RabbitMqBasicConsumer(IChannel channel,
            byte concurrent,
            Func<TransportMessage, object?, string, Task> msgCallback,
            Action<LogMessageEventArgs> logCallback, bool isAutoAck = false
            ) : base(channel)
        {
            _usingTaskRun = concurrent > 0;
            _msgCallback = msgCallback;
            _logCallback = logCallback;
            _isAutoAck = isAutoAck;
        }

        /// <summary>
        /// 处理 RabbitMQ 服务器推送的基本消息交付
        /// 这是消息消费的核心入口方法，当有新消息到达时由 RabbitMQ 客户端库自动调用
        /// </summary>
        /// <param name="consumerTag">消费者标签，唯一标识此消费者</param>
        /// <param name="deliveryTag">消息投递标签，用于消息确认/拒绝</param>
        /// <param name="redelivered">消息是否为重新投递（之前被拒绝或未确认的消息）</param>
        /// <param name="exchange">消息来源的交换机名称</param>
        /// <param name="routingKey">消息的路由键</param>
        /// <param name="properties">消息的基本属性（如持久化模式、优先级等）</param>
        /// <param name="body">消息体的字节内容</param>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步消息处理操作的任务</returns>
        /// <remarks>
        /// 处理模式：
        /// <list type="bullet">
        ///   <item><description>当 <see cref="_usingTaskRun"/> 为 true 时：
        ///     消息在 Task.Run 中异步处理，body 会被复制到安全副本以避免缓冲区回收问题，
        ///     实际并发度由 BasicQos(prefetchCount) 控制</description></item>
        ///   <item><description>当 <see cref="_usingTaskRun"/> 为 false 时：直接在当前上下文中顺序处理消息</description></item>
        /// </list>
        /// </remarks>
        public override async Task HandleBasicDeliverAsync(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey, IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken = default)
        {
            if (_usingTaskRun)
            {
                // 复制消息体到安全副本，防止原始缓冲区被回收后数据丢失
                ReadOnlyMemory<byte> safeBody = body.ToArray();

                // Fire-and-forget：不 await，立即返回已完成的 Task，让通道继续分发下一条消息
                // 实际并发度由 BasicQos(prefetchCount) 控制
                // 不传 cancellationToken 给 Task.Run，避免通道取消时 Task 未执行即丢弃消息
                _ = Task.Run(async () =>
                {
                    await Consume(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, safeBody);
                });
            }
            else
            {
                // 顺序处理模式：直接在当前上下文中消费消息
                await Consume(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);
            }
        }

        /// <summary>
        /// 消费消息的核心逻辑
        /// 将原始消息体解码为 <see cref="TransportMessage"/> 对象，并通过回调委托传递给上层处理
        /// </summary>
        /// <param name="consumerTag">消费者标签</param>
        /// <param name="deliveryTag">消息投递标签</param>
        /// <param name="redelivered">是否为重新投递的消息</param>
        /// <param name="exchange">来源交换机</param>
        /// <param name="routingKey">路由键</param>
        /// <param name="properties">消息属性</param>
        /// <param name="body">消息体字节内容</param>
        /// <returns>表示异步消费操作的任务</returns>
        private Task Consume(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
            string routingKey, IReadOnlyBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            // 构造传输消息对象，将原始字节解码为 UTF-8 字符串
            var message = new TransportMessage(body)
            {
                RoutingKey = routingKey,
                BodyContent = Encoding.UTF8.GetString(body.Span)
            };
            // 调用消息回调，传递消息内容、投递标签（用于后续确认）和消费者标签（用于查找处理器）
            return _msgCallback(message, deliveryTag, consumerTag);
        }

        /// <summary>
        /// 处理消费者注册成功事件
        /// 当消费者成功注册到 RabbitMQ 服务器后被调用
        /// </summary>
        /// <param name="consumerTag">被注册的消费者标签</param>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步操作的任务</returns>
        public override async Task HandleBasicConsumeOkAsync(string consumerTag, CancellationToken cancellationToken = default)
        {
            await base.HandleBasicConsumeOkAsync(consumerTag, cancellationToken).ConfigureAwait(false);

            var args = new LogMessageEventArgs
            {
                LogType = MqLogType.ConsumerRegistered,
                Reason = consumerTag
            };

            _logCallback(args);
        }

        /// <summary>
        /// 处理消费者被取消事件（服务器端发起的取消）
        /// 当 RabbitMQ 服务器取消消费者时被调用（例如队列被删除）
        /// </summary>
        /// <param name="consumerTags">被取消的消费者标签数组</param>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步操作的任务</returns>
        protected override async Task OnCancelAsync(string[] consumerTags, CancellationToken cancellationToken = default)
        {
            await base.OnCancelAsync(consumerTags, cancellationToken).ConfigureAwait(false);

            var args = new LogMessageEventArgs
            {
                LogType = MqLogType.ConsumerCancelled,
                Reason = string.Join(",", consumerTags)
            };

            _logCallback(args);
        }

        /// <summary>
        /// 处理消费者取消确认事件
        /// 当消费者成功取消注册后被调用（客户端发起的取消）
        /// </summary>
        /// <param name="consumerTag">被取消的消费者标签</param>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步操作的任务</returns>
        public override async Task HandleBasicCancelOkAsync(string consumerTag, CancellationToken cancellationToken = default)
        {
            await base.HandleBasicCancelOkAsync(consumerTag, cancellationToken).ConfigureAwait(false);

            var args = new LogMessageEventArgs
            {
                LogType = MqLogType.ConsumerUnregistered,
                Reason = consumerTag
            };

            _logCallback(args);
        }

        /// <summary>
        /// 处理通道关闭事件
        /// 当 RabbitMQ 通道因异常或主动关闭时被调用
        /// </summary>
        /// <param name="channel">被关闭的通道对象</param>
        /// <param name="reason">关闭原因，包含回复码和回复文本</param>
        /// <returns>表示异步操作的任务</returns>
        public override async Task HandleChannelShutdownAsync(object channel, ShutdownEventArgs reason)
        {
            await base.HandleChannelShutdownAsync(channel, reason).ConfigureAwait(false);

            var args = new LogMessageEventArgs
            {
                LogType = MqLogType.ConsumerShutdown,
                Reason = reason.ReplyText
            };

            _logCallback(args);
        }

        /// <summary>
        /// 确认消息已成功处理
        /// 调用后 RabbitMQ 服务器将不再重复投递该消息
        /// 仅在手动确认模式（非 AutoAck）下生效
        /// </summary>
        /// <param name="deliveryTag">消息投递标签</param>
        /// <returns>表示异步确认操作的任务</returns>
        public async Task BasicAck(ulong deliveryTag)
        {
            if (Channel is { IsOpen: true } && !_isAutoAck)
                await Channel.BasicAckAsync(deliveryTag, false).ConfigureAwait(false);
        }

        /// <summary>
        /// 拒绝消息
        /// 仅在手动确认模式（非 AutoAck）下生效
        /// </summary>
        /// <param name="deliveryTag">消息投递标签</param>
        /// <param name="requeue">是否重新入队。为 false 时消息将进入死信队列（需配置 DLX），避免毒消息无限循环</param>
        /// <returns>表示异步拒绝操作的任务</returns>
        public async Task BasicReject(ulong deliveryTag, bool requeue = true)
        {
            if (Channel is { IsOpen: true } && !_isAutoAck)
                await Channel.BasicRejectAsync(deliveryTag, requeue).ConfigureAwait(false);
        }
    }
}