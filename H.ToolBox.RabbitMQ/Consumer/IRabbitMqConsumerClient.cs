using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Transport;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ 消费者客户端接口
    /// 定义消费者客户端的生命周期管理契约，包括连接、监听、确认/拒绝消息等操作
    /// </summary>
    /// <remarks>
    /// 消费者客户端负责与 RabbitMQ 服务器建立连接、声明队列/交换机、
    /// 设置 QoS 参数并持续监听消息。收到消息后通过回调委托通知上层处理。
    /// </remarks>
    public interface IRabbitMqConsumerClient : IAsyncDisposable
    {
        /// <summary>
        /// 获取或设置日志回调委托
        /// 当消费者状态发生变化（注册、取消、关闭等）时触发
        /// </summary>
        Action<LogMessageEventArgs>? OnLogCallback { get; set; }

        /// <summary>
        /// 获取或设置消息接收回调委托
        /// 当消费者收到消息时触发，参数分别为：消息对象、投递标签（用于确认）、消费者标签
        /// </summary>
        Func<TransportMessage, object?, string, Task>? OnMessageCallback { get; set; }

        /// <summary>
        /// 异步连接到 RabbitMQ 服务器并建立通道
        /// </summary>
        /// <returns>表示异步连接操作的任务</returns>
        Task ConnectAsync();

        /// <summary>
        /// 异步确认消息已成功处理
        /// 调用后 RabbitMQ 服务器将不再重复投递该消息
        /// </summary>
        /// <param name="sender">消息投递标签（deliveryTag），由消息回调传入</param>
        /// <returns>表示异步确认操作的任务</returns>
        Task CommitAsync(object? sender);

        /// <summary>
        /// 异步拒绝消息。默认 <c>requeue:false</c>，消息进入死信队列（需启用 DLX），
        /// 避免毒消息无限重投打满 CPU 和日志。
        /// </summary>
        /// <param name="sender">消息投递标签（deliveryTag），由消息回调传入</param>
        /// <param name="requeue">
        /// 是否重新入队。仅在处理瞬时故障且业务侧自行保证重试上限时才应显式传 true。
        /// </param>
        /// <returns>表示异步拒绝操作的任务</returns>
        /// <remarks>
        /// 破坏性变更：旧版本无此参数且底层固定 requeue:true；新版默认 false。
        /// </remarks>
        Task RejectAsync(object? sender, bool requeue = false);

        /// <summary>
        /// 启动消息监听，持续消费指定队列中的消息
        /// 该方法会阻塞当前任务直到取消令牌被触发
        /// </summary>
        /// <param name="timeout">监听超时时间，控制轮询等待间隔</param>
        /// <param name="cancellationToken">取消令牌，用于停止监听</param>
        /// <param name="options">监听配置选项，包含队列名、交换机、QoS 等参数</param>
        /// <returns>表示异步监听操作的任务</returns>
        Task ListeningAsync(TimeSpan timeout, CancellationToken cancellationToken, ListeningOptions options);
    }
}
