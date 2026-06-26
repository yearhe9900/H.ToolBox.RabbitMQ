using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// Publisher Confirm 追踪器。
    /// RabbitMQ .NET Client 7.x 移除了 WaitForConfirmsAsync，
    /// 改为通过 BasicAcksAsync/BasicNacksAsync 事件接收确认回调。
    /// 本类通过注册通道事件 + ConcurrentDictionary 存储待确认消息 + SemaphoreSlim 轮询等待，
    /// 模拟阻塞等待所有消息确认的行为。
    /// </summary>
    internal sealed class PublishConfirmsTracker : IDisposable
    {
        private sealed class PendingConfirm
        {
            public TaskCompletionSource<bool> Tcs { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private readonly ConcurrentDictionary<ulong, PendingConfirm> _pendingConfirms = new();
        private readonly ConcurrentBag<ulong> _nackedTags = new();
        private readonly SemaphoreSlim _signal = new(0);
        private readonly ILogger _logger;
        private readonly TimeSpan _timeout;
        private IChannel? _channel;
        private volatile bool _disposed;

        /// <summary>
        /// 是否有待确认的消息
        /// </summary>
        public bool HasPending => !_pendingConfirms.IsEmpty;

        public PublishConfirmsTracker(ILogger logger, TimeSpan timeout)
        {
            _logger = logger;
            _timeout = timeout;
        }

        /// <summary>
        /// 注册通道的 BasicAcksAsync/BasicNacksAsync 事件。
        /// 应在首次使用通道发布消息前调用一次。
        /// </summary>
        public void RegisterChannel(IChannel channel)
        {
            _channel = channel;
            channel.BasicAcksAsync += OnBasicAcks;
            channel.BasicNacksAsync += OnBasicNacks;
        }

        /// <summary>
        /// 追踪一个待确认的 deliveryTag
        /// </summary>
        public void Track(ulong deliveryTag)
        {
            _pendingConfirms.TryAdd(deliveryTag, new PendingConfirm());
        }

        /// <summary>
        /// 轮询等待所有已追踪的消息被确认，超时则抛出 TimeoutException。
        /// 如果任何消息被 Broker NACK，抛出 InvalidOperationException。
        /// </summary>
        public async Task WaitForAllConfirmsAsync(CancellationToken cancellationToken)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_timeout);

            while (!_pendingConfirms.IsEmpty)
            {
                try
                {
                    await _signal.WaitAsync(cts.Token).ConfigureAwait(false);
                    // 补充检查：信号可能在 WaitAsync 之前已被释放，此时队列已空
                    if (_pendingConfirms.IsEmpty) break;
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    // 超时而非外部取消
                    var pendingTags = string.Join(", ", _pendingConfirms.Keys.OrderBy(k => k));
                    throw new TimeoutException(
                        $"等待发布确认超时（{_timeout.TotalSeconds}秒），仍有 {_pendingConfirms.Count} 条消息未确认。" +
                        $"未确认的 deliveryTag: [{pendingTags}]");
                }
            }

            // 检查是否有被 NACK 的消息
            if (!_nackedTags.IsEmpty)
            {
                var nackedTags = string.Join(", ", _nackedTags.OrderBy(k => k));
                throw new InvalidOperationException(
                    $"消息被 Broker 拒绝（NACK），deliveryTag: [{nackedTags}]");
            }
        }

        /// <summary>
        /// 移除通道的事件订阅，防止内存泄漏
        /// </summary>
        public void UnregisterChannel(IChannel channel)
        {
            channel.BasicAcksAsync -= OnBasicAcks;
            channel.BasicNacksAsync -= OnBasicNacks;
        }

        /// <summary>
        /// 清理所有待确认项（超时或异常退出时调用）
        /// </summary>
        public void Clear()
        {
            foreach (var kvp in _pendingConfirms)
            {
                kvp.Value.Tcs.TrySetCanceled();
            }
            _pendingConfirms.Clear();
        }

        private Task OnBasicAcks(object? sender, BasicAckEventArgs args)
        {
            ProcessConfirm(args.DeliveryTag, isAck: true);
            return Task.CompletedTask;
        }

        private Task OnBasicNacks(object? sender, BasicNackEventArgs args)
        {
            ProcessConfirm(args.DeliveryTag, isAck: false);
            return Task.CompletedTask;
        }

        /// <summary>
        /// 处理 Broker 确认（ACK 或 NACK）。
        /// Broker 的确认是累积的：deliveryTag=N 表示所有 tag &lt;= N 的消息都已确认。
        /// </summary>
        private void ProcessConfirm(ulong deliveryTag, bool isAck)
        {
            // 累积确认：移除所有 <= deliveryTag 的待确认项
            foreach (var tag in _pendingConfirms.Keys.Where(k => k <= deliveryTag).OrderBy(k => k))
            {
                if (_pendingConfirms.TryRemove(tag, out var pending))
                {
                    if (isAck)
                    {
                        pending.Tcs.TrySetResult(true);
                    }
                    else
                    {
                        _nackedTags.Add(tag);
                        _logger.LogWarning("消息被 Broker NACK，deliveryTag: {DeliveryTag}", tag);
                        pending.Tcs.TrySetResult(false);
                    }
                }
            }

            // 通知等待循环有状态变化（可能在 Dispose 之后被回调，需要保护）
            if (!_disposed)
            {
                _signal.Release();
            }
        }

        public void Dispose()
        {
            _disposed = true;
            // 风险1修复：取消事件订阅，防止已 Dispose 的 tracker 被 channel 事件引用链持有
            if (_channel != null)
            {
                UnregisterChannel(_channel);
                _channel = null;
            }
            Clear();
            _signal.Dispose();
        }
    }
}
