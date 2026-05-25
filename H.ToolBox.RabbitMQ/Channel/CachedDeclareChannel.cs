using RabbitMQ.Client;
using System.Runtime.CompilerServices;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 带声明缓存的通道装饰器
    /// 对同一个 exchange/queue 只声明一次，后续调用直接跳过，避免每次发送都产生一次网络往返
    /// </summary>
    internal sealed class CachedDeclareChannel
    {
        /// <summary>
        /// 缓存附加到 IChannel 实例上，通道被 GC 时缓存自动回收
        /// </summary>
        private static readonly ConditionalWeakTable<IChannel, HashSet<string>> s_caches = new();
        private static readonly object s_cacheLock = new();

        private readonly IChannel _channel;
        private readonly HashSet<string> _declared;

        public CachedDeclareChannel(IChannel channel)
        {
            _channel = channel;
            _declared = GetOrCreateCache(channel);
        }

        public bool IsOpen => _channel.IsOpen;

        /// <summary>
        /// 声明交换机（幂等缓存，同参数只声明一次）
        /// </summary>
        public async Task ExchangeDeclareAsync(string exchange, string type, bool durable, CancellationToken cancellationToken)
        {
            var key = $"e|{exchange}|{type}|{durable}";
            if (!_declared.Add(key)) return;

            await _channel.ExchangeDeclareAsync(exchange, type, durable, false, null, false, cancellationToken);
        }

        /// <summary>
        /// 声明队列（幂等缓存，同参数只声明一次）
        /// </summary>
        public async Task QueueDeclareAsync(string queue, bool durable, CancellationToken cancellationToken)
        {
            var key = $"q|{queue}|{durable}";
            if (!_declared.Add(key)) return;

            await _channel.QueueDeclareAsync(queue, durable, false, false, null, false, cancellationToken);
        }

        /// <summary>
        /// 发布消息（直接委托给底层通道）
        /// </summary>
        public ValueTask BasicPublishAsync<TBasicProperties>(
            string exchange, string routingKey, bool mandatory,
            TBasicProperties basicProperties, ReadOnlyMemory<byte> body,
            CancellationToken cancellationToken = default)
            where TBasicProperties : IReadOnlyBasicProperties, IAmqpHeader
        {
            return _channel.BasicPublishAsync(exchange, routingKey, mandatory, basicProperties, body, cancellationToken);
        }

        /// <summary>
        /// 强制释放通道（通道状态异常时使用）
        /// </summary>
        public ValueTask DisposeAsync()
        {
            return _channel.DisposeAsync();
        }

        private static HashSet<string> GetOrCreateCache(IChannel channel)
        {
            lock (s_cacheLock)
            {
                if (s_caches.TryGetValue(channel, out var existing))
                    return existing;

                var cache = new HashSet<string>(StringComparer.Ordinal);
                s_caches.Add(channel, cache);
                return cache;
            }
        }
    }
}
