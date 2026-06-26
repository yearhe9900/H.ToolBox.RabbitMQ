using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 连接通道池
    /// </summary>
    public class ConnectionChannelPool : IConnectionChannelPool, IDisposable
    {
        /// <summary>
        /// 默认通道池大小
        /// </summary>
        private const int DefaultPoolSize = 15;

        /// <summary>
        /// 空闲扫描间隔（固定30秒）
        /// </summary>
        private static readonly TimeSpan SweepInterval = TimeSpan.FromSeconds(30);

#if NET10_0_OR_GREATER
        private readonly Lock _connectionLock = new();
#else
        private readonly object _connectionLock = new();
#endif

        private readonly Func<Task<IConnection>> _connectionActivator;
        private readonly bool _isPublishConfirms;
        private readonly ILogger<ConnectionChannelPool> _logger;

        /// <summary>
        /// 是否启用了发布确认模式
        /// </summary>
        public bool IsPublishConfirms => _isPublishConfirms;

        /// <summary>
        /// 通道池，每个条目携带归还时的时间戳（Ticks），用于空闲超时判断
        /// </summary>
        private readonly ConcurrentQueue<(IChannel Channel, long ReturnedAt)> _pool;

        private readonly SemaphoreSlim _semaphore;
        private int _currentMaxSize;
        private readonly int _absoluteMaxSize;
        private readonly int _minSize;
        private readonly long _idleTimeoutTicks;
        private readonly Timer? _sweepTimer;

        private IConnection? _connection;

        public ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            IOptions<RabbitMQOptions> optionsAccessor)
            : this(logger, optionsAccessor.Value)
        {
        }

        public ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            RabbitMQOptions options)
        {
            _logger = logger;
            _currentMaxSize = options.PoolSize > 0 ? options.PoolSize : DefaultPoolSize;
            _absoluteMaxSize = options.AbsoluteMaxPoolSize > 0
                ? Math.Max(options.AbsoluteMaxPoolSize, _currentMaxSize)
                : _currentMaxSize;
            _minSize = Math.Max(0, Math.Min(options.MinPoolSize, _currentMaxSize));
            _idleTimeoutTicks = options.IdleTimeout.Ticks;
            _pool = new ConcurrentQueue<(IChannel, long)>();
            _semaphore = new SemaphoreSlim(_currentMaxSize, _absoluteMaxSize);

            _connectionActivator = CreateConnection(options);
            _isPublishConfirms = options.PublishConfirms;

            // 空闲超时未禁用时启动定时扫描
            if (_idleTimeoutTicks > 0)
            {
                _sweepTimer = new Timer(SweepIdleChannels, null, SweepInterval, SweepInterval);
            }
        }

        /// <summary>
        /// 异步获取连接，如果连接不存在或已关闭则创建新连接
        /// </summary>
        public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            lock (_connectionLock)
            {
                if (_connection != null && _connection.IsOpen) return _connection;
            }

            var connection = await _connectionActivator().ConfigureAwait(false);
            lock (_connectionLock)
            {
                if (_connection == null || !_connection.IsOpen)
                {
                    _connection?.Dispose();
                    _connection = connection;
                }
                else
                {
                    connection.Dispose();
                }
                return _connection;
            }
        }

        /// <summary>
        /// 创建RabbitMQ连接工厂函数
        /// </summary>
        private static Func<Task<IConnection>> CreateConnection(RabbitMQOptions options)
        {
            if (options.HostName.Count == 0)
            {
                throw new ArgumentException("HostName 列表不能为空，至少需要配置一个 RabbitMQ 节点。");
            }

            var clientProvidedName = string.IsNullOrWhiteSpace(options.ClientProvidedName)
                ? Assembly.GetEntryAssembly()?.GetName().Name!.ToLower()
                : options.ClientProvidedName;

            var factory = new ConnectionFactory
            {
                UserName = options.UserName,
                Port = options.Port,
                Password = options.Password,
                VirtualHost = options.VirtualHost,
                ClientProvidedName = clientProvidedName,
            };

            options.ConnectionFactoryOptions?.Invoke(factory);

            var hostWithPort = options.HostName.Select(h => h.Contains(':') ? h : $"{h}:{options.Port}");
            var endpoints = AmqpTcpEndpoint.ParseMultiple(string.Join(",", hostWithPort));

            foreach (var endpoint in endpoints)
            {
                endpoint.Ssl = factory.Ssl;
            }

            return () => factory.CreateConnectionAsync(endpoints);
        }

        /// <summary>
        /// 释放通道池
        /// </summary>
        public void Dispose()
        {
            _sweepTimer?.Dispose();
            _semaphore.Dispose();

            while (_pool.TryDequeue(out var entry))
            {
                entry.Channel.Dispose();
            }

            _connection?.Dispose();
        }

        /// <summary>
        /// 租借一个通道，当通道池满时动态扩展容量
        /// </summary>
        async Task<IChannel> IConnectionChannelPool.Rent(CancellationToken cancellationToken)
        {
            while (true)
            {
                // 先尝试非阻塞获取信号量，避免无谓的等待
                if (_semaphore.Wait(0))
                {
                    try
                    {
                        if (_pool.TryDequeue(out var entry))
                        {
                            return entry.Channel;
                        }

                        var conn = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                        return await conn.CreateChannelAsync(new CreateChannelOptions(_isPublishConfirms, false), cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        _semaphore.Release();
                        throw;
                    }
                }

                // 信号量已满，尝试动态扩展容量
                var current = _currentMaxSize;
                if (current < _absoluteMaxSize)
                {
                    var newSize = Math.Min(current * 2, _absoluteMaxSize);
                    if (Interlocked.CompareExchange(ref _currentMaxSize, newSize, current) == current)
                    {
                        var additional = newSize - current;
                        _semaphore.Release(additional);
                        _logger.LogInformation("通道池动态扩展: {Old} -> {New}", current, newSize);
                        // 扩展成功，重新循环获取信号量
                    }
                }

                // 等待信号量（支持取消）
                await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    if (_pool.TryDequeue(out var entry))
                    {
                        return entry.Channel;
                    }

                    var conn = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                    return await conn.CreateChannelAsync(new CreateChannelOptions(_isPublishConfirms, false), cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    _semaphore.Release();
                    throw;
                }
            }
        }

        /// <summary>
        /// 租借通道的核心实现（同步兼容）
        /// </summary>
        public virtual async Task<IChannel> Rent()
        {
            return await ((IConnectionChannelPool)this).Rent().ConfigureAwait(false);
        }

        /// <summary>
        /// 归还通道到池中
        /// </summary>
        public bool Return(IChannel channel)
        {
            if (!channel.IsOpen)
            {
                channel.Dispose();
                _semaphore.Release();
                return false;
            }

            // 记录归还时间戳，供空闲扫描使用
            _pool.Enqueue((channel, DateTime.UtcNow.Ticks));
            _semaphore.Release();
            return true;
        }

        /// <summary>
        /// 定时扫描并回收空闲超时的通道
        /// 保留至少 _minSize 个通道不被回收，避免高峰重建开销
        /// </summary>
        private void SweepIdleChannels(object? state)
        {
            try
            {
                var now = DateTime.UtcNow.Ticks;
                var currentPoolCount = _pool.Count;

                // 当前池中通道数不超过最小保留数，无需回收
                if (currentPoolCount <= _minSize) return;

                // 计算可回收数量
                var disposableCount = currentPoolCount - _minSize;
                var disposed = 0;

                // 临时队列，用于暂存不满足回收条件的通道
                var keepers = new List<(IChannel Channel, long ReturnedAt)>();

                while (_pool.TryDequeue(out var entry))
                {
                    // 已达到回收数量上限，剩余通道全部保留
                    if (disposed >= disposableCount)
                    {
                        keepers.Add(entry);
                        continue;
                    }

                    // 空闲时间未超时，保留
                    if (now - entry.ReturnedAt < _idleTimeoutTicks)
                    {
                        keepers.Add(entry);
                        continue;
                    }

                    // 回收空闲通道
                    try
                    {
                        entry.Channel.Dispose();
                        disposed++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "回收空闲通道时发生异常");
                    }
                    finally
                    {
                        _semaphore.Release();
                    }
                }

                // 将保留的通道放回池中
                foreach (var entry in keepers)
                {
                    _pool.Enqueue(entry);
                }

                if (disposed > 0)
                {
                    _logger.LogInformation("已回收 {Count} 个空闲通道，当前池中剩余 {Remaining} 个", disposed, keepers.Count);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "空闲通道扫描过程中发生异常");
            }
        }
    }
}
