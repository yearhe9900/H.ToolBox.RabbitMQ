using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System;
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
        /// 连接数量，高吞吐场景下多连接可分摊 AMQP 帧处理压力，避免单 TCP 连接成为瓶颈
        /// </summary>
        public int ConnectionCount => _connectionCount;

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

        /// <summary>
        /// 连接数量（已在构造函数中归一化到 [1, 64]）
        /// </summary>
        private readonly int _connectionCount;

        /// <summary>
        /// 连接数组，每个槽位持有一条独立的 RabbitMQ TCP 连接
        /// </summary>
        private IConnection?[] _connections;

        /// <summary>
        /// Round-Robin 计数器，用于在多条连接间均匀分发通道创建请求
        /// </summary>
        private int _robin = -1;

        public ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            IOptions<RabbitMQOptions> optionsAccessor)
            : this(logger, optionsAccessor.Value)
        {
        }

        public ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            RabbitMQOptions options)
            : this(logger, options, CreateConnection(options), startSweepTimer: true)
        {
        }

        /// <summary>
        /// 可注入连接工厂的内部构造函数，供单元测试验证池的信号量不变量，
        /// 无需真实 RabbitMQ Broker。
        /// </summary>
        /// <param name="logger">日志记录器</param>
        /// <param name="options">连接池配置</param>
        /// <param name="connectionActivator">连接工厂委托</param>
        /// <param name="startSweepTimer">是否启动定时空闲扫描；测试时为 false，改用 SweepIdleChannelsNow() 显式触发</param>
        internal ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            RabbitMQOptions options,
            Func<Task<IConnection>> connectionActivator,
            bool startSweepTimer)
        {
            _logger = logger;

            // 归一化连接数量到 [1, 64]，避免过多连接压垮 Broker
            _connectionCount = Math.Max(1, Math.Min(options.ConnectionCount > 0 ? options.ConnectionCount : 1, 64));
            _connections = new IConnection?[_connectionCount];

            // 计算通道池大小，确保至少等于连接数，以充分利用所有 TCP 连接
            var configuredPoolSize = options.PoolSize > 0 ? options.PoolSize : DefaultPoolSize;
            if (configuredPoolSize < _connectionCount)
            {
                _logger.LogInformation(
                    "配置优化：PoolSize ({ConfiguredPoolSize}) 小于 ConnectionCount ({ConnectionCount})，" +
                    "已自动调整为 {AdjustedPoolSize} 以充分利用所有 TCP 连接。",
                    configuredPoolSize, _connectionCount, _connectionCount);
                _currentMaxSize = _connectionCount;
            }
            else
            {
                _currentMaxSize = configuredPoolSize;
            }

            // 计算最大通道池大小，确保至少等于当前大小
            var configuredAbsoluteMaxSize = options.AbsoluteMaxPoolSize > 0
                ? Math.Max(options.AbsoluteMaxPoolSize, _currentMaxSize)
                : _currentMaxSize;

            // 如果 AbsoluteMaxPoolSize 小于 ConnectionCount，自动调整
            if (configuredAbsoluteMaxSize < _connectionCount)
            {
                _logger.LogInformation(
                    "配置优化：AbsoluteMaxPoolSize ({ConfiguredAbsoluteMaxSize}) 小于 ConnectionCount ({ConnectionCount})，" +
                    "已自动调整为 {AdjustedAbsoluteMaxSize} 以确保动态扩展后能充分利用所有 TCP 连接。",
                    configuredAbsoluteMaxSize, _connectionCount, _connectionCount * 2);
                _absoluteMaxSize = _connectionCount * 2;
            }
            else
            {
                _absoluteMaxSize = configuredAbsoluteMaxSize;
            }

            _minSize = Math.Max(0, Math.Min(options.MinPoolSize, _currentMaxSize));
            _idleTimeoutTicks = options.IdleTimeout.Ticks;
            _pool = new ConcurrentQueue<(IChannel, long)>();
            _semaphore = new SemaphoreSlim(_currentMaxSize, _absoluteMaxSize);

            _connectionActivator = connectionActivator ?? throw new ArgumentNullException(nameof(connectionActivator));
            _isPublishConfirms = options.PublishConfirms;

            // 空闲超时未禁用且需要自动扫描时启动定时扫描
            if (startSweepTimer && _idleTimeoutTicks > 0)
            {
                _sweepTimer = new Timer(SweepIdleChannels, null, SweepInterval, SweepInterval);
            }
        }

        /// <summary>
        /// 异步获取一个可用的 RabbitMQ 连接。
        /// 通过 Round-Robin 在多条连接间均匀分发；若选中槽位的连接不存在或已关闭，则按需创建新连接。
        /// 连接采用懒创建策略，首次租借通道 / 创建消费通道时才会真正建立 TCP 连接。
        /// </summary>
        public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            // 每个槽位最多尝试重建一次（再叠加一次兜底），避免底层 activator 异常时陷入死循环
            var maxAttempts = _connectionCount * 2 + 1;
            IConnection? resolved = null;

            for (var attempt = 0; attempt < maxAttempts && resolved == null; attempt++)
            {
                int idx;
                IConnection? candidate;
                lock (_connectionLock)
                {
                    idx = (_robin = (_robin + 1) % _connectionCount);
                    candidate = _connections[idx];
                }

                if (candidate != null && candidate.IsOpen)
                {
                    resolved = candidate;
                    break;
                }

                // 该槽位连接不存在或已关闭，创建新连接
                var connection = await _connectionActivator().ConfigureAwait(false);

                lock (_connectionLock)
                {
                    var existing = _connections[idx];
                    if (existing == null || !existing.IsOpen)
                    {
                        existing?.Dispose();
                        _connections[idx] = connection;
                    }
                    else
                    {
                        // 并发下已有其它线程完成重建，丢弃本次多余连接
                        connection.Dispose();
                    }

                    resolved = _connections[idx];
                }
            }

            if (resolved == null)
            {
                throw new InvalidOperationException(
                    $"无法为连接通道池建立可用的 RabbitMQ 连接（已尝试 {maxAttempts} 次）。请检查 Broker 地址与凭据配置。");
            }

            return resolved;
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
                // 消费者分发并发度：控制单个 channel 上同时分发给 HandleBasicDeliverAsync 的消息数
                // 注意：此值仅对 ConsumerService API 生效；RabbitMQConsumer 特性使用 RabbitMqBasicConsumer
                // 内部的 Task.Run 并发机制，两者不能同时使用，否则会导致并发失控
                ConsumerDispatchConcurrency = options.ConsumerDispatchConcurrency,
                // 高并发场景下 broker 响应可能变慢，增加超时时间避免连接创建被误取消
                HandshakeContinuationTimeout = TimeSpan.FromSeconds(30),
                ContinuationTimeout = TimeSpan.FromSeconds(30),
                SocketReadTimeout = TimeSpan.FromSeconds(30),
                SocketWriteTimeout = TimeSpan.FromSeconds(30)
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

            lock (_connectionLock)
            {
                foreach (var conn in _connections)
                {
                    try { conn?.Dispose(); }
                    catch { /* 释放失败忽略 */ }
                }

                _connections = Array.Empty<IConnection?>();
            }
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

                        return await CreateChannelOnAnyConnectionAsync(cancellationToken).ConfigureAwait(false);
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

                    return await CreateChannelOnAnyConnectionAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    _semaphore.Release();
                    throw;
                }
            }
        }

        /// <summary>
        /// 在任意一条可用连接上创建通道，实现连接级故障转移。
        /// 某条连接上通道创建失败时，自动将该连接标记为失效并切换到下一条连接重试，
        /// 直到成功或尝试完所有连接，避免单条连接故障导致整体不可用。
        /// </summary>
        private async Task<IChannel> CreateChannelOnAnyConnectionAsync(CancellationToken cancellationToken)
        {
            Exception? lastException = null;

            for (var attempt = 0; attempt < _connectionCount; attempt++)
            {
                IConnection connection;
                try
                {
                    connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    continue;
                }

                try
                {
                    return await connection.CreateChannelAsync(
                        BuildChannelOptions(_isPublishConfirms),
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    // 只在连接真正断开时才标记为失效并关闭
                    // 通道创建失败可能是临时网络抖动或 broker 过载，不应盲目关闭整个连接
                    if (!connection.IsOpen)
                    {
                        MarkConnectionFaulted(connection);
                    }
                }
            }

            throw new InvalidOperationException(
                $"无法在任一 RabbitMQ 连接上创建通道（已尝试 {_connectionCount} 条连接）。", lastException);
        }

        /// <summary>
        /// 构建通道创建选项。
        /// publisherConfirmationTrackingEnabled 必须与 publisherConfirmationsEnabled 同时开启：
        /// 关闭 tracking 时 BasicPublishAsync 不会等待 broker 确认，也不会在 nack / basic.return
        /// 时抛出 PublishException，发布确认形同虚设。
        /// </summary>
        internal static CreateChannelOptions BuildChannelOptions(bool publishConfirms) =>
            new(publishConfirms, publishConfirms);

        /// <summary>
        /// 将指定连接从池中移除并释放，下次获取时自动重建。
        /// 用于通道创建失败时的连接级故障转移。
        /// </summary>
        private void MarkConnectionFaulted(IConnection faulted)
        {
            lock (_connectionLock)
            {
                for (var i = 0; i < _connections.Length; i++)
                {
                    if (ReferenceEquals(_connections[i], faulted))
                    {
                        _connections[i] = null;
                        break;
                    }
                }
            }

            try { faulted.Dispose(); }
            catch { /* 释放失败忽略 */ }
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
        /// 保留至少 _minSize 个通道不被回收，避免高峰重建开销。
        /// 注意：连接本身常驻，不随通道回收而关闭，避免频繁重建 TCP 连接的开销。
        /// </summary>
        private void SweepIdleChannels(object? state) => SweepIdleChannelsNow();

        /// <summary>
        /// 当前可用许可数（仅用于测试/诊断，用于验证池的信号量不变量）。
        /// </summary>
        internal int AvailablePermits => _semaphore.CurrentCount;

        /// <summary>
        /// 当前池中空闲通道数（仅用于测试/诊断）。
        /// </summary>
        internal int PooledChannelCount => _pool.Count;

        /// <summary>
        /// 立即执行一次空闲回收，返回被回收的通道数量。
        /// </summary>
        /// <remarks>
        /// 信号量不变量：<c>许可数 = 池容量 - 正在使用的通道数</c>。
        /// 通道归还时 <see cref="Return"/> 已经 Release 了一次许可；空闲回收只是把池中的
        /// 物理通道销毁，下次 Rent 发现池为空会按需新建，许可数不应变化。
        /// 旧实现在回收时又 Release() 一次，导致许可数随每次扫描单调递增，
        /// 最终触发 SemaphoreFullException 并使池容量上限失效。
        /// </remarks>
        internal int SweepIdleChannelsNow()
        {
            try
            {
                var now = DateTime.UtcNow.Ticks;
                var currentPoolCount = _pool.Count;

                // 当前池中通道数不超过最小保留数，无需回收
                if (currentPoolCount <= _minSize) return 0;

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

                    // 回收空闲通道：只销毁物理通道，不再操作信号量
                    // （归还时许可已经释放，此处 Release 会让许可数膨胀）
                    try
                    {
                        entry.Channel.Dispose();
                        disposed++;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "回收空闲通道时发生异常");
                        keepers.Add(entry);
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

                return disposed;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "空闲通道扫描过程中发生异常");
                return 0;
            }
        }

        /// <summary>
        /// 预热连接池，预先创建所有配置的 TCP 连接
        /// 使用渐进式创建策略，每创建一个连接后短暂延迟，避免瞬间压力过大
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步预热操作的任务</returns>
        public async Task WarmupAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("开始预热连接池，目标创建 {ConnectionCount} 个连接", _connectionCount);

            var created = 0;
            var failed = 0;

            for (var i = 0; i < _connectionCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    lock (_connectionLock)
                    {
                        // 如果该槽位已有打开的连接，跳过
                        if (_connections[i] != null && _connections[i]!.IsOpen)
                        {
                            created++;
                            continue;
                        }
                    }

                    // 创建新连接
                    var connection = await _connectionActivator().ConfigureAwait(false);

                    lock (_connectionLock)
                    {
                        var existing = _connections[i];
                        if (existing == null || !existing.IsOpen)
                        {
                            existing?.Dispose();
                            _connections[i] = connection;
                            created++;
                        }
                        else
                        {
                            // 并发下已有其它线程完成创建，丢弃本次多余连接
                            connection.Dispose();
                        }
                    }

                    // 渐进式创建，避免瞬间压力过大
                    if (i < _connectionCount - 1)
                    {
                        await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    failed++;
                    _logger.LogWarning(ex, "预热连接池时创建第 {Index} 个连接失败", i + 1);

                    // 失败后增加延迟，避免快速重试导致压力叠加
                    await Task.Delay(500, cancellationToken).ConfigureAwait(false);
                }
            }

            _logger.LogInformation("连接池预热完成：成功创建 {Created} 个连接，失败 {Failed} 个", created, failed);
        }
    }
}
