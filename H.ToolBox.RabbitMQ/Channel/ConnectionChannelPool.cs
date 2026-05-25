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

#if NET10_0_OR_GREATER
        private static readonly Lock SLock = new();
#else
        private static readonly object SLock = new();
#endif

        /// <summary>
        /// 一个异步委托，用于创建并返回新的连接实例。
        /// </summary>
        /// <remarks>该委托应返回一个已完成初始化的连接任务。它通常用于提供自定义的连接创建逻辑，例如连接池管理或依赖注入。</remarks>
        private readonly Func<Task<IConnection>> _connectionActivator;

        /// <summary>
        /// 是否启动发布确认
        /// </summary>
        private readonly bool _isPublishConfirms;

        /// <summary>
        /// 日志接口
        /// </summary>
        private readonly ILogger<ConnectionChannelPool> _logger;

        /// <summary>
        /// 线程安全的通道池，使用ConcurrentQueue来存储可用的通道实例，确保在多线程环境中对通道池的访问不会引发竞争条件
        /// </summary>
        private readonly ConcurrentQueue<IChannel> _pool;

        /// <summary>
        /// 使用 SemaphoreSlim 替代 lock + Thread.SpinWait，支持真正的异步等待，
        /// 避免高并发下线程阻塞和 CPU 空转
        /// </summary>
        private readonly SemaphoreSlim _semaphore;

        /// <summary>
        /// 最大池大小
        /// </summary>
        private readonly int _maxSize;

        /// <summary>
        /// 当前MQ连接
        /// </summary>
        private IConnection? _connection;

        /// <summary>
        /// 连接通道池（通过 IOptions 注入）
        /// </summary>
        /// <param name="logger">日志记录器</param>
        /// <param name="optionsAccessor">RabbitMQ 配置选项访问器</param>
        public ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            IOptions<RabbitMQOptions> optionsAccessor)
            : this(logger, optionsAccessor.Value)
        {
        }

        /// <summary>
        /// 连接通道池（直接传入配置选项，供 ConnectionChannelPoolManager 等场景使用）
        /// </summary>
        /// <param name="logger">日志记录器</param>
        /// <param name="options">RabbitMQ 配置选项</param>
        public ConnectionChannelPool(
            ILogger<ConnectionChannelPool> logger,
            RabbitMQOptions options)
        {
            _logger = logger;
            _maxSize = options.PoolSize > 0 ? options.PoolSize : DefaultPoolSize;
            _pool = new ConcurrentQueue<IChannel>();
            _semaphore = new SemaphoreSlim(_maxSize, _maxSize);

            _connectionActivator = CreateConnection(options);
            _isPublishConfirms = options.PublishConfirms;
        }

        /// <summary>
        /// 获取连接
        /// </summary>
        /// <returns></returns>
        public IConnection GetConnection()
        {
            lock (SLock)
            {
                // 如果有现有可用的连接，则直接返回
                if (_connection != null && _connection.IsOpen) return _connection;

                // 先释放所有连接
                _connection?.Dispose();

                // 重新创建新的连接
                _connection = _connectionActivator().GetAwaiter().GetResult();
                return _connection;
            }
        }

        /// <summary>
        /// 创建RabbitMQ连接工厂函数
        /// 根据配置选项创建并返回一个异步连接创建委托
        /// </summary>
        /// <param name="options">RabbitMQ配置选项，包含主机名、用户名、密码等连接信息</param>
        /// <returns>返回一个异步委托，调用时创建新的RabbitMQ连接</returns>
        private static Func<Task<IConnection>> CreateConnection(RabbitMQOptions options)
        {
            // 验证配置：至少需要配置一个主机节点
            if (options.HostName.Count == 0)
            {
                throw new ArgumentException("RabbitMQ HostName 至少配置一个节点");
            }

            // 确定客户端提供的连接名称
            // 如果配置中未指定，则使用当前应用程序集名称（转换为小写）作为默认名称
            var clientProvidedName = string.IsNullOrWhiteSpace(options.ClientProvidedName) 
                ? Assembly.GetEntryAssembly()?.GetName().Name!.ToLower() 
                : options.ClientProvidedName;

            // 创建连接工厂实例，配置基本连接参数
            var factory = new ConnectionFactory
            {
                UserName = options.UserName,          // 用户名
                Port = options.Port,                  // 端口号
                Password = options.Password,          // 密码
                VirtualHost = options.VirtualHost,    // 虚拟主机
                ClientProvidedName = clientProvidedName,  // 客户端提供的连接名称，便于在RabbitMQ管理界面识别
            };

            // 如果配置了自定义的连接工厂选项，执行自定义配置
            // 这允许调用方对ConnectionFactory进行额外的个性化设置
            options.ConnectionFactoryOptions?.Invoke(factory);

            // 解析多个主机节点为AMQP端点列表
            // 支持配置多个主机以实现高可用性或负载均衡
            // 将端口号附加到每个主机名，确保使用配置中的端口而非默认端口
            var hostWithPort = options.HostName.Select(h => h.Contains(':') ? h : $"{h}:{options.Port}");
            var endpoints = AmqpTcpEndpoint.ParseMultiple(string.Join(",", hostWithPort));
            
            // 为每个端点应用SSL配置（从工厂继承SSL设置）
            foreach (var endpoint in endpoints)
            {
                endpoint.Ssl = factory.Ssl;
            }

            // 返回一个异步委托，用于创建RabbitMQ连接
            // 使用闭包捕获factory和endpoints，延迟执行连接创建
            return () => factory.CreateConnectionAsync(endpoints);
        }

        /// <summary>
        /// 释放通道池
        /// </summary>
        public void Dispose()
        {
            // 释放信号量，阻止新的租借请求
            _semaphore.Dispose();

            // 释放通道池中的所有通道实例
            while (_pool.TryDequeue(out var channel))
            {
                channel.Dispose();
            }

            // 释放MQ连接
            _connection?.Dispose();
        }

        /// <summary>
        /// 租借一个通道
        /// 使用 SemaphoreSlim 进行异步等待，当通道池满时线程会挂起让出 CPU，
        /// 而非 lock + Thread.SpinWait 的忙等模式，大幅降低高并发下的 CPU 消耗和线程争用
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>返回一个可用的RabbitMQ通道</returns>
        async Task<IChannel> IConnectionChannelPool.Rent(CancellationToken cancellationToken)
        {
            // 异步等待信号量，当池中所有通道都在使用中时，调用线程会被挂起（不占用 CPU），
            // 直到有通道被归还释放信号量，或 CancellationToken 被取消
            await _semaphore.WaitAsync(cancellationToken);

            try
            {
                // 优先复用池中的已有通道（ConcurrentQueue.TryDequeue 是线程安全的）
                if (_pool.TryDequeue(out var model))
                {
                    return model;
                }

                // 池中无可用通道，创建新通道
                return await GetConnection().CreateChannelAsync(new CreateChannelOptions(_isPublishConfirms, false), cancellationToken);
            }
            catch
            {
                // 通道获取失败（创建异常或连接断开），释放信号量槽位避免泄漏
                _semaphore.Release();
                throw;
            }
        }

        /// <summary>
        /// 租借通道的核心实现（同步兼容，供内部或遗留代码使用）
        /// </summary>
        /// <returns>返回一个可用的RabbitMQ通道</returns>
        public virtual async Task<IChannel> Rent()
        {
            return await ((IConnectionChannelPool)this).Rent();
        }

        /// <summary>
        /// 归还通道到池中
        /// 将使用完毕的通道归还到通道池以便复用，如果通道已关闭则释放该通道
        /// </summary>
        /// <param name="channel">要归还的RabbitMQ通道</param>
        /// <returns>归还是否成功（true表示成功回收到池中，false表示通道被释放）</returns>
        public bool Return(IChannel channel)
        {
            // 通道关闭不可复用，直接释放并归还信号量槽位
            if (!channel.IsOpen)
            {
                channel.Dispose();
                _semaphore.Release();
                return false;
            }

            // 将可用通道放回池中
            _pool.Enqueue(channel);

            // 归还信号量槽位，唤醒等待中的 Rent 调用
            _semaphore.Release();

            return true;
        }
    }
}
