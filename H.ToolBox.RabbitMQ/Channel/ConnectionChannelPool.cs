using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Collections.Concurrent;
using System.Reflection;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 连接通道池
    /// </summary>
    public class ConnectionChannelPool : IConnectionChannelPool, IDisposable
    {
        /// <summary>
        /// 定义一个默认通道池大小，默认值为15，实际使用中可以根据需要进行调整
        /// </summary>
        private const int DefaultPoolSize = 15;

        /// <summary>
        /// 定义一个对象锁，用于在多线程环境中保护对连接通道池的访问，确保线程安全
        /// </summary>
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
        /// 当前MQ连接
        /// </summary>
        private IConnection? _connection;

        /// <summary>
        /// 当前通道池中已创建的通道数量，初始值为0，每当创建一个新的通道时，这个计数器会增加，当通道被释放时，这个计数器会减少，以确保不超过最大池大小
        /// </summary>
        private int _count;

        /// <summary>
        /// 最大池大小，表示通道池中允许存在的最大通道数量，默认值为15，可以通过构造函数或配置文件进行调整，以适应不同的应用需求
        /// </summary>
        private int _maxSize;

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
            _maxSize = DefaultPoolSize;
            _pool = new ConcurrentQueue<IChannel>();

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
            // 第一步，将最大值归0，防止在释放过程中有新的通道被创建，造成的内存泄露问题
            _maxSize = 0;

            // 第二步，释放通道池中的所有通道实例，使用TryDequeue方法从通道池中逐个取出通道实例，并调用Dispose方法释放它们，确保所有资源都被正确清理
            while (_pool.TryDequeue(out var channel))
            {
                channel.Dispose();
            }

            // 第三步，释放MQ连接，检查当前的连接实例是否不为null，如果存在，则调用Dispose方法释放连接资源，确保所有与MQ服务器的连接都被正确关闭
            _connection?.Dispose();
        }

        /// <summary>
        /// 租借一个通道（显式接口实现）
        /// 此方法通过锁机制确保在多线程环境下对通道数量的控制
        /// </summary>
        /// <returns>返回一个可用的RabbitMQ通道</returns>
        Task<IChannel> IConnectionChannelPool.Rent()
        {
            // 使用静态锁对象确保线程安全，防止多个线程同时创建过多通道
            lock (SLock)
            {
                // 当当前通道数量超过最大池大小时，进行自旋等待
                // Thread.SpinWait(1) 是一种轻量级的等待方式，适用于短时间等待场景
                // 这样可以避免频繁创建新通道，控制资源使用
                while (_count > _maxSize)
                {
                    Thread.SpinWait(1);
                }

                // 当通道数量在允许范围内时，调用实际的租借逻辑
                return Rent();
            }
        }

        /// <summary>
        /// 租借通道的核心实现
        /// 优先从通道池中获取可用通道，如果池中没有可用通道则创建新通道
        /// </summary>
        /// <returns>返回一个可用的RabbitMQ通道</returns>
        public virtual async Task<IChannel> Rent()
        {
            // 尝试从通道池中获取一个已存在的可用通道
            // 如果成功，减少计数器并返回该通道，实现通道复用
            if (_pool.TryDequeue(out var model))
            {
                // 使用原子操作减少计数器，确保线程安全
                // 注意：这里减少计数是因为从池中取出的通道将被使用，不再属于"可用"状态
                Interlocked.Decrement(ref _count);
                return model;
            }

            // 如果通道池中没有可用通道，则创建新的通道
            try
            {
                // 通过当前连接创建新的通道
                // CreateChannelOptions参数说明：
                // - 第一个参数：是否启用发布确认模式
                // - 第二个参数：是否自动关闭通道（false表示手动管理）
                model = await GetConnection().CreateChannelAsync(new CreateChannelOptions(_isPublishConfirms, false));
            }
            catch (Exception e)
            {
                // 记录通道创建失败的日志，便于问题排查
                _logger.LogError(e, "RabbitMQ 通道创建失败");
                // 重新抛出异常，让调用方处理
                throw;
            }

            // 返回新创建的通道
            return model;
        }

        /// <summary>
        /// 归还通道到池中
        /// 将使用完毕的通道归还到通道池以便复用，如果池已满或通道已关闭则释放该通道
        /// </summary>
        /// <param name="channel">要归还的RabbitMQ通道</param>
        /// <returns>归还是否成功（true表示成功回收到池中，false表示通道被释放）</returns>
        public bool Return(IChannel channel)
        {
            // 原子性地增加计数器，检查是否超过最大池大小
            // 如果未超过且通道仍处于打开状态，则将通道回收到池中
            if (Interlocked.Increment(ref _count) <= _maxSize && channel.IsOpen)
            {
                // 将通道加入线程安全的队列中，供后续租借使用
                _pool.Enqueue(channel);

                return true;
            }

            // 以下情况会执行通道释放：
            // 1. 通道池已满（计数器超过最大值）
            // 2. 通道已经关闭，无法复用
            
            // 释放通道资源，避免内存泄漏
            channel.Dispose();

            // 由于通道未被回收到池中，需要减少计数器以保持计数准确
            Interlocked.Decrement(ref _count);

            return false;
        }
    }
}
