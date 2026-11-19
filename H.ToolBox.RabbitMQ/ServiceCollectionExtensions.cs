using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using H.ToolBox.RabbitMQ.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace H.ToolBox.RabbitMQ
{
    /// <summary>
    /// 服务集合扩展方法，提供 RabbitMQ 相关服务的便捷注册入口
    /// </summary>
    /// <remarks>
    /// 通过 <c>services.AddRabbitMQ(options => { ... })</c> 一行代码即可完成所有
    /// RabbitMQ 相关服务的依赖注入注册，包括连接池、消费者、生产者等组件。
    /// 支持单集群和多集群两种模式。
    /// </remarks>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// 注册一个 <see cref="IToolBoxRabbitMQ"/> 消费者实现（多类库场景推荐入口）。
        /// </summary>
        /// <typeparam name="T">实现了 <see cref="IToolBoxRabbitMQ"/> 的消费者类型</typeparam>
        /// <param name="services">服务集合</param>
        /// <returns>服务集合本身，便于链式调用</returns>
        /// <remarks>
        /// 消费者发现依赖 <c>GetServices&lt;IToolBoxRabbitMQ&gt;()</c>，它只返回 ServiceType
        /// 就是 <see cref="IToolBoxRabbitMQ"/> 的注册。若各业务类库用
        /// <c>TryAddSingleton&lt;IToolBoxRabbitMQ, T&gt;()</c>，TryAdd 会按 ServiceType 判重，
        /// 导致只有最早注入的一个实现能被发现；若用
        /// <c>AddSingleton&lt;T&gt;()</c> 按具体类型注册，则一个都发现不了。
        /// 本方法按 (ServiceType, ImplementationType) 判重，保证每个实现类都被注册一次。
        /// 已通过程序集扫描兜底兼容按具体类型注册的旧写法。
        /// </remarks>
        public static IServiceCollection AddRabbitMqConsumer<T>(this IServiceCollection services)
            where T : class, IToolBoxRabbitMQ
        {
            if (services == null) throw new ArgumentNullException(nameof(services));

            var alreadyRegistered = services.Any(d =>
                d.ServiceType == typeof(IToolBoxRabbitMQ) && d.ImplementationType == typeof(T));

            if (!alreadyRegistered)
            {
                services.AddSingleton<IToolBoxRabbitMQ, T>();
            }

            return services;
        }

        /// <summary>
        /// 注册一个 <see cref="IConfigConsumer"/> 消费者实现（配置驱动场景，语义同
        /// <see cref="AddRabbitMqConsumer{T}(IServiceCollection)"/>）。
        /// </summary>
        /// <typeparam name="T">实现了 <see cref="IConfigConsumer"/> 的消费者类型</typeparam>
        /// <param name="services">服务集合</param>
        /// <returns>服务集合本身</returns>
        public static IServiceCollection AddConfigConsumerService<T>(this IServiceCollection services)
            where T : class, IConfigConsumer
        {
            if (services == null) throw new ArgumentNullException(nameof(services));

            var alreadyRegistered = services.Any(d =>
                d.ServiceType == typeof(IConfigConsumer) && d.ImplementationType == typeof(T));

            if (!alreadyRegistered)
            {
                services.AddSingleton<IConfigConsumer, T>();
            }

            return services;
        }

        /// <summary>
        /// 向服务集合中注册 RabbitMQ 所需的所有服务（单集群模式，兼容旧版本）
        /// </summary>
        /// <param name="services">服务集合</param>
        /// <param name="optionsAction">
        /// RabbitMQ 配置选项的委托，用于配置连接信息（主机、端口、用户名、密码等）
        /// </param>
        /// <param name="checkIntervalSeconds">健康检查间隔时间（秒），默认 30 秒。当值小于等于 0 时，不开启健康检查</param>
        /// <exception cref="ArgumentNullException">当 <paramref name="optionsAction"/> 为 null 时抛出</exception>
        public static void AddRabbitMQ(this IServiceCollection services, Action<RabbitMQOptions> optionsAction, int checkIntervalSeconds = 30)
        {
            if (optionsAction == null) throw new ArgumentNullException(nameof(optionsAction));

            // 构建选项对象以获取 ConfigId
            var options = new RabbitMQOptions();
            optionsAction(options);

            services.AddRabbitMQ(configs =>
            {
                configs.Add(options);
            }, checkIntervalSeconds);
        }

        /// <summary>
        /// 向服务集合中注册 RabbitMQ 所需的所有服务（多集群模式）
        /// 通过 <paramref name="configsAction"/> 注册多个集群配置，每个集群通过 ConfigId 区分
        /// </summary>
        /// <param name="services">服务集合</param>
        /// <param name="configsAction">
        /// 集群配置委托，通过 <c>Add</c> 方法添加多个 <see cref="RabbitMQOptions"/> 实例，
        /// 每个实例的 <see cref="RabbitMQOptions.ConfigId"/> 必须唯一
        /// </param>
        /// <param name="checkIntervalSeconds">健康检查间隔时间（秒），默认 30 秒。当值小于等于 0 时，不开启健康检查</param>
        /// <exception cref="ArgumentNullException">当 <paramref name="configsAction"/> 为 null 时抛出</exception>
        /// <remarks>
        /// 注册的服务包括：
        /// <list type="bullet">
        ///   <item><description><see cref="IConsumerServiceSelector"/> - 消费者服务选择器（Singleton，TryAdd）</description></item>
        ///   <item><description><see cref="IRabbitMqTransport"/> - 消息传输服务（Singleton）</description></item>
        ///   <item><description><see cref="IRabbitMqConsumerClientFactory"/> - 消费者客户端工厂（Singleton）</description></item>
        ///   <item><description><see cref="IConnectionChannelPoolManager"/> - 连接通道池管理器（Singleton）</description></item>
        ///   <item><description><see cref="MethodMatcherCache"/> - 方法匹配缓存（Singleton，TryAdd）</description></item>
        ///   <item><description>每个集群的 <see cref="IRabbitConsumerRegister"/> - 消费者注册器（Singleton）</description></item>
        ///   <item><description><see cref="Bootstrapper"/> - 后台启动器（Singleton + HostedService）</description></item>
        ///   <item><description><see cref="TransportCheckProcessor"/> - 传输健康检查处理器（Singleton + HostedService，仅当 checkIntervalSeconds > 0 时注册）</description></item>
        /// </list>
        /// </remarks>
        public static void AddRabbitMQ(this IServiceCollection services, Action<RabbitMQConfigCollection> configsAction, int checkIntervalSeconds = 30)
        {
            if (configsAction == null) throw new ArgumentNullException(nameof(configsAction));

            // 收集所有集群配置
            var collection = new RabbitMQConfigCollection();
            configsAction(collection);

            if (collection.Configs.Count == 0)
                throw new ArgumentException("至少需要注册一个 RabbitMQ 集群配置。");

            // 验证 ConfigId 唯一性
            var configIds = collection.Configs.Select(c => c.ConfigId).ToList();
            var duplicates = configIds.GroupBy(x => x).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
            if (duplicates.Count > 0)
                throw new ArgumentException($"存在重复的 ConfigId: {string.Join(", ", duplicates)}");

            // 把本次配置合并进共享注册表：多次调用 AddRabbitMQ（例如各业务类库各自注册）
            // 时，早期实现只捕获第一次的 collection，后续集群会被静默丢弃。
            var registry = services
                .FirstOrDefault(d => d.ServiceType == typeof(RabbitMqClusterConfiguration))?
                .ImplementationInstance as RabbitMqClusterConfiguration;

            if (registry == null)
            {
                registry = new RabbitMqClusterConfiguration();
                services.AddSingleton(registry);
            }

            registry.Merge(collection.Configs);

            // 注册连接通道池管理器（Singleton），在容器构建时读取合并后的完整配置
            services.TryAddSingleton<IConnectionChannelPoolManager>(sp =>
            {
                var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ConnectionChannelPool>>();
                var manager = new ConnectionChannelPoolManager(logger);
                foreach (var config in sp.GetRequiredService<RabbitMqClusterConfiguration>().Configs)
                {
                    manager.RegisterPool(config.ConfigId, config);
                }
                return manager;
            });

            // 注册消费者服务选择器：扫描 IToolBoxRabbitMQ 和 IConfigConsumer 实现类
            services.TryAddSingleton<IConsumerServiceSelector, ConsumerServiceSelector>();

            // 注册消息序列化器（TryAdd：用户可在调用 AddRabbitMQ 前注册自定义实现以覆盖默认值）
            services.TryAddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();

            // 注册消息传输服务（生产者），用于发送消息到 RabbitMQ
            // 使用 TryAddSingleton 允许用户在调用 AddRabbitMQ 前注册自定义实现以覆盖默认值
            services.TryAddSingleton<IRabbitMqTransport, RabbitMqTransport>();

            // 注册消费者客户端工厂，负责创建并初始化消费者客户端实例
            services.TryAddSingleton<IRabbitMqConsumerClientFactory, RabbitMqConsumerClientFactory>();

            // 注册消费者服务，提供简化的队列消费和路由消费 API
            services.TryAddSingleton<IConsumerService, ConsumerService>();

            // 注册方法匹配缓存，缓存消费者名称到扫描结果的映射，避免重复反射扫描
            services.TryAddSingleton<MethodMatcherCache>();

            // 注册单一的消费者注册器（TryAdd：重复调用 AddRabbitMQ 不应产生多个注册器）
            services.TryAddSingleton<IRabbitConsumerRegister, RabbitConsumerRegister>();

            // 基础设施只注册一次：重复 AddRabbitMQ 若产生多个 Bootstrapper / 健康检查器，
            // 同一个消费者会被启动多次并重复消费消息。
            var infraAlreadyRegistered = services.Any(d => d.ServiceType == typeof(RabbitMqInfrastructureMarker));
            if (!infraAlreadyRegistered)
            {
                services.AddSingleton<RabbitMqInfrastructureMarker>();

                // Bootstrapper 作为 Singleton，同时以同一实例注册为 HostedService
                services.TryAddSingleton<Bootstrapper>();
                services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<Bootstrapper>());

                // 仅当 checkIntervalSeconds > 0 时注册健康检查处理器（以首次调用的配置为准）
                if (checkIntervalSeconds > 0)
                {
                    var checkInterval = TimeSpan.FromSeconds(checkIntervalSeconds);
                    services.TryAddSingleton<TransportCheckProcessor>(sp =>
                    {
                        var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<TransportCheckProcessor>>();
                        return new TransportCheckProcessor(sp, logger, checkInterval);
                    });
                    services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TransportCheckProcessor>());
                }
            }
        }

        /// <summary>
        /// 向服务集合中注册配置驱动的 RabbitMQ 消费者服务
        /// </summary>
        /// <param name="services">服务集合</param>
        /// <param name="configuration">配置对象，用于读取 ConfigConsumer 配置节</param>
        /// <param name="sectionName">配置节名称，默认为 "ConfigConsumer"</param>
        /// <remarks>
        /// 此方法会从配置文件中读取 <paramref name="sectionName"/> 节的配置，
        /// 并注册 <see cref="IConfigConsumer"/> 接口的实现类。
        /// 配置文件格式示例：
        /// <code>
        /// {
        ///   "ConfigConsumer": {
        ///     "Consumers": [
        ///       {
        ///         "ConfigId": "default",
        ///         "QueueName": "my.queue",
        ///         "ClassName": "MyNamespace.MyConsumerClass",
        ///         "MethodName": "HandleMessage"
        ///       }
        ///     ]
        ///   }
        /// }
        /// </code>
        /// </remarks>
        public static void AddConfigConsumer(this IServiceCollection services, IConfiguration configuration, string sectionName = "ConfigConsumer")
        {
            // 绑定配置
            var configConsumerOptions = configuration.GetSection(sectionName).Get<ConfigConsumerOptions>();
            if (configConsumerOptions != null)
            {
                services.Configure<ConfigConsumerOptions>(options =>
                {
                    options.Consumers = configConsumerOptions.Consumers;
                });
            }
        }

        /// <summary>
        /// 向服务集合中注册配置驱动的 RabbitMQ 消费者服务（通过委托配置）
        /// </summary>
        /// <param name="services">服务集合</param>
        /// <param name="optionsAction">配置选项委托</param>
        public static void AddConfigConsumer(this IServiceCollection services, Action<ConfigConsumerOptions> optionsAction)
        {
            services.Configure(optionsAction);
        }
    }

    /// <summary>
    /// 防止多次调用 AddRabbitMQ 时重复注册 HostedService 的标记类型。
    /// </summary>
    internal sealed class RabbitMqInfrastructureMarker
    {
    }

    /// <summary>
    /// 多次调用 AddRabbitMQ 时共用的一份集群配置注册表。
    /// 旧实现把第一次调用的 collection 闭包进单例工厂，后续调用新增的集群会被静默丢弃。
    /// </summary>
    internal sealed class RabbitMqClusterConfiguration
    {
        private readonly object _sync = new();
        private readonly List<RabbitMQOptions> _configs = new();

        public IReadOnlyList<RabbitMQOptions> Configs
        {
            get
            {
                lock (_sync)
                {
                    return _configs.ToList();
                }
            }
        }

        public void Merge(IEnumerable<RabbitMQOptions> configs)
        {
            lock (_sync)
            {
                foreach (var config in configs)
                {
                    if (_configs.Any(c => c.ConfigId == config.ConfigId))
                    {
                        throw new ArgumentException(
                            $"存在重复的 ConfigId: {config.ConfigId}（多次调用 AddRabbitMQ 时配置会合并）");
                    }

                    _configs.Add(config);
                }
            }
        }
    }

    /// <summary>
    /// RabbitMQ 多集群配置集合
    /// 用于在 <see cref="ServiceCollectionExtensions.AddRabbitMQ(IServiceCollection, Action{RabbitMQConfigCollection})"/> 中
    /// 收集多个集群的配置
    /// </summary>
    public class RabbitMQConfigCollection
    {
        /// <summary>
        /// 已注册的集群配置列表
        /// </summary>
        internal List<RabbitMQOptions> Configs { get; } = new();

        /// <summary>
        /// 添加一个集群配置
        /// </summary>
        /// <param name="options">RabbitMQ 配置选项，其中 <see cref="RabbitMQOptions.ConfigId"/> 必须唯一</param>
        public void Add(RabbitMQOptions options)
        {
            Configs.Add(options);
        }

        /// <summary>
        /// 添加一个集群配置（通过委托方式配置）
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <param name="optionsAction">配置选项委托</param>
        public void Add(string configId, Action<RabbitMQOptions> optionsAction)
        {
            var options = new RabbitMQOptions { ConfigId = configId };
            optionsAction(options);
            Configs.Add(options);
        }
    }
}