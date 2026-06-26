using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using H.ToolBox.RabbitMQ.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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

            // 注册连接通道池管理器（Singleton），负责管理所有集群的连接池
            services.TryAddSingleton<IConnectionChannelPoolManager>(sp =>
            {
                var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<ConnectionChannelPool>>();
                var manager = new ConnectionChannelPoolManager(logger);
                foreach (var config in collection.Configs)
                {
                    manager.RegisterPool(config.ConfigId, config);
                }
                return manager;
            });

            // 注册消费者服务选择器：扫描 IToolBoxRabbitMQ 实现类中的 [RabbitMQConsumer] 特性方法
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

            // 注册单一的消费者注册器，内部根据消费者的 ConfigId 自动路由到对应集群
            services.AddSingleton<IRabbitConsumerRegister, RabbitConsumerRegister>();

            // 注册 Bootstrapper 为 Singleton，同时作为 HostedService 使用同一实例
            // 这样可以在 HostedService 和其他服务之间共享同一个 Bootstrapper 实例
            services.AddSingleton<Bootstrapper>();
            services.AddHostedService(sp => sp.GetRequiredService<Bootstrapper>());

            // 仅当 checkIntervalSeconds > 0 时注册健康检查处理器
            if (checkIntervalSeconds > 0)
            {
                var checkInterval = TimeSpan.FromSeconds(checkIntervalSeconds);
                // 注册传输健康检查处理器为 Singleton，同时作为 HostedService 使用同一实例
                // 定时检查消费者注册器的健康状态，在不健康时自动重启
                services.AddSingleton<TransportCheckProcessor>(sp =>
                {
                    var logger = sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<TransportCheckProcessor>>();
                    return new TransportCheckProcessor(sp, logger, checkInterval);
                });
                services.AddHostedService(sp => sp.GetRequiredService<TransportCheckProcessor>());
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