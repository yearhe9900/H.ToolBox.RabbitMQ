using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Reflection;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// 消费者服务选择器
    /// 负责扫描所有实现了 <see cref="IToolBoxRabbitMQ"/> 或 <see cref="IConfigConsumer"/> 接口的服务，
    /// 并从中提取带有 <see cref="RabbitMQConsumerAttribute"/> 特性的方法作为消费者候选
    /// </summary>
    /// <remarks>
    /// 发现来源有两层：
    ///   1. DI 容器中以接口 ServiceType 注册的实现（<c>GetServices&lt;IToolBoxRabbitMQ&gt;()</c>）；
    ///   2. 程序集扫描兜底：以具体类型注册（<c>AddSingleton&lt;MyConsumer&gt;()</c>）的实现，
    ///      通过 <c>GetService(具体类型)</c> 解析。
    /// 只做"发现"，不自行 new 实例，避免绕过 DI 生命周期。
    /// </remarks>
    public class ConsumerServiceSelector : IConsumerServiceSelector
    {
        private readonly ILogger<ConsumerServiceSelector> _logger;
        private readonly IServiceProvider _serviceProvider;
        private readonly ConfigConsumerOptions _configConsumerOptions;

        /// <summary>
        /// 初始化消费者服务选择器实例
        /// </summary>
        /// <param name="serviceProvider">服务提供者，用于解析所有 <see cref="IToolBoxRabbitMQ"/> 和 <see cref="IConfigConsumer"/> 实现</param>
        /// <param name="logger">日志记录器</param>
        /// <param name="configConsumerOptions">配置驱动的消费者选项</param>
        public ConsumerServiceSelector(
            IServiceProvider serviceProvider,
            ILogger<ConsumerServiceSelector> logger,
            IOptions<ConfigConsumerOptions> configConsumerOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _configConsumerOptions = configConsumerOptions.Value;
        }

        /// <summary>
        /// 扫描特性驱动与配置驱动的消费者候选
        /// </summary>
        /// <returns>全部消费者方法扫描结果</returns>
        public IReadOnlyList<MethodScanResult> SelectCandidates()
        {
            var resultList = new List<MethodScanResult>();

            // 扫描 IToolBoxRabbitMQ 接口的实现（特性驱动）
            ScanToolBoxRabbitMQServices(resultList);

            // 扫描 IConfigConsumer 接口的实现（配置驱动）
            ScanConfigConsumerServices(resultList);

            _logger.LogInformation("消费者扫描完成，共发现 {Count} 个消费者方法，来自 {Types} 个实现类型。",
                resultList.Count,
                resultList.Where(r => r.Instance != null).Select(r => r.Instance!.GetType()).Distinct().Count());

            return resultList;
        }

        /// <summary>
        /// 扫描 IToolBoxRabbitMQ 接口的实现，提取带有 [RabbitMQConsumer] 特性的方法
        /// </summary>
        private void ScanToolBoxRabbitMQServices(List<MethodScanResult> resultList)
        {
            foreach (var serviceInstance in ResolveImplementations<IToolBoxRabbitMQ>())
            {
                var instanceType = serviceInstance.GetType();

                // 找出带 [RabbitMQConsumer] 特性的方法
                var methods = instanceType.GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .Where(m => m.IsDefined(typeof(RabbitMQConsumerAttribute), false))
                    .ToList();

                foreach (var method in methods)
                {
                    var attr = method.GetCustomAttribute<RabbitMQConsumerAttribute>();
                    resultList.Add(new MethodScanResult
                    {
                        Instance = serviceInstance,
                        MethodInfo = method,
                        ConsumerAttribute = attr
                    });
                }
            }
        }

        /// <summary>
        /// 扫描 IConfigConsumer 接口的实现，根据配置文件注册消费者
        /// </summary>
        private void ScanConfigConsumerServices(List<MethodScanResult> resultList)
        {
            if (_configConsumerOptions?.Consumers == null || _configConsumerOptions.Consumers.Count == 0)
            {
                return;
            }

            foreach (var serviceInstance in ResolveImplementations<IConfigConsumer>())
            {
                var instanceType = serviceInstance.GetType();

                // 查找匹配的配置项
                var matchingConfigs = _configConsumerOptions.Consumers
                    .Where(c => c.ClassName == instanceType.FullName || c.ClassName == instanceType.Name)
                    .ToList();

                foreach (var config in matchingConfigs)
                {
                    // 查找匹配的方法
                    var method = instanceType.GetMethod(config.MethodName, BindingFlags.Public | BindingFlags.Instance);
                    if (method == null)
                    {
                        _logger.LogWarning("配置驱动消费者: 在类 {ClassName} 中未找到方法 {MethodName}", config.ClassName, config.MethodName);
                        continue;
                    }

                    // 从配置创建 RabbitMQConsumerAttribute
                    var attr = new RabbitMQConsumerAttribute
                    {
                        ConfigId = config.ConfigId,
                        QueueName = config.QueueName,
                        ConsumerName = config.ConsumerName,
                        Exchange = config.Exchange,
                        ExchangeType = config.ExchangeType,
                        RoutingKey = config.RoutingKey,
                        Durable = config.Durable,
                        AutoAck = config.AutoAck,
                        PrefetchCount = config.PrefetchCount,
                        ListenerCount = config.ListenerCount,
                        WaitOneTimeOut = config.WaitOneTimeOut,
                        ConsumerConcurrent = config.ConsumerConcurrent,
                        QueueType = config.QueueType,
                        QuorumGroupSize = config.QuorumGroupSize,
                        Exclusive = config.Exclusive,
                        AutoDelete = config.AutoDelete
                    };

                    resultList.Add(new MethodScanResult
                    {
                        Instance = serviceInstance,
                        MethodInfo = method,
                        ConsumerAttribute = attr
                    });

                    _logger.LogInformation("配置驱动消费者: 已注册 {ClassName}.{MethodName} -> 队列 {QueueName}",
                        config.ClassName, config.MethodName, config.QueueName);
                }
            }
        }

        /// <summary>
        /// 合并"按接口注册"与"按具体类型注册"的两种实现，保证多个类库的消费者都能被发现。
        /// 不会自行创建 DI 未注册的类型实例；按实现类型去重，避免同一实例被扫描两次。
        /// </summary>
        private IEnumerable<T> ResolveImplementations<T>() where T : class
        {
            var seen = new HashSet<Type>();

            // 直接用 GetService(typeof(IEnumerable<T>))，避免 GetServices<T>() 在
            // 未注册 IEnumerable<T> 的轻量 IServiceProvider 上抛 InvalidOperationException。
            var registered = _serviceProvider.GetService(typeof(IEnumerable<T>)) as IEnumerable<T>;
            if (registered != null)
            {
                foreach (var serviceInstance in registered)
                {
                    if (serviceInstance == null) continue;
                    seen.Add(serviceInstance.GetType());
                    yield return serviceInstance;
                }
            }

            // 兜底：按具体类型注册的实现（多类库各自 AddSingleton<MyConsumer>() 的常见写法）
            foreach (var implementationType in FindImplementationTypes<T>())
            {
                if (seen.Contains(implementationType)) continue;

                if (_serviceProvider.GetService(implementationType) is T instance)
                {
                    seen.Add(implementationType);
                    _logger.LogInformation(
                        "消费者 {Type} 未以 {Interface} 注册，已由程序集扫描兜底发现；建议使用 AddRabbitMqConsumer<T>() 显式注册。",
                        implementationType.FullName, typeof(T).Name);
                    yield return instance;
                }
            }
        }

        /// <summary>
        /// 从已加载程序集中找出 T 的具体实现类型，跳过抽象类型、接口、泛型定义与库自身类型。
        /// </summary>
        private static IEnumerable<Type> FindImplementationTypes<T>() where T : class
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                Type?[] types;
                try
                {
                    types = assembly.GetTypes();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    types = ex.Types;
                }
                catch
                {
                    continue;
                }

                foreach (var type in types)
                {
                    if (type == null || type.IsAbstract || type.IsInterface || type.ContainsGenericParameters)
                        continue;

                    if (typeof(T).IsAssignableFrom(type))
                        yield return type;
                }
            }
        }
    }
}
