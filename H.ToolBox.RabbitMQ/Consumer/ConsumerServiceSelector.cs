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

        public IReadOnlyList<MethodScanResult> SelectCandidates()
        {
            var resultList = new List<MethodScanResult>();

            // 扫描 IToolBoxRabbitMQ 接口的实现（特性驱动）
            ScanToolBoxRabbitMQServices(resultList);

            // 扫描 IConfigConsumer 接口的实现（配置驱动）
            ScanConfigConsumerServices(resultList);

            return resultList;
        }

        /// <summary>
        /// 扫描 IToolBoxRabbitMQ 接口的实现，提取带有 [RabbitMQConsumer] 特性的方法
        /// </summary>
        private void ScanToolBoxRabbitMQServices(List<MethodScanResult> resultList)
        {
            var mqServices = _serviceProvider.GetServices<IToolBoxRabbitMQ>();

            foreach (var serviceInstance in mqServices)
            {
                if (serviceInstance == null) continue;

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

            var configConsumerServices = _serviceProvider.GetServices<IConfigConsumer>();

            foreach (var serviceInstance in configConsumerServices)
            {
                if (serviceInstance == null) continue;

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
    }
}
