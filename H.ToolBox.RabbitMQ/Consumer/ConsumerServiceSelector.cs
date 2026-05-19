using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// 消费者服务选择器
    /// 负责扫描所有实现了 <see cref="IToolBoxRabbitMQ"/> 接口的服务，
    /// 并从中提取带有 <see cref="RabbitMQConsumerAttribute"/> 特性的方法作为消费者候选
    /// </summary>
    public class ConsumerServiceSelector : IConsumerServiceSelector
    {
        private readonly ILogger<ConsumerServiceSelector> _logger;
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// 初始化消费者服务选择器实例
        /// </summary>
        /// <param name="serviceProvider">服务提供者，用于解析所有 <see cref="IToolBoxRabbitMQ"/> 实现</param>
        /// <param name="logger">日志记录器</param>
        public ConsumerServiceSelector(IServiceProvider serviceProvider, ILogger<ConsumerServiceSelector> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        public IReadOnlyList<MethodScanResult> SelectCandidates()
        {
            var resultList = new List<MethodScanResult>();

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
            return resultList;
        }
    }
}
