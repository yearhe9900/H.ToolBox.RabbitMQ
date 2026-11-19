using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace H.ToolBox.RabbitMQ.Tests.Consumer
{
    /// <summary>
    /// 复现用户报告的缺陷：多个类库各有一个实现 IToolBoxRabbitMQ 的消费者时，
    /// 旧实现只能发现最早注入的那个。这里用两个不同命名空间但同短类名的类型做回归。
    /// </summary>
    public class MultiImplementationDiscoveryTests
    {
        private static ConsumerServiceSelector CreateSelector(IServiceProvider provider)
        {
            return new ConsumerServiceSelector(
                provider,
                new Mock<ILogger<ConsumerServiceSelector>>().Object,
                Options.Create(new ConfigConsumerOptions()));
        }

        [Fact]
        public void SelectCandidates_WithTwoSameNamedImplementations_ShouldFindBoth()
        {
            // Arrange
            var services = new ServiceCollection();
            services.AddRabbitMqConsumer<MultiLibA.OrderConsumer>();
            services.AddRabbitMqConsumer<MultiLibB.OrderConsumer>();
            using var provider = services.BuildServiceProvider();

            // Act
            var results = CreateSelector(provider).SelectCandidates();

            // Assert - 两个类库的消费者都必须被发现
            Assert.Equal(2, results.Count);
            Assert.Contains(results, r => r.ConsumerAttribute!.QueueName == "queue.a");
            Assert.Contains(results, r => r.ConsumerAttribute!.QueueName == "queue.b");
        }

        [Fact]
        public void MethodMatcherCache_WithTwoSameNamedImplementations_ShouldHaveDistinctKeys()
        {
            // Arrange - 旧实现用 DeclaringType.Name（短类名）生成 ConsumerName，
            // 两个 OrderConsumer.Handle 会撞名并被 Entries[name] = ... 静默覆盖成 1 条。
            var services = new ServiceCollection();
            services.AddRabbitMqConsumer<MultiLibA.OrderConsumer>();
            services.AddRabbitMqConsumer<MultiLibB.OrderConsumer>();
            using var provider = services.BuildServiceProvider();
            var cache = new MethodMatcherCache(CreateSelector(provider));

            // Act
            var entries = cache.TrySetExecutor();

            // Assert
            Assert.Equal(2, entries.Count);
            Assert.Contains(entries.Keys, k => k.Contains("MultiLibA"));
            Assert.Contains(entries.Keys, k => k.Contains("MultiLibB"));
        }

        [Fact]
        public void SelectCandidates_WhenRegisteredByConcreteType_ShouldFallbackToAssemblyScan()
        {
            // Arrange - 按具体类型注册时 GetServices<IToolBoxRabbitMQ>() 查不到，
            // 需要程序集扫描 + GetService(具体类型) 兜底
            var services = new ServiceCollection();
            services.AddSingleton<MultiLibA.OrderConsumer>();
            using var provider = services.BuildServiceProvider();

            // Act
            var results = CreateSelector(provider).SelectCandidates();

            // Assert
            Assert.Single(results);
            Assert.Equal("queue.a", results[0].ConsumerAttribute!.QueueName);
        }

        [Fact]
        public void AddRabbitMqConsumer_CalledTwice_ShouldRegisterOnlyOnce()
        {
            var services = new ServiceCollection();

            services.AddRabbitMqConsumer<MultiLibA.OrderConsumer>();
            services.AddRabbitMqConsumer<MultiLibA.OrderConsumer>();

            Assert.Single(services.Where(d => d.ServiceType == typeof(IToolBoxRabbitMQ)));
        }

        [Fact]
        public void AddRabbitMQ_CalledTwice_ShouldMergeConfigsAndRegisterInfrastructureOnce()
        {
            // Arrange - 多类库各自调用 AddRabbitMQ，旧实现只保留第一次的集群配置并重复注册基础设施
            var services = new ServiceCollection();
            services.AddRabbitMQ(o =>
            {
                o.ConfigId = "cluster-a";
                o.HostName = new List<string> { "host-a" };
            }, checkIntervalSeconds: 0);
            services.AddRabbitMQ(o =>
            {
                o.ConfigId = "cluster-b";
                o.HostName = new List<string> { "host-b" };
            }, checkIntervalSeconds: 0);

            // Assert - 配置合并
            var registry = services.Single(d => d.ServiceType == typeof(RabbitMqClusterConfiguration))
                .ImplementationInstance as RabbitMqClusterConfiguration;
            Assert.NotNull(registry);
            Assert.Equal(2, registry!.Configs.Count);
            Assert.Contains(registry.Configs, c => c.ConfigId == "cluster-a");
            Assert.Contains(registry.Configs, c => c.ConfigId == "cluster-b");

            // Assert - 注册器 / Bootstrapper 只注册一次
            Assert.Single(services.Where(d => d.ServiceType == typeof(IRabbitConsumerRegister)));
            Assert.Single(services.Where(d => d.ServiceType == typeof(Bootstrapper)));
        }

        [Fact]
        public void AddRabbitMQ_CalledTwiceWithSameConfigId_ShouldThrow()
        {
            var services = new ServiceCollection();
            services.AddRabbitMQ(o =>
            {
                o.ConfigId = "same";
                o.HostName = new List<string> { "host-a" };
            }, checkIntervalSeconds: 0);

            Assert.Throws<ArgumentException>(() => services.AddRabbitMQ(o =>
            {
                o.ConfigId = "same";
                o.HostName = new List<string> { "host-b" };
            }, checkIntervalSeconds: 0));
        }
    }
}

namespace H.ToolBox.RabbitMQ.Tests.Consumer.MultiLibA
{
    public class OrderConsumer : IToolBoxRabbitMQ
    {
        [RabbitMQConsumer(QueueName = "queue.a")]
        public void Handle(string source, string routingKey) { }
    }
}

namespace H.ToolBox.RabbitMQ.Tests.Consumer.MultiLibB
{
    public class OrderConsumer : IToolBoxRabbitMQ
    {
        [RabbitMQConsumer(QueueName = "queue.b")]
        public void Handle(string source, string routingKey) { }
    }
}
