using Testcontainers.RabbitMq;

namespace H.ToolBox.RabbitMQ.Tests.Integration;

/// <summary>
/// 集成测试集合定义。所有真实 Broker 用例共享同一个容器实例，避免每个测试重复拉起 RabbitMQ。
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class RabbitMqIntegrationCollection : ICollectionFixture<RabbitMqFixture>
{
    public const string Name = "RabbitMqIntegration";
}

/// <summary>
/// 真实 RabbitMQ 容器 fixture。
/// 注意：xUnit 可能在所有测试都被 Skip 时仍然构造 fixture，因此 InitializeAsync 做了短路，
/// 保证没有 Docker 的机器上不会因为 fixture 初始化而抛异常。
/// </summary>
public sealed class RabbitMqFixture : IAsyncLifetime
{
    private const int AmqpPort = 5672;

    // 显式指定镜像版本：Testcontainers 4.x 已弃用无参构造，且固定 tag 可避免上游默认镜像漂移。
    private readonly RabbitMqContainer _container =
        new RabbitMqBuilder("rabbitmq:3.13-management-alpine").Build();

    public string HostName => _container.Hostname;

    public int Port => _container.GetMappedPublicPort(AmqpPort);

    public string UserName => "guest";

    public string Password => "guest";

    public string VirtualHost => "/";

    public Task InitializeAsync()
    {
        if (!DockerEnvironment.IsAvailable)
        {
            return Task.CompletedTask;
        }

        return _container.StartAsync();
    }

    public Task DisposeAsync()
    {
        if (!DockerEnvironment.IsAvailable)
        {
            return Task.CompletedTask;
        }

        return _container.DisposeAsync().AsTask();
    }
}
