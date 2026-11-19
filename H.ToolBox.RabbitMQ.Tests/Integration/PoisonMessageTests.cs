using H.ToolBox.RabbitMQ.Model;
using System.Text;
using Xunit;

namespace H.ToolBox.RabbitMQ.Tests.Integration;

/// <summary>
/// 真实 Broker 下的毒消息回归：requeue:false 拒绝后消息必须落到死信队列，
/// 且不再回到业务队列（旧实现 requeue:true 会造成无限循环）。
/// 本机无 Docker 时由 DockerFactAttribute 自动跳过。
/// </summary>
[Collection(RabbitMqIntegrationCollection.Name)]
public sealed class PoisonMessageTests
{
    private readonly RabbitMqFixture _fixture;

    public PoisonMessageTests(RabbitMqFixture fixture)
    {
        _fixture = fixture;
    }

    [DockerFact]
    public async Task RejectWithoutRequeue_ShouldRouteMessageToDeadLetterQueue()
    {
        // Arrange - 用库内真实的拓扑构建器声明业务队列，确保测试覆盖生产参数
        await using var harness = await ProducerConsumerHarness.CreateAsync(_fixture);
        var options = new RabbitMQOptions();
        var queue = $"poison.test.{Guid.NewGuid():N}";
        var dlq = DeadLetterTopology.GetDeadLetterQueueName(queue, options);

        await harness.DeclareDirectExchangeAsync(options.DeadLetterExchange);
        await harness.DeclareQueueAsync(dlq);
        await harness.BindQueueAsync(dlq, options.DeadLetterExchange, dlq);
        await harness.DeclareQueueAsync(
            queue,
            DeadLetterTopology.BuildQueueArguments(queue, QueueType.Classic, null, options));

        // Act - 发布一条消息，取回但不确认，然后以 requeue:false 拒绝
        await harness.PublishAsync(string.Empty, queue, "poison-body");

        var delivered = await harness.WaitForUnackedMessageAsync(queue, TimeSpan.FromSeconds(10));
        Assert.NotNull(delivered);
        await harness.RejectAsync(delivered!.DeliveryTag, requeue: false);

        // Assert - 消息进入死信队列，且业务队列不再有该消息
        var dead = await harness.WaitForMessageAsync(dlq, TimeSpan.FromSeconds(10));
        Assert.NotNull(dead);
        Assert.Equal("poison-body", Encoding.UTF8.GetString(dead!.Body.Span));
        Assert.Null(await harness.GetAsync(queue));
    }
}
