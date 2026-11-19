using H.ToolBox.RabbitMQ.Model;
using Xunit;

namespace H.ToolBox.RabbitMQ.Tests.Model;

public class DeadLetterTopologyTests
{
    [Fact]
    public void GetDeadLetterQueueName_ShouldAppendSuffix()
    {
        var options = new RabbitMQOptions();

        Assert.Equal("order.queue.dlq", DeadLetterTopology.GetDeadLetterQueueName("order.queue", options));
    }

    [Fact]
    public void BuildQueueArguments_WhenEnabled_ShouldContainDlxAndRoutingKey()
    {
        var options = new RabbitMQOptions();
        var arguments = DeadLetterTopology.BuildQueueArguments("order.queue", QueueType.Classic, null, options);

        Assert.NotNull(arguments);
        Assert.Equal(options.DeadLetterExchange, arguments!["x-dead-letter-exchange"]);
        Assert.Equal("order.queue.dlq", arguments["x-dead-letter-routing-key"]);
    }

    [Fact]
    public void BuildQueueArguments_WhenDisabled_ShouldNotContainDlx()
    {
        var options = new RabbitMQOptions { EnableDeadLetter = false };

        Assert.Null(DeadLetterTopology.BuildQueueArguments("order.queue", QueueType.Classic, null, options));

        var quorumArgs = DeadLetterTopology.BuildQueueArguments("order.queue", QueueType.Quorum, 3, options);
        Assert.NotNull(quorumArgs);
        Assert.False(quorumArgs!.ContainsKey("x-dead-letter-exchange"));
        Assert.Equal("quorum", quorumArgs["x-queue-type"]);
    }

    [Fact]
    public void BuildQueueArguments_Quorum_ShouldKeepQueueTypeAndAddDlx()
    {
        var options = new RabbitMQOptions();
        var arguments = DeadLetterTopology.BuildQueueArguments("order.queue", QueueType.Quorum, 3, options);

        Assert.NotNull(arguments);
        Assert.Equal("quorum", arguments!["x-queue-type"]);
        Assert.Equal(3, arguments["x-quorum-initial-group-size"]);
        Assert.Equal(options.DeadLetterExchange, arguments["x-dead-letter-exchange"]);
    }

    [Fact]
    public void BuildQueueArguments_ProducerAndConsumer_ShouldBeIdentical()
    {
        // 生产者与消费者声明同一队列时 arguments 必须完全一致，
        // 否则 RabbitMQ 返回 PRECONDITION_FAILED 并关闭通道。
        var options = new RabbitMQOptions();

        var producerArgs = DeadLetterTopology.BuildQueueArguments("order.queue", QueueType.Classic, null, options);
        var consumerArgs = DeadLetterTopology.BuildQueueArguments("order.queue", QueueType.Classic, null, options);

        Assert.NotNull(producerArgs);
        Assert.NotNull(consumerArgs);
        Assert.Equal(producerArgs!.Count, consumerArgs!.Count);
        foreach (var kv in producerArgs)
        {
            Assert.True(consumerArgs.TryGetValue(kv.Key, out var value));
            Assert.Equal(kv.Value, value);
        }
    }

    [Fact]
    public void BuildQueueArguments_WhenDisabled_ShouldMatchLegacyHelper()
    {
        var options = new RabbitMQOptions { EnableDeadLetter = false };

        var legacy = QueueArgumentsHelper.BuildArguments(QueueType.Classic);
        var actual = DeadLetterTopology.BuildQueueArguments("q", QueueType.Classic, null, options);
        Assert.Equal(legacy, actual);
    }
}
