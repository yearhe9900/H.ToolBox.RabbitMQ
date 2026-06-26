using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Transport;

namespace H.ToolBox.RabbitMQ.Tests.Transport;

public class TransportMessageTests
{
    [Fact]
    public void Constructor_ShouldSetBody()
    {
        // Arrange
        var body = new byte[] { 1, 2, 3 };

        // Act
        var msg = new TransportMessage(body);

        // Assert
        Assert.Equal(body, msg.Body.ToArray());
    }

    [Fact]
    public void DefaultValues_ShouldBeCorrect()
    {
        // Arrange & Act
        var msg = new TransportMessage(ReadOnlyMemory<byte>.Empty);

        // Assert
        Assert.Equal("default", msg.ConfigId);
        Assert.Equal(string.Empty, msg.Exchange);
        Assert.Equal(string.Empty, msg.RoutingKey);
        Assert.Equal(string.Empty, msg.QueueName);
        Assert.Equal(string.Empty, msg.BodyContent);
        Assert.True(msg.Durable);
        Assert.Equal(QueueType.Classic, msg.QueueType);
        Assert.Null(msg.QuorumGroupSize);
    }

    [Fact]
    public void Properties_ShouldBeSettable()
    {
        // Arrange
        var msg = new TransportMessage(new byte[] { 1 });

        // Act
        msg.ConfigId = "cluster-a";
        msg.Exchange = "my.exchange";
        msg.ExchangeType = ExchangeTypeConst.Topic;
        msg.RoutingKey = "my.routing.key";
        msg.QueueName = "my.queue";
        msg.Durable = false;
        msg.QueueType = QueueType.Quorum;
        msg.QuorumGroupSize = 3;
        msg.BodyContent = "content";

        // Assert
        Assert.Equal("cluster-a", msg.ConfigId);
        Assert.Equal("my.exchange", msg.Exchange);
        Assert.Equal(ExchangeTypeConst.Topic, msg.ExchangeType);
        Assert.Equal("my.routing.key", msg.RoutingKey);
        Assert.Equal("my.queue", msg.QueueName);
        Assert.False(msg.Durable);
        Assert.Equal(QueueType.Quorum, msg.QueueType);
        Assert.Equal(3, msg.QuorumGroupSize);
        Assert.Equal("content", msg.BodyContent);
    }

    [Fact]
    public void StructCopy_ShouldBeIndependent()
    {
        // Arrange
        var original = new TransportMessage(new byte[] { 1, 2, 3 })
        {
            QueueName = "original",
            ConfigId = "c1"
        };

        // Act
        var copy = original;
        copy.QueueName = "copy";
        copy.ConfigId = "c2";

        // Assert
        Assert.Equal("original", original.QueueName);
        Assert.Equal("c1", original.ConfigId);
        Assert.Equal("copy", copy.QueueName);
        Assert.Equal("c2", copy.ConfigId);
    }
}
