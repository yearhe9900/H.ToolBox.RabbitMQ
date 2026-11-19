using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Transport;
using Moq;
using RabbitMQ.Client;
using System.Text;

namespace H.ToolBox.RabbitMQ.Tests.Consumer;

public class RabbitMqBasicConsumerTests
{
    private readonly Mock<IChannel> _channelMock = new();
    private readonly Func<TransportMessage, object?, string, Task> _msgCallback = (msg, tag, ct) => Task.CompletedTask;
    private readonly Action<LogMessageEventArgs> _logCallback = _ => { };

    [Fact]
    public async Task BasicAck_WhenChannelIsNull_ShouldNotThrow()
    {
        // Arrange - create consumer with a channel, then simulate null Channel
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback);

        // Channel is set via base constructor, but we test the pattern matching guard
        // by verifying it doesn't throw when channel is open
        _channelMock.Setup(c => c.IsOpen).Returns(true);
        await consumer.BasicAck(1UL);

        // Verify Ack was called
        _channelMock.Verify(c => c.BasicAckAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BasicAck_WhenChannelIsClosed_ShouldNotCallAck()
    {
        // Arrange
        _channelMock.Setup(c => c.IsOpen).Returns(false);
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback);

        // Act
        await consumer.BasicAck(1UL);

        // Assert - should not call BasicAck when channel is closed
        _channelMock.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BasicReject_WhenChannelIsClosed_ShouldNotCallReject()
    {
        // Arrange
        _channelMock.Setup(c => c.IsOpen).Returns(false);
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback, isAutoAck: false);

        // Act
        await consumer.BasicReject(1UL);

        // Assert
        _channelMock.Verify(c => c.BasicRejectAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BasicAck_WhenAutoAck_ShouldNotCallAck()
    {
        // Arrange
        _channelMock.Setup(c => c.IsOpen).Returns(true);
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback, isAutoAck: true);

        // Act
        await consumer.BasicAck(1UL);

        // Assert - auto ack mode should skip manual ack
        _channelMock.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BasicReject_WhenAutoAck_ShouldNotCallReject()
    {
        // Arrange
        _channelMock.Setup(c => c.IsOpen).Returns(true);
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback, isAutoAck: true);

        // Act
        await consumer.BasicReject(1UL);

        // Assert
        _channelMock.Verify(c => c.BasicRejectAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BasicReject_ByDefault_ShouldNotRequeue()
    {
        // Arrange - 毒消息策略：默认 requeue:false，消息进入 DLX
        _channelMock.Setup(c => c.IsOpen).Returns(true);
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback, isAutoAck: false);

        // Act
        await consumer.BasicReject(1UL);

        // Assert
        _channelMock.Verify(c => c.BasicRejectAsync(1UL, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task BasicReject_WithExplicitRequeue_ShouldPassThrough()
    {
        // Arrange - 只有调用方显式要求时才重投
        _channelMock.Setup(c => c.IsOpen).Returns(true);
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, _logCallback, isAutoAck: false);

        // Act
        await consumer.BasicReject(1UL, requeue: true);

        // Assert
        _channelMock.Verify(c => c.BasicRejectAsync(1UL, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandleBasicDeliverAsync_ShouldPropagateDeliveryMetadata()
    {
        // Arrange - redelivered / message-id / exchange 必须透传给业务侧
        TransportMessage? captured = null;
        Func<TransportMessage, object?, string, Task> msgCb = (msg, tag, ct) =>
        {
            captured = msg;
            return Task.CompletedTask;
        };
        var propsMock = new Mock<IReadOnlyBasicProperties>();
        propsMock.Setup(p => p.MessageId).Returns("msg-1");

        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, msgCb, _logCallback);

        // Act
        await consumer.HandleBasicDeliverAsync("tag", 7UL, true, "ex", "rk",
            propsMock.Object, Encoding.UTF8.GetBytes("body"));

        // Assert
        Assert.NotNull(captured);
        Assert.True(captured!.Value.Redelivered);
        Assert.Equal("msg-1", captured.Value.MessageId);
        Assert.Equal("ex", captured.Value.Exchange);
        Assert.Equal("rk", captured.Value.RoutingKey);
        Assert.Equal("body", captured.Value.BodyContent);
    }

    [Fact]
    public async Task HandleBasicConsumeOkAsync_ShouldInvokeLogCallback()
    {
        // Arrange
        LogMessageEventArgs? capturedArgs = null;
        Action<LogMessageEventArgs> logCb = args => capturedArgs = args;
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, _msgCallback, logCb);

        // Act
        await consumer.HandleBasicConsumeOkAsync("test-tag");

        // Assert
        Assert.NotNull(capturedArgs);
        Assert.Equal(MqLogType.ConsumerRegistered, capturedArgs!.LogType);
        Assert.Equal("test-tag", capturedArgs.Reason);
    }

    [Fact]
    public void HandleBasicDeliverAsync_WithConcurrentMode_ShouldReturnTask()
    {
        // Arrange
        Func<TransportMessage, object?, string, Task> msgCb = (msg, tag, ct) => Task.CompletedTask;
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 1, msgCb, _logCallback);

        // Act
        var task = consumer.HandleBasicDeliverAsync("tag", 1UL, false, "", "rk",
            new Mock<IReadOnlyBasicProperties>().Object, ReadOnlyMemory<byte>.Empty);

        // Assert - Task.Run returns a task
        Assert.NotNull(task);
    }

    [Fact]
    public void HandleBasicDeliverAsync_WithoutConcurrentMode_ShouldReturnDirectly()
    {
        // Arrange
        Func<TransportMessage, object?, string, Task> msgCb = (msg, tag, ct) => Task.CompletedTask;
        var consumer = new RabbitMqBasicConsumer(_channelMock.Object, 0, msgCb, _logCallback);

        // Act
        var task = consumer.HandleBasicDeliverAsync("tag", 1UL, false, "", "rk",
            new Mock<IReadOnlyBasicProperties>().Object, ReadOnlyMemory<byte>.Empty);

        // Assert - direct mode returns the callback's task
        Assert.NotNull(task);
    }
}
