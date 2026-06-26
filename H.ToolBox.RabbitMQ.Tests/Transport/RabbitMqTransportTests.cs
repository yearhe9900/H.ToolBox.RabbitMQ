using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using H.ToolBox.RabbitMQ.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace H.ToolBox.RabbitMQ.Tests.Transport;

public class RabbitMqTransportTests
{
    private readonly Mock<IConnectionChannelPoolManager> _poolManagerMock = new();
    private readonly Mock<IConnectionChannelPool> _poolMock = new();
    private readonly Mock<ILogger<RabbitMqTransport>> _loggerMock = new();
    private readonly IMessageSerializer _serializer = new SystemTextJsonMessageSerializer();
    private readonly Mock<IChannel> _channelMock = new();
    private readonly IOptions<RabbitMQOptions> _options = Options.Create(new RabbitMQOptions());

    public RabbitMqTransportTests()
    {
        _poolManagerMock.Setup(pm => pm.GetPool(It.IsAny<string>())).Returns(_poolMock.Object);
        _poolMock.Setup(p => p.Rent(It.IsAny<CancellationToken>())).ReturnsAsync(_channelMock.Object);
        _poolMock.Setup(p => p.IsPublishConfirms).Returns(false);
        _channelMock.Setup(c => c.IsOpen).Returns(true);
    }

    private RabbitMqTransport CreateTransport() =>
        new(_loggerMock.Object, _poolManagerMock.Object, _serializer, _options);

    [Fact]
    public async Task SendAsync_WithEmptyQueueAndExchange_ShouldReturnFailed()
    {
        // Arrange
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 });

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.NotNull(result.Exception);
    }

    [Fact]
    public async Task SendAsync_WithEmptyBody_ShouldReturnFailed()
    {
        // Arrange
        var transport = CreateTransport();
        var message = new TransportMessage(ReadOnlyMemory<byte>.Empty)
        {
            QueueName = "test.queue"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
    }

    [Fact]
    public async Task SendAsync_WithCancelledToken_ShouldThrow()
    {
        // Arrange
        var transport = CreateTransport();
        var cts = new CancellationTokenSource();
        cts.Cancel();
        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(
            () => transport.SendAsync(message, cts.Token));
    }

    [Fact]
    public async Task SendAsync_QueueMode_ShouldRentAndReturnChannel()
    {
        // Arrange
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1, 2, 3 })
        {
            QueueName = "test.queue"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        _poolMock.Verify(p => p.Rent(It.IsAny<CancellationToken>()), Times.Once);
        _poolMock.Verify(p => p.Return(_channelMock.Object), Times.Once);
    }

    [Fact]
    public async Task SendAsync_ExchangeMode_ShouldRentAndReturnChannel()
    {
        // Arrange
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            Exchange = "test.exchange",
            RoutingKey = "test.key"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        _poolMock.Verify(p => p.Rent(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SendAsync_QueueMode_WithoutRoutingKey_ShouldSucceed()
    {
        // Arrange - RoutingKey 为空时，SendAsync 内部会用 QueueName 作为 RoutingKey
        // 注意：TransportMessage 是 struct，SendAsync 修改的是副本，不影响原始变量
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "my.queue",
            RoutingKey = ""
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public async Task SendAsync_WhenChannelThrows_ShouldReturnFailedAndReturnChannel()
    {
        // Arrange
        var transport = CreateTransport();
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("publish failed"));

        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.IsType<PublisherSentFailedException>(result.Exception);
        _poolMock.Verify(p => p.Return(_channelMock.Object), Times.Once);
    }

    [Fact]
    public async Task SendAsync_Generic_QueueMode_ShouldSerializeAndSend()
    {
        // Arrange
        var transport = CreateTransport();
        var msg = new { Name = "test", Value = 42 };

        // Act
        var result = await transport.SendAsync("default", "test.queue", msg);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public async Task SendAsync_Generic_ExchangeMode_ShouldSerializeAndSend()
    {
        // Arrange
        var transport = CreateTransport();
        var msg = new { Name = "test" };

        // Act
        var result = await transport.SendAsync("default", "test.exchange", "test.key", msg);

        // Assert
        Assert.True(result.Succeeded);
    }

    [Fact]
    public async Task SendAsync_ShouldGetPoolByConfigId()
    {
        // Arrange
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            ConfigId = "cluster-a",
            QueueName = "q"
        };

        // Act
        await transport.SendAsync(message);

        // Assert
        _poolManagerMock.Verify(pm => pm.GetPool("cluster-a"), Times.Once);
    }

    [Fact]
    public async Task SendAsync_WithPublishConfirms_ShouldTrackDeliveryTag()
    {
        // Arrange - 7.x 移除了 WaitForConfirmsAsync，改用 BasicAcksAsync/BasicNacksAsync 事件追踪
        _poolMock.Setup(p => p.IsPublishConfirms).Returns(true);
        _channelMock.Setup(c => c.GetNextPublishSequenceNumberAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(1UL);

        // 捕获注册的 ack 事件处理器，在 BasicPublishAsync 时触发以模拟 Broker 确认
        AsyncEventHandler<BasicAckEventArgs>? ackHandler = null;
        _channelMock.SetupAdd(c => c.BasicAcksAsync += It.IsAny<AsyncEventHandler<BasicAckEventArgs>>())
            .Callback<AsyncEventHandler<BasicAckEventArgs>>(handler => ackHandler = handler);
        _channelMock.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => ackHandler?.Invoke(null!, new BasicAckEventArgs(1, false)).GetAwaiter().GetResult())
            .Returns(ValueTask.CompletedTask);

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "q"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        _channelMock.Verify(c => c.GetNextPublishSequenceNumberAsync(It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SendAsync_WithoutPublishConfirms_ShouldNotTrackDeliveryTag()
    {
        // Arrange
        _poolMock.Setup(p => p.IsPublishConfirms).Returns(false);
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "q"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        _channelMock.Verify(c => c.GetNextPublishSequenceNumberAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SendAsync_WithMandatory_ShouldPassMandatoryToPublish()
    {
        // Arrange
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            Exchange = "test.exchange",
            RoutingKey = "test.key",
            Mandatory = true
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        _channelMock.Verify(c => c.BasicPublishAsync(
            "test.exchange", "test.key", true,
            It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SendAsync_WithRetry_ShouldRetryOnFailure()
    {
        // Arrange
        var callCount = 0;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount <= 1)
                    throw new InvalidOperationException("transient failure");
                return ValueTask.CompletedTask;
            });

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "q",
            MaxRetries = 2
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        Assert.Equal(2, callCount);
    }

    [Fact]
    public async Task SendAsync_WithRetryExhausted_ShouldReturnFailed()
    {
        // Arrange
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("persistent failure"));

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "q",
            MaxRetries = 1
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.IsType<PublisherSentFailedException>(result.Exception);
    }
}
