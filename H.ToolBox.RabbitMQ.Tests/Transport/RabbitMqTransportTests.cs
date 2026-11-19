using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Serializer;
using H.ToolBox.RabbitMQ.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

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
        _poolManagerMock.Setup(pm => pm.GetProducerPool(It.IsAny<string>())).Returns(_poolMock.Object);
        _poolManagerMock.Setup(pm => pm.GetOptions(It.IsAny<string>())).Returns(_options.Value);
        _poolMock.Setup(p => p.Rent(It.IsAny<CancellationToken>())).ReturnsAsync(_channelMock.Object);
        _channelMock.Setup(c => c.IsOpen).Returns(true);

        // BasicPublishAsync 返回 ValueTask（无 bool 结果）；开启 tracking 后 await 即等待 broker ack，
        // nack / basic.return 以 PublishException 抛出。显式给出完成态，避免 Moq 默认值造成误判。
        _channelMock.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
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
        _poolManagerMock.Verify(pm => pm.GetProducerPool("cluster-a"), Times.Once);
    }

    [Fact]
    public async Task SendAsync_WhenBrokerAcks_ShouldSucceed()
    {
        // Arrange - 默认 setup 返回正常完成的 ValueTask，等价于 broker ack
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.True(result.Succeeded);
        _poolMock.Verify(p => p.Return(_channelMock.Object), Times.Once);
    }

    [Fact]
    public async Task SendAsync_WhenBrokerNacks_ShouldReturnFailed()
    {
        // Arrange - nack 由客户端抛 PublishException(IsReturn=false)，绝不能再返回 Success
        // 注意：PublishException 构造函数要求 publishSequenceNumber 非 0
        _poolManagerMock.Setup(pm => pm.GetOptions(It.IsAny<string>()))
            .Returns(new RabbitMQOptions { MaxRetries = 0 });
        _channelMock.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new PublishException(1UL, isReturn: false));

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.IsType<PublisherSentFailedException>(result.Exception);
        Assert.IsType<PublishNackedException>(result.Exception!.InnerException);
        _poolMock.Verify(p => p.Return(_channelMock.Object), Times.Once);
    }

    [Fact]
    public async Task SendAsync_WhenBrokerNacks_ShouldRetryUpToMaxRetries()
    {
        // Arrange - nack 归类为瞬时错误，按 MaxRetries 重试，且每次都归还通道
        _poolManagerMock.Setup(pm => pm.GetOptions(It.IsAny<string>()))
            .Returns(new RabbitMQOptions { MaxRetries = 2 });

        var callCount = 0;
        _channelMock.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                throw new PublishException((ulong)callCount, isReturn: false);
            });

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Equal(3, callCount);
        Assert.IsType<PublishNackedException>(result.Exception!.InnerException);
        _poolMock.Verify(p => p.Rent(It.IsAny<CancellationToken>()), Times.Exactly(3));
        _poolMock.Verify(p => p.Return(_channelMock.Object), Times.Exactly(3));
    }

    [Fact]
    public async Task SendAsync_WhenMessageIsReturned_ShouldFailWithoutRetry()
    {
        // Arrange - basic.return 属于拓扑/路由错误，IsReturn=true 时禁止重试
        _poolManagerMock.Setup(pm => pm.GetOptions(It.IsAny<string>()))
            .Returns(new RabbitMQOptions { MaxRetries = 3 });

        var callCount = 0;
        _channelMock.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                throw new PublishReturnException(1UL, "NO_ROUTE", exchange: "ex", routingKey: "rk");
            });

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            Exchange = "ex",
            RoutingKey = "rk",
            Mandatory = true
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Equal(1, callCount);
        Assert.IsType<PublisherSentFailedException>(result.Exception);
        Assert.IsType<PublishReturnedException>(result.Exception!.InnerException);
        _poolMock.Verify(p => p.Rent(It.IsAny<CancellationToken>()), Times.Once);
        _poolMock.Verify(p => p.Return(_channelMock.Object), Times.Once);
    }

    [Fact]
    public async Task SendAsync_ShouldNotTrackDeliveryTagManually()
    {
        // Arrange - 发布序列号由客户端 tracking 内部维护，传输层不应手动读取/记录
        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

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
    public async Task SendAsync_OnFailure_ShouldReturnFailedImmediately()
    {
        // Arrange - InvalidOperationException 为非瞬时错误，不会重试，失败后立即返回
        var callCount = 0;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                throw new InvalidOperationException("publish failure");
            });

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "q"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.IsType<PublisherSentFailedException>(result.Exception);
        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task SendAsync_OnTransientAmqpFailure_ShouldRetryThenFail()
    {
        // Arrange - 底层 IO/网络中断（The AMQP operation was interrupted 的典型成因）属于瞬时错误，应触发重试
        // 将 MaxRetries 设为 2，预期共尝试 1（首次）+ 2（重试）= 3 次
        var retryOptions = Options.Create(new RabbitMQOptions { MaxRetries = 2 });
        _poolManagerMock.Setup(pm => pm.GetOptions(It.IsAny<string>())).Returns(retryOptions.Value);

        var callCount = 0;
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                throw new IOException("Already closed: The AMQP operation was interrupted");
            });

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 }) { QueueName = "q" };

        // Act
        var result = await transport.SendAsync(message);

        // Assert - 达到重试上限后返回失败，且每次重试都重新租借通道
        Assert.False(result.Succeeded);
        Assert.Equal(3, callCount);
        _poolMock.Verify(p => p.Rent(It.IsAny<CancellationToken>()), Times.Exactly(3));
        Assert.IsType<PublisherSentFailedException>(result.Exception);

        // 还原 GetOptions 默认行为，避免影响其它测试
        _poolManagerMock.Setup(pm => pm.GetOptions(It.IsAny<string>())).Returns(_options.Value);
    }

    [Fact]
    public async Task SendAsync_OnPublishFailure_ShouldWrapException()
    {
        // Arrange - 验证异常被包装为 PublisherSentFailedException
        _channelMock
            .Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(), It.IsAny<string>(), It.IsAny<bool>(),
                It.IsAny<BasicProperties>(), It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("persistent failure"));

        var transport = CreateTransport();
        var message = new TransportMessage(new byte[] { 1 })
        {
            QueueName = "q"
        };

        // Act
        var result = await transport.SendAsync(message);

        // Assert
        Assert.False(result.Succeeded);
        Assert.IsType<PublisherSentFailedException>(result.Exception);
        Assert.Contains("persistent failure", result.Exception!.InnerException!.Message);
    }
}
