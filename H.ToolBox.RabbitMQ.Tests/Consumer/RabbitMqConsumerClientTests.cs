using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using Moq;
using RabbitMQ.Client;

namespace H.ToolBox.RabbitMQ.Tests.Consumer;

public class RabbitMqConsumerClientTests
{
    private readonly Mock<IConnectionChannelPool> _poolMock = new();
    private readonly Mock<IConnection> _connectionMock = new();
    private readonly Mock<IChannel> _channelMock = new();

    public RabbitMqConsumerClientTests()
    {
        _poolMock.Setup(p => p.GetConnectionAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(_connectionMock.Object);
        _connectionMock.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(_channelMock.Object);
        _channelMock.Setup(c => c.IsClosed).Returns(false);
    }

    [Fact]
    public async Task CommitAsync_WhenConsumerIsNull_ShouldThrowInvalidOperation()
    {
        // Arrange - client created but ConnectAsync not called, so _consumer is null
        var client = new RabbitMqConsumerClient(_poolMock.Object);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.CommitAsync(1UL));
    }

    [Fact]
    public async Task RejectAsync_WhenConsumerIsNull_ShouldThrowInvalidOperation()
    {
        // Arrange
        var client = new RabbitMqConsumerClient(_poolMock.Object);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.RejectAsync(1UL));
    }

    [Fact]
    public async Task CommitAsync_WhenSenderIsNull_ShouldThrowInvalidOperation()
    {
        // Arrange - consumer is null before ListeningAsync, so InvalidOperationException is thrown first
        var client = new RabbitMqConsumerClient(_poolMock.Object);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.CommitAsync(null));
    }

    [Fact]
    public async Task RejectAsync_WhenSenderIsNull_ShouldThrowInvalidOperation()
    {
        // Arrange - consumer is null before ListeningAsync, so InvalidOperationException is thrown first
        var client = new RabbitMqConsumerClient(_poolMock.Object);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => client.RejectAsync(null));
    }

    [Fact]
    public void DisposeAsync_WhenChannelIsNull_ShouldNotThrow()
    {
        // Arrange - never connected
        var client = new RabbitMqConsumerClient(_poolMock.Object);

        // Act & Assert
        var exception = Record.Exception(() => client.DisposeAsync().AsTask().Wait());
        Assert.Null(exception);
    }

    [Fact]
    public async Task ConnectAsync_CalledTwice_ShouldReuseExistingChannel()
    {
        // Arrange
        var client = new RabbitMqConsumerClient(_poolMock.Object);

        // Act
        await client.ConnectAsync();
        await client.ConnectAsync();

        // Assert - connection should only be created once
        _connectionMock.Verify(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}
