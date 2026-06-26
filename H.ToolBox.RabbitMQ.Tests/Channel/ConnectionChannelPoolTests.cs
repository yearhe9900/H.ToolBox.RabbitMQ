using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;

namespace H.ToolBox.RabbitMQ.Tests.Channel;

public class ConnectionChannelPoolTests
{
    [Fact]
    public void Constructor_WithValidOptions_ShouldNotThrow()
    {
        // Arrange
        var logger = new Mock<ILogger<ConnectionChannelPool>>().Object;
        var options = new RabbitMQOptions
        {
            PoolSize = 5,
            AbsoluteMaxPoolSize = 10,
            MinPoolSize = 1,
            IdleTimeout = TimeSpan.FromMinutes(1),
            PublishConfirms = true,
            HostName = new List<string> { "localhost" },
            Port = 5672,
            UserName = "guest",
            Password = "guest"
        };

        // Act & Assert - constructor should not throw
        var pool = new ConnectionChannelPool(logger, options);
        Assert.NotNull(pool);
        pool.Dispose();
    }

    [Fact]
    public void Constructor_WithZeroPoolSize_ShouldUseDefault()
    {
        // Arrange
        var logger = new Mock<ILogger<ConnectionChannelPool>>().Object;
        var options = new RabbitMQOptions
        {
            PoolSize = 0,
            AbsoluteMaxPoolSize = 0,
            HostName = new List<string> { "localhost" }
        };

        // Act & Assert
        var pool = new ConnectionChannelPool(logger, options);
        Assert.NotNull(pool);
        pool.Dispose();
    }

    [Fact]
    public void Dispose_CalledTwice_ShouldNotThrow()
    {
        // Arrange
        var logger = new Mock<ILogger<ConnectionChannelPool>>().Object;
        var options = new RabbitMQOptions
        {
            PoolSize = 5,
            AbsoluteMaxPoolSize = 10,
            HostName = new List<string> { "localhost" }
        };
        var pool = new ConnectionChannelPool(logger, options);

        // Act & Assert
        pool.Dispose();
        pool.Dispose(); // second dispose should be safe
    }
}
