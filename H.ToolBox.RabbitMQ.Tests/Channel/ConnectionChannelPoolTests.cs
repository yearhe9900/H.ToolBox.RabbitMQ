using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace H.ToolBox.RabbitMQ.Tests.Channel;

public class ConnectionChannelPoolTests
{
    private static ConnectionChannelPool CreatePool(RabbitMQOptions options)
    {
        // 构造函数会校验 HostName 并预创建连接工厂，必须提供合法地址
        options.HostName = new List<string> { "127.0.0.1" };
        var logger = new Mock<ILogger<ConnectionChannelPool>>().Object;
        return new ConnectionChannelPool(logger, options);
    }

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

    [Fact]
    public void ConnectionCount_DefaultsToTwo_WhenNotSpecified()
    {
        var pool = CreatePool(new RabbitMQOptions());
        Assert.Equal(2, pool.ConnectionCount);
    }

    [Fact]
    public void ConnectionCount_NormalizesZeroToOne()
    {
        var pool = CreatePool(new RabbitMQOptions { ConnectionCount = 0 });
        Assert.Equal(1, pool.ConnectionCount);
    }

    [Fact]
    public void ConnectionCount_NormalizesNegativeToOne()
    {
        var pool = CreatePool(new RabbitMQOptions { ConnectionCount = -5 });
        Assert.Equal(1, pool.ConnectionCount);
    }

    [Fact]
    public void ConnectionCount_KeepsValidValue()
    {
        var pool = CreatePool(new RabbitMQOptions { ConnectionCount = 4 });
        Assert.Equal(4, pool.ConnectionCount);
    }

    [Fact]
    public void ConnectionCount_NormalizesLargeValueToCap()
    {
        var pool = CreatePool(new RabbitMQOptions { ConnectionCount = 1000 });
        Assert.Equal(64, pool.ConnectionCount);
    }
}
