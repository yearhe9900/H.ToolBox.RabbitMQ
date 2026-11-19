using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
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

    [Fact]
    public void BuildChannelOptions_WithPublishConfirms_ShouldEnableTracking()
    {
        // 回归：tracking 被关闭时 BasicPublishAsync 不等待 broker 确认，发布确认形同虚设
        var options = ConnectionChannelPool.BuildChannelOptions(publishConfirms: true);

        Assert.True(options.PublisherConfirmationsEnabled);
        Assert.True(options.PublisherConfirmationTrackingEnabled);
    }

    [Fact]
    public void BuildChannelOptions_WithoutPublishConfirms_ShouldDisableBoth()
    {
        var options = ConnectionChannelPool.BuildChannelOptions(publishConfirms: false);

        Assert.False(options.PublisherConfirmationsEnabled);
        Assert.False(options.PublisherConfirmationTrackingEnabled);
    }

    /// <summary>
    /// 构造一个使用 mock 连接工厂、不启动定时扫描的连接池，便于精确验证信号量不变量。
    /// </summary>
    private static (ConnectionChannelPool Pool, Mock<IChannel> ChannelMock) CreateTestPool()
    {
        var options = new RabbitMQOptions
        {
            PoolSize = 5,
            AbsoluteMaxPoolSize = 10,
            MinPoolSize = 0,
            ConnectionCount = 1,
            IdleTimeout = TimeSpan.Zero, // 使所有归还通道在扫描时都视为空闲，测试无需等待
            PublishConfirms = false,
            HostName = new List<string> { "127.0.0.1" }
        };

        var channelMock = new Mock<IChannel>();
        channelMock.SetupGet(c => c.IsOpen).Returns(true);

        var connectionMock = new Mock<IConnection>();
        connectionMock.SetupGet(c => c.IsOpen).Returns(true);
        connectionMock
            .Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channelMock.Object);

        var logger = new Mock<ILogger<ConnectionChannelPool>>().Object;
        // 通过内部构造函数注入连接工厂并关闭定时扫描，测试可用 SweepIdleChannelsNow 显式触发
        var pool = new ConnectionChannelPool(logger, options, () => Task.FromResult(connectionMock.Object), startSweepTimer: false);
        return (pool, channelMock);
    }

    [Fact]
    public async Task SweepIdleChannels_ShouldNotInflateSemaphorePermits()
    {
        // 回归：旧实现在回收空闲通道时又 Release() 一次许可（归还时已 Release），
        // 导致许可数随每次扫描单调递增，最终 SemaphoreFullException 且池容量上限失效。
        var (pool, channelMock) = CreateTestPool();
        var iface = (IConnectionChannelPool)pool;
        var initialPermits = pool.AvailablePermits;

        try
        {
            // 租借 3 个通道，再全部归还，使池中有 3 个空闲通道、许可数回到初始值
            var rented = new List<IChannel>();
            for (var i = 0; i < 3; i++)
            {
                rented.Add(await iface.Rent());
            }
            Assert.Equal(initialPermits - 3, pool.AvailablePermits);

            foreach (var channel in rented)
            {
                Assert.True(pool.Return(channel));
            }
            Assert.Equal(initialPermits, pool.AvailablePermits);
            Assert.Equal(3, pool.PooledChannelCount);

            // 回收空闲通道：物理通道被销毁，但许可数必须保持不变
            var disposed = pool.SweepIdleChannelsNow();
            Assert.Equal(3, disposed);
            Assert.Equal(0, pool.PooledChannelCount);
            Assert.Equal(initialPermits, pool.AvailablePermits);

            // 再次扫描（池已空）仍不应改变许可数
            Assert.Equal(0, pool.SweepIdleChannelsNow());
            Assert.Equal(initialPermits, pool.AvailablePermits);

            channelMock.Verify(c => c.Dispose(), Times.Exactly(3));
        }
        finally
        {
            pool.Dispose();
        }
    }

    [Fact]
    public async Task SweepIdleChannels_ThenRent_ShouldStillRespectCapacity()
    {
        // 回收后池为空，后续 Rent 应能正常新建通道并正确扣减许可，而非因许可膨胀绕过容量上限
        var (pool, _) = CreateTestPool();
        var iface = (IConnectionChannelPool)pool;
        var initialPermits = pool.AvailablePermits;

        try
        {
            var channel = await iface.Rent();
            Assert.True(pool.Return(channel));
            Assert.Equal(initialPermits, pool.AvailablePermits);

            pool.SweepIdleChannelsNow();
            Assert.Equal(initialPermits, pool.AvailablePermits);

            // 回收后再租借，许可应正常扣减
            var channel2 = await iface.Rent();
            Assert.Equal(initialPermits - 1, pool.AvailablePermits);
            Assert.True(pool.Return(channel2));
            Assert.Equal(initialPermits, pool.AvailablePermits);
        }
        finally
        {
            pool.Dispose();
        }
    }
}
