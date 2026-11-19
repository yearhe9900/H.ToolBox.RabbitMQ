using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Moq;

namespace H.ToolBox.RabbitMQ.Tests.Channel;

public class ConnectionChannelPoolManagerTests
{
    private readonly Mock<ILogger<ConnectionChannelPool>> _loggerMock = new();

    private static RabbitMQOptions CreateOptions(string configId = "default")
    {
        return new RabbitMQOptions
        {
            ConfigId = configId,
            HostName = new List<string> { "localhost" },
            Port = 5672,
            UserName = "guest",
            Password = "guest"
        };
    }

    [Fact]
    public void RegisterPool_WithValidConfigId_ShouldSucceed()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        var options = CreateOptions();

        // Act
        var pool = manager.RegisterPool("default", options);

        // Assert
        Assert.NotNull(pool);
    }

    [Fact]
    public void RegisterPool_WithDuplicateConfigId_ShouldThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        var options = CreateOptions();
        manager.RegisterPool("default", options);

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => manager.RegisterPool("default", options));
    }

    [Fact]
    public void RegisterPool_WithEmptyConfigId_ShouldThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);

        // Act & Assert
        Assert.Throws<ArgumentException>(() => manager.RegisterPool("", CreateOptions()));
    }

    [Fact]
    public void GetPool_WithRegisteredConfigId_ShouldReturnPool()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var pool = manager.GetPool("cluster1");

        // Assert
        Assert.NotNull(pool);
    }

    [Fact]
    public void GetPool_WithUnregisteredConfigId_ShouldThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);

        // Act & Assert
        Assert.Throws<KeyNotFoundException>(() => manager.GetPool("nonexistent"));
    }

    [Fact]
    public void GetOptions_WithRegisteredConfigId_ShouldReturnOptions()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        var options = CreateOptions("cluster1");
        manager.RegisterPool("cluster1", options);

        // Act
        var result = manager.GetOptions("cluster1");

        // Assert
        Assert.Same(options, result);
    }

    [Fact]
    public void GetOptions_WithUnregisteredConfigId_ShouldThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);

        // Act & Assert
        Assert.Throws<KeyNotFoundException>(() => manager.GetOptions("nonexistent"));
    }

    [Fact]
    public void Dispose_ShouldDisposeAllPools()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("c1", CreateOptions("c1"));
        manager.RegisterPool("c2", CreateOptions("c2"));

        // Act
        manager.Dispose();

        // Assert - getting pool after dispose should throw (pools cleared)
        Assert.Throws<KeyNotFoundException>(() => manager.GetPool("c1"));
    }

    [Fact]
    public void Dispose_CalledTwice_ShouldNotThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("c1", CreateOptions("c1"));

        // Act & Assert
        manager.Dispose();
        var exception = Record.Exception(() => manager.Dispose());
        Assert.Null(exception);
    }

    [Fact]
    public void RegisterPool_MultipleClusters_ShouldIsolatePools()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);

        // Act
        manager.RegisterPool("c1", CreateOptions("c1"));
        manager.RegisterPool("c2", CreateOptions("c2"));

        // Assert
        var pool1 = manager.GetPool("c1");
        var pool2 = manager.GetPool("c2");
        Assert.NotSame(pool1, pool2);
    }

    [Fact]
    public void GetProducerPool_WithRegisteredConfigId_ShouldReturnProducerPool()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var producerPool = manager.GetProducerPool("cluster1");

        // Assert
        Assert.NotNull(producerPool);
    }

    [Fact]
    public void GetProducerPool_WithUnregisteredConfigId_ShouldThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);

        // Act & Assert
        Assert.Throws<KeyNotFoundException>(() => manager.GetProducerPool("nonexistent"));
    }

    [Fact]
    public void GetProducerPool_ShouldReturnDifferentInstanceFromGetPool()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var consumerPool = manager.GetPool("cluster1");
        var producerPool = manager.GetProducerPool("cluster1");

        // Assert - producer and consumer pools must be different instances (separate TCP connections)
        Assert.NotSame(consumerPool, producerPool);
    }

    [Fact]
    public void GetProducerPool_CalledTwice_ShouldReturnSameInstance()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var pool1 = manager.GetProducerPool("cluster1");
        var pool2 = manager.GetProducerPool("cluster1");

        // Assert - producer pool should be stable per ConfigId
        Assert.Same(pool1, pool2);
    }

    [Fact]
    public void GetConsumerServicePool_WithRegisteredConfigId_ShouldReturnConsumerServicePool()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var consumerServicePool = manager.GetConsumerServicePool("cluster1");

        // Assert
        Assert.NotNull(consumerServicePool);
    }

    [Fact]
    public void GetConsumerServicePool_WithUnregisteredConfigId_ShouldThrow()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);

        // Act & Assert
        Assert.Throws<KeyNotFoundException>(() => manager.GetConsumerServicePool("nonexistent"));
    }

    [Fact]
    public void GetConsumerServicePool_ShouldReturnDifferentInstanceFromGetPool()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var consumerPool = manager.GetPool("cluster1");
        var consumerServicePool = manager.GetConsumerServicePool("cluster1");

        // Assert - ConsumerService pool must be different instance from consumer pool
        Assert.NotSame(consumerPool, consumerServicePool);
    }

    [Fact]
    public void GetConsumerServicePool_ShouldReturnDifferentInstanceFromGetProducerPool()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var producerPool = manager.GetProducerPool("cluster1");
        var consumerServicePool = manager.GetConsumerServicePool("cluster1");

        // Assert - ConsumerService pool must be different instance from producer pool
        Assert.NotSame(producerPool, consumerServicePool);
    }

    [Fact]
    public void GetConsumerServicePool_CalledTwice_ShouldReturnSameInstance()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var pool1 = manager.GetConsumerServicePool("cluster1");
        var pool2 = manager.GetConsumerServicePool("cluster1");

        // Assert - ConsumerService pool should be stable per ConfigId
        Assert.Same(pool1, pool2);
    }

    [Fact]
    public void RegisterPool_ShouldCreateThreeSeparatePools()
    {
        // Arrange
        var manager = new ConnectionChannelPoolManager(_loggerMock.Object);
        manager.RegisterPool("cluster1", CreateOptions("cluster1"));

        // Act
        var consumerPool = manager.GetPool("cluster1");
        var producerPool = manager.GetProducerPool("cluster1");
        var consumerServicePool = manager.GetConsumerServicePool("cluster1");

        // Assert - all three pools should be different instances
        Assert.NotSame(consumerPool, producerPool);
        Assert.NotSame(consumerPool, consumerServicePool);
        Assert.NotSame(producerPool, consumerServicePool);
    }
}
