using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using Moq;
using System.Reflection;

namespace H.ToolBox.RabbitMQ.Tests.Model;

public class MethodMatcherCacheTests
{
    private readonly Mock<IConsumerServiceSelector> _selectorMock = new();

    [Fact]
    public void TrySetExecutor_ShouldPopulateCache()
    {
        // Arrange
        var scanResult = CreateScanResult("consumer.1", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        var entries = cache.TrySetExecutor();

        // Assert
        Assert.Single(entries);
        Assert.True(entries.ContainsKey("consumer.1"));
    }

    [Fact]
    public void TryGetExecutor_WithExistingName_ShouldReturnTrue()
    {
        // Arrange
        var scanResult = CreateScanResult("consumer.1", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);
        cache.TrySetExecutor();

        // Act
        var found = cache.TryGetExecutor("consumer.1", out var result);

        // Assert
        Assert.True(found);
        Assert.NotNull(result);
        Assert.Equal("consumer.1", result!.ConsumerAttribute!.ConsumerName);
    }

    [Fact]
    public void TryGetExecutor_WithNonExistingName_ShouldReturnFalse()
    {
        // Arrange
        var scanResult = CreateScanResult("consumer.1", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);
        cache.TrySetExecutor();

        // Act
        var found = cache.TryGetExecutor("nonexistent", out var result);

        // Assert
        Assert.False(found);
        Assert.Null(result);
    }

    [Fact]
    public void TrySetExecutor_WithConfigIdFilter_ShouldReturnOnlyMatching()
    {
        // Arrange
        var result1 = CreateScanResult("consumer.1", "queue.1", configId: "cluster-a");
        var result2 = CreateScanResult("consumer.2", "queue.2", configId: "cluster-b");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { result1, result2 });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        var entries = cache.TrySetExecutor("cluster-a");

        // Assert
        Assert.Single(entries);
        Assert.True(entries.ContainsKey("consumer.1"));
    }

    [Fact]
    public void TrySetExecutor_ShouldSkipInvalidEntries()
    {
        // Arrange
        var invalid = new MethodScanResult { MethodInfo = null, ConsumerAttribute = null };
        var valid = CreateScanResult("consumer.1", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { invalid, valid });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        var entries = cache.TrySetExecutor();

        // Assert
        Assert.Single(entries);
    }

    [Fact]
    public void TrySetExecutor_EmptyConsumerName_ShouldAutoGenerate()
    {
        // Arrange - ConsumerName 为空时自动生成 "类名.方法名" 格式的稳定名称
        var scanResult = CreateScanResult("", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        var entries = cache.TrySetExecutor();

        // Assert
        Assert.Single(entries);
        Assert.True(cache.TryGetExecutor("FakeConsumer.Handle", out _));
    }

    [Fact]
    public void TrySetExecutor_CalledTwice_ShouldUpdateExisting()
    {
        // Arrange
        var scanResult = CreateScanResult("consumer.1", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        cache.TrySetExecutor();
        cache.TrySetExecutor();

        // Assert - 索引器会更新已有条目，仍为单条
        Assert.True(cache.TryGetExecutor("consumer.1", out _));
    }

    private static MethodScanResult CreateScanResult(string consumerName, string queueName, string configId = "default")
    {
        var attr = new RabbitMQConsumerAttribute
        {
            ConsumerName = consumerName,
            QueueName = queueName,
            ConfigId = configId
        };

        var method = typeof(FakeConsumer).GetMethod(nameof(FakeConsumer.Handle))!;

        return new MethodScanResult
        {
            Instance = new FakeConsumer(),
            MethodInfo = method,
            ConsumerAttribute = attr
        };
    }

    private class FakeConsumer : IToolBoxRabbitMQ
    {
        public void Handle(string source, string routingKey) { }
    }
}
