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
    public void TrySetExecutor_EmptyConsumerName_ShouldAutoGenerateFullName()
    {
        // Arrange - 自动生成名必须含命名空间，否则不同类库的同名类会撞名
        var scanResult = CreateScanResult("", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        var entries = cache.TrySetExecutor();

        // Assert
        var expected = $"{typeof(FakeConsumer).FullName}.{nameof(FakeConsumer.Handle)}";
        Assert.Single(entries);
        Assert.Equal(expected, Assert.Single(entries).Key);
        Assert.True(cache.TryGetExecutor(expected, out _));
    }

    [Fact]
    public void TrySetExecutor_WithDuplicateConsumerName_ShouldFailFast()
    {
        // Arrange - 两个不同方法显式使用同一 ConsumerName，旧实现会静默覆盖
        var first = CreateScanResult("shared.name", "queue.1");
        var second = CreateScanResult("shared.name", "queue.2");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { first, second });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act & Assert
        var ex = Assert.Throws<InvalidOperationException>(() => cache.TrySetExecutor());
        Assert.Contains("shared.name", ex.Message);
    }

    [Fact]
    public void TrySetExecutor_CalledTwice_ShouldClearStaleEntries()
    {
        // Arrange - 第一次有两个消费者，第二次扫描只剩一个
        var first = CreateScanResult("consumer.1", "queue.1");
        var second = CreateScanResult("consumer.2", "queue.2");
        var cache = new MethodMatcherCache(_selectorMock.Object);

        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { first, second });
        cache.TrySetExecutor();

        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { first });

        // Act
        var entries = cache.TrySetExecutor();

        // Assert - 重建缓存不能留下上一批次的残留
        Assert.Single(entries);
        Assert.False(cache.TryGetExecutor("consumer.2", out _));
        Assert.True(cache.TryGetExecutor("consumer.1", out _));
    }

    [Fact]
    public void TrySetExecutor_CalledTwice_ShouldNotThrowOnSameResults()
    {
        // Arrange - 重启时扫描结果不变，不应误判为重名冲突
        var scanResult = CreateScanResult("consumer.1", "queue.1");
        _selectorMock.Setup(s => s.SelectCandidates()).Returns(new[] { scanResult });
        var cache = new MethodMatcherCache(_selectorMock.Object);

        // Act
        cache.TrySetExecutor();
        cache.TrySetExecutor();

        // Assert
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
