using H.ToolBox.RabbitMQ.Consumer;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using Moq;

namespace H.ToolBox.RabbitMQ.Tests.Consumer;

public class ConsumerServiceSelectorTests
{
    private readonly Mock<IServiceProvider> _serviceProviderMock = new();
    private readonly Mock<ILogger<ConsumerServiceSelector>> _loggerMock = new();

    private ConsumerServiceSelector CreateSelector(IEnumerable<IToolBoxRabbitMQ> services)
    {
        _serviceProviderMock
            .Setup(sp => sp.GetService(typeof(IEnumerable<IToolBoxRabbitMQ>)))
            .Returns(services);

        return new ConsumerServiceSelector(_serviceProviderMock.Object, _loggerMock.Object);
    }

    [Fact]
    public void SelectCandidates_WithValidConsumerMethod_ShouldReturnResult()
    {
        // Arrange
        var service = new FakeServiceWithConsumer();
        var selector = CreateSelector(new IToolBoxRabbitMQ[] { service });

        // Act
        var results = selector.SelectCandidates();

        // Assert
        Assert.Single(results);
        Assert.Equal("test.consumer", results[0].ConsumerAttribute!.ConsumerName);
        Assert.Equal("test.queue", results[0].ConsumerAttribute!.QueueName);
        Assert.Same(service, results[0].Instance);
    }

    [Fact]
    public void SelectCandidates_WithMultipleConsumerMethods_ShouldReturnAll()
    {
        // Arrange
        var service = new FakeServiceWithMultipleConsumers();
        var selector = CreateSelector(new IToolBoxRabbitMQ[] { service });

        // Act
        var results = selector.SelectCandidates();

        // Assert
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.ConsumerAttribute!.ConsumerName == "consumer.a");
        Assert.Contains(results, r => r.ConsumerAttribute!.ConsumerName == "consumer.b");
    }

    [Fact]
    public void SelectCandidates_WithNoConsumerMethods_ShouldReturnEmpty()
    {
        // Arrange
        var service = new FakeServiceWithoutConsumer();
        var selector = CreateSelector(new IToolBoxRabbitMQ[] { service });

        // Act
        var results = selector.SelectCandidates();

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public void SelectCandidates_WithMultipleServices_ShouldScanAll()
    {
        // Arrange
        var service1 = new FakeServiceWithConsumer();
        var service2 = new FakeServiceWithMultipleConsumers();
        var selector = CreateSelector(new IToolBoxRabbitMQ[] { service1, service2 });

        // Act
        var results = selector.SelectCandidates();

        // Assert
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void SelectCandidates_WithNullService_ShouldSkip()
    {
        // Arrange
        var selector = CreateSelector(new IToolBoxRabbitMQ?[] { null, new FakeServiceWithConsumer() }!);

        // Act
        var results = selector.SelectCandidates();

        // Assert
        Assert.Single(results);
    }

    [Fact]
    public void SelectCandidates_ShouldPreserveMethodInfoReference()
    {
        // Arrange
        var service = new FakeServiceWithConsumer();
        var selector = CreateSelector(new IToolBoxRabbitMQ[] { service });

        // Act
        var results = selector.SelectCandidates();

        // Assert
        Assert.NotNull(results[0].MethodInfo);
        Assert.Equal(nameof(FakeServiceWithConsumer.HandleMessage), results[0].MethodInfo!.Name);
    }

    #region Fake Services

    private class FakeServiceWithConsumer : IToolBoxRabbitMQ
    {
        [RabbitMQConsumer(ConsumerName = "test.consumer", QueueName = "test.queue")]
        public void HandleMessage(string source, string routingKey) { }
    }

    private class FakeServiceWithMultipleConsumers : IToolBoxRabbitMQ
    {
        [RabbitMQConsumer(ConsumerName = "consumer.a", QueueName = "queue.a")]
        public void MethodA(string source, string routingKey) { }

        [RabbitMQConsumer(ConsumerName = "consumer.b", Exchange = "ex", RoutingKey = "rk")]
        public void MethodB(string source, string routingKey) { }
    }

    private class FakeServiceWithoutConsumer : IToolBoxRabbitMQ
    {
        public void NotAConsumer(string source, string routingKey) { }
    }

    #endregion
}
