using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Tests.Model;

public class RabbitMQOptionsTests
{
    [Fact]
    public void DefaultPassword_ShouldBeGuest()
    {
        // Arrange & Act
        var options = new RabbitMQOptions();

        // Assert - default password should match RabbitMQ default credentials
        Assert.Equal("guest", options.Password);
    }

    [Fact]
    public void DefaultUserName_ShouldBeGuest()
    {
        var options = new RabbitMQOptions();
        Assert.Equal("guest", options.UserName);
    }

    [Fact]
    public void DefaultConfigId_ShouldBeDefault()
    {
        var options = new RabbitMQOptions();
        Assert.Equal("default", options.ConfigId);
    }

    [Fact]
    public void DefaultPublishConfirms_ShouldBeTrue()
    {
        var options = new RabbitMQOptions();
        Assert.True(options.PublishConfirms);
    }

    [Fact]
    public void DefaultPoolSize_ShouldBe15()
    {
        var options = new RabbitMQOptions();
        Assert.Equal(15, options.PoolSize);
    }

    [Fact]
    public void DefaultAbsoluteMaxPoolSize_ShouldBe128()
    {
        var options = new RabbitMQOptions();
        Assert.Equal(128, options.AbsoluteMaxPoolSize);
    }

    [Fact]
    public void DefaultIdleTimeout_ShouldBe60Seconds()
    {
        var options = new RabbitMQOptions();
        Assert.Equal(TimeSpan.FromSeconds(60), options.IdleTimeout);
    }
}
