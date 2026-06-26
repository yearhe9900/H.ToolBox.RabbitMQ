using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Tests.Model;

public class OperateResultTests
{
    [Fact]
    public void Success_ShouldReturnSucceededResult()
    {
        // Act
        var result = OperateResult.Success;

        // Assert
        Assert.True(result.Succeeded);
        Assert.Null(result.Exception);
    }

    [Fact]
    public void Failed_ShouldReturnFailedResultWithException()
    {
        // Arrange
        var ex = new InvalidOperationException("test error");

        // Act
        var result = OperateResult.Failed(ex);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Same(ex, result.Exception);
    }

    [Fact]
    public void Failed_WithError_ShouldPreserveErrorDetails()
    {
        // Arrange
        var ex = new Exception("error");
        var error = new OperateError { Code = "E001", Description = "something went wrong" };

        // Act
        var result = OperateResult.Failed(ex, error);

        // Assert
        Assert.False(result.Succeeded);
        Assert.Same(ex, result.Exception);
    }

    [Fact]
    public void Constructor_WithSucceededTrue_ShouldWork()
    {
        // Act
        var result = new OperateResult(true);

        // Assert
        Assert.True(result.Succeeded);
        Assert.Null(result.Exception);
    }

    [Fact]
    public void Constructor_WithSucceededFalse_ShouldWork()
    {
        // Arrange
        var ex = new Exception("fail");

        // Act
        var result = new OperateResult(false, ex);

        // Assert
        Assert.False(result.Succeeded);
        Assert.NotNull(result.Exception);
    }
}
