using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Tests.Model;

public class QueueArgumentsHelperTests
{
    [Fact]
    public void BuildArguments_Classic_ShouldReturnNull()
    {
        // Act
        var result = QueueArgumentsHelper.BuildArguments(QueueType.Classic);

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void BuildArguments_Quorum_WithoutGroupSize_ShouldContainQueueType()
    {
        // Act
        var result = QueueArgumentsHelper.BuildArguments(QueueType.Quorum);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("quorum", result!["x-queue-type"]);
        Assert.False(result.ContainsKey("x-quorum-initial-group-size"));
    }

    [Fact]
    public void BuildArguments_Quorum_WithGroupSize_ShouldContainGroupSize()
    {
        // Act
        var result = QueueArgumentsHelper.BuildArguments(QueueType.Quorum, 5);

        // Assert
        Assert.NotNull(result);
        Assert.Equal("quorum", result!["x-queue-type"]);
        Assert.Equal(5, result["x-quorum-initial-group-size"]);
    }

    [Fact]
    public void BuildArguments_Quorum_WithZeroGroupSize_ShouldNotContainGroupSize()
    {
        // Act
        var result = QueueArgumentsHelper.BuildArguments(QueueType.Quorum, 0);

        // Assert
        Assert.NotNull(result);
        Assert.False(result!.ContainsKey("x-quorum-initial-group-size"));
    }

    [Fact]
    public void BuildArguments_Quorum_WithNegativeGroupSize_ShouldNotContainGroupSize()
    {
        // Act
        var result = QueueArgumentsHelper.BuildArguments(QueueType.Quorum, -1);

        // Assert
        Assert.NotNull(result);
        Assert.False(result!.ContainsKey("x-quorum-initial-group-size"));
    }

    [Fact]
    public void GetQueueTypeString_Classic_ShouldReturnClassic()
    {
        Assert.Equal("classic", QueueArgumentsHelper.GetQueueTypeString(QueueType.Classic));
    }

    [Fact]
    public void GetQueueTypeString_Quorum_ShouldReturnQuorum()
    {
        Assert.Equal("quorum", QueueArgumentsHelper.GetQueueTypeString(QueueType.Quorum));
    }
}
