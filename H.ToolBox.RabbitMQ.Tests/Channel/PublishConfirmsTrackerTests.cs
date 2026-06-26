using H.ToolBox.RabbitMQ.Channel;
using Microsoft.Extensions.Logging;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace H.ToolBox.RabbitMQ.Tests.Channel;

public class PublishConfirmsTrackerTests : IDisposable
{
    private readonly Mock<ILogger> _loggerMock = new();
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

    public void Dispose()
    {
        // cleanup if needed
    }

    [Fact]
    public async Task WaitForAllConfirmsAsync_WhenNoPending_ShouldReturnImmediately()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);

        // Act & Assert - 不应阻塞
        await tracker.WaitForAllConfirmsAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WaitForAllConfirmsAsync_WithAck_ShouldComplete()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        // 模拟发布并追踪 deliveryTag=1
        tracker.Track(1);

        // 模拟 Broker 发送 ACK
        // 通过 Raise 触发 BasicAcksAsync 事件
        channelMock.Raise(c => c.BasicAcksAsync += null, this, new BasicAckEventArgs(1, false));

        // Act
        await tracker.WaitForAllConfirmsAsync(CancellationToken.None);

        // Assert - 正常返回表示所有消息已确认
        Assert.False(tracker.HasPending);
    }

    [Fact]
    public async Task WaitForAllConfirmsAsync_WithNack_ShouldThrowInvalidOperation()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        tracker.Track(1);

        // 模拟 Broker 发送 NACK
        channelMock.Raise(c => c.BasicNacksAsync += null, this, new BasicNackEventArgs(1, false, false));

        // Act & Assert - NACK 应抛出 InvalidOperationException
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => tracker.WaitForAllConfirmsAsync(CancellationToken.None));

        Assert.Contains("NACK", ex.Message);
        Assert.Contains("1", ex.Message);
    }

    [Fact]
    public async Task WaitForAllConfirmsAsync_WithCumulativeAck_ShouldCompleteAll()
    {
        // Arrange - Broker 的 ACK 是累积的：ack(3) 表示 tag 1,2,3 都已确认
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        tracker.Track(1);
        tracker.Track(2);
        tracker.Track(3);

        // 只发送 ack(3)，应该确认所有 <= 3 的消息
        channelMock.Raise(c => c.BasicAcksAsync += null, this, new BasicAckEventArgs(3, false));

        // Act
        await tracker.WaitForAllConfirmsAsync(CancellationToken.None);

        Assert.False(tracker.HasPending);
    }

    [Fact]
    public async Task WaitForAllConfirmsAsync_WithTimeout_ShouldThrowTimeoutException()
    {
        // Arrange - 使用极短的超时时间
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, TimeSpan.FromMilliseconds(50));
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        // 追踪但不发送 ACK，让它超时
        tracker.Track(1);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => tracker.WaitForAllConfirmsAsync(CancellationToken.None));

        Assert.Contains("超时", ex.Message);
        Assert.Contains("1", ex.Message); // 应包含未确认的 deliveryTag
    }

    [Fact]
    public async Task WaitForAllConfirmsAsync_WithExternalCancellation_ShouldThrowOperationCanceled()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, TimeSpan.FromSeconds(30));
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        tracker.Track(1);

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // 立即取消

        // Act & Assert - 外部取消应抛 OperationCanceledException，而非 TimeoutException
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => tracker.WaitForAllConfirmsAsync(cts.Token));
    }

    [Fact]
    public void UnregisterChannel_ShouldRemoveEventHandlers()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        // Act
        tracker.UnregisterChannel(channelMock.Object);

        // Assert - 验证事件已取消订阅
        channelMock.VerifyRemove(c => c.BasicAcksAsync -= It.IsAny<AsyncEventHandler<BasicAckEventArgs>>(), Times.Once);
        channelMock.VerifyRemove(c => c.BasicNacksAsync -= It.IsAny<AsyncEventHandler<BasicNackEventArgs>>(), Times.Once);
    }

    [Fact]
    public void Dispose_ShouldClearPendingConfirms()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        tracker.Track(1);
        tracker.Track(2);

        // Act
        tracker.Dispose();

        // Assert
        Assert.False(tracker.HasPending);
    }

    [Fact]
    public void HasPending_WithTrackedMessages_ShouldReturnTrue()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);

        // Act & Assert
        Assert.False(tracker.HasPending);
        tracker.Track(1);
        Assert.True(tracker.HasPending);
    }
}
