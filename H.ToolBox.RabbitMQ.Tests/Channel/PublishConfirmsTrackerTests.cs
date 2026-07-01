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
    public async Task TrackAsync_WithAck_ShouldReturnTrue()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        var confirmTask = tracker.TrackAsync(1, CancellationToken.None);

        // 模拟 Broker 发送 ACK
        channelMock.Raise(c => c.BasicAcksAsync += null, this, new BasicAckEventArgs(1, false));

        // Act
        var result = await confirmTask;

        // Assert
        Assert.True(result);
    }

    [Fact]
    public async Task TrackAsync_WithNack_ShouldReturnFalse()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        var confirmTask = tracker.TrackAsync(1, CancellationToken.None);

        // 模拟 Broker 发送 NACK
        channelMock.Raise(c => c.BasicNacksAsync += null, this, new BasicNackEventArgs(1, false, false));

        // Act
        var result = await confirmTask;

        // Assert - NACK 返回 false
        Assert.False(result);
    }

    [Fact]
    public async Task TrackAsync_WithCumulativeAck_ShouldCompleteAll()
    {
        // Arrange - Broker 的 ACK 是累积的：ack(3) 表示 tag 1,2,3 都已确认
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        var task1 = tracker.TrackAsync(1, CancellationToken.None);
        var task2 = tracker.TrackAsync(2, CancellationToken.None);
        var task3 = tracker.TrackAsync(3, CancellationToken.None);

        // 只发送 ack(3)，应该确认所有 <= 3 的消息
        channelMock.Raise(c => c.BasicAcksAsync += null, this, new BasicAckEventArgs(3, false));

        // Act & Assert
        Assert.True(await task1);
        Assert.True(await task2);
        Assert.True(await task3);
    }

    [Fact]
    public async Task TrackAsync_WithTimeout_ShouldThrowTimeoutException()
    {
        // Arrange - 使用极短的超时时间
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, TimeSpan.FromMilliseconds(50));
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        // 追踪但不发送 ACK，让它超时
        var confirmTask = tracker.TrackAsync(1, CancellationToken.None);

        // Act & Assert
        var ex = await Assert.ThrowsAsync<TimeoutException>(() => confirmTask);
        Assert.Contains("超时", ex.Message);
    }

    [Fact]
    public async Task TrackAsync_WithExternalCancellation_ShouldThrowOperationCanceled()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, TimeSpan.FromSeconds(30));
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // 立即取消

        var confirmTask = tracker.TrackAsync(1, cts.Token);

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => confirmTask);
    }

    [Fact]
    public void Dispose_ShouldCancelPendingTasks()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        var task = tracker.TrackAsync(1, CancellationToken.None);

        // Act
        tracker.Dispose();

        // Assert - pending task should be canceled
        Assert.True(task.IsCompleted);
        Assert.True(task.IsCanceled);
    }

    [Fact]
    public void RegisterChannel_CalledTwice_ShouldBeIdempotent()
    {
        // Arrange
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();

        // Act
        tracker.RegisterChannel(channelMock.Object);
        tracker.RegisterChannel(channelMock.Object); // 第二次调用不应重复注册

        // Assert - 事件只注册一次
        channelMock.VerifyAdd(c => c.BasicAcksAsync += It.IsAny<AsyncEventHandler<BasicAckEventArgs>>(), Times.Once);
        channelMock.VerifyAdd(c => c.BasicNacksAsync += It.IsAny<AsyncEventHandler<BasicNackEventArgs>>(), Times.Once);
    }

    [Fact]
    public async Task TrackAsync_ConcurrentPublishes_ShouldConfirmIndependently()
    {
        // Arrange - 模拟多个并发发布
        var tracker = new PublishConfirmsTracker(_loggerMock.Object, _timeout);
        var channelMock = new Mock<IChannel>();
        tracker.RegisterChannel(channelMock.Object);

        var task1 = tracker.TrackAsync(1, CancellationToken.None);
        var task2 = tracker.TrackAsync(2, CancellationToken.None);

        // 只确认 tag=1
        channelMock.Raise(c => c.BasicAcksAsync += null, this, new BasicAckEventArgs(1, false));

        // Assert - task1 完成，task2 仍 pending
        Assert.True(await task1);
        Assert.False(task2.IsCompleted);

        // 确认 tag=2
        channelMock.Raise(c => c.BasicAcksAsync += null, this, new BasicAckEventArgs(2, false));
        Assert.True(await task2);
    }
}
