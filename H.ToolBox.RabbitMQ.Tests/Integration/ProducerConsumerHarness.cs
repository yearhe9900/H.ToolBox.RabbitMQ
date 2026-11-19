using RabbitMQ.Client;
using System.Text;

namespace H.ToolBox.RabbitMQ.Tests.Integration;

/// <summary>
/// 集成测试用的轻量 AMQP 辅助类：声明拓扑、发布消息、轮询取消息。
/// 只用于测试，不承载生产逻辑。
/// </summary>
public sealed class ProducerConsumerHarness : IAsyncDisposable
{
    private readonly IConnection _connection;
    private readonly IChannel _channel;

    private ProducerConsumerHarness(IConnection connection, IChannel channel)
    {
        _connection = connection;
        _channel = channel;
    }

    public static async Task<ProducerConsumerHarness> CreateAsync(
        RabbitMqFixture fixture, CancellationToken cancellationToken = default)
    {
        var factory = new ConnectionFactory
        {
            HostName = fixture.HostName,
            Port = fixture.Port,
            UserName = fixture.UserName,
            Password = fixture.Password,
            VirtualHost = fixture.VirtualHost
        };

        var connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
        var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
        return new ProducerConsumerHarness(connection, channel);
    }

    public Task DeclareDirectExchangeAsync(string exchange, CancellationToken cancellationToken = default) =>
        _channel.ExchangeDeclareAsync(exchange, "direct", durable: true, autoDelete: false,
            arguments: null, cancellationToken: cancellationToken);

    public Task DeclareQueueAsync(
        string queue,
        IDictionary<string, object?>? arguments = null,
        CancellationToken cancellationToken = default) =>
        _channel.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false,
            arguments: arguments, cancellationToken: cancellationToken);

    public Task BindQueueAsync(string queue, string exchange, string routingKey,
        CancellationToken cancellationToken = default) =>
        _channel.QueueBindAsync(queue, exchange, routingKey, arguments: null,
            cancellationToken: cancellationToken);

    public Task PublishAsync(string exchange, string routingKey, string body,
        CancellationToken cancellationToken = default) =>
        _channel.BasicPublishAsync(exchange, routingKey, mandatory: false,
            new BasicProperties { DeliveryMode = DeliveryModes.Persistent },
            Encoding.UTF8.GetBytes(body), cancellationToken).AsTask();

    public Task<BasicGetResult?> GetAsync(string queue, CancellationToken cancellationToken = default) =>
        _channel.BasicGetAsync(queue, autoAck: true, cancellationToken: cancellationToken);

    /// <summary>
    /// 取一条消息但先不确认，供后续 BasicRejectAsync 测试毒消息路由。
    /// </summary>
    public async Task<BasicGetResult?> WaitForUnackedMessageAsync(
        string queue, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        while (!cts.IsCancellationRequested)
        {
            var result = await _channel.BasicGetAsync(queue, autoAck: false, cts.Token).ConfigureAwait(false);
            if (result is not null)
            {
                return result;
            }

            try
            {
                await Task.Delay(50, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return null;
    }

    /// <summary>
    /// 以指定 requeue 策略拒绝消息。
    /// </summary>
    public Task RejectAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default) =>
        _channel.BasicRejectAsync(deliveryTag, requeue, cancellationToken).AsTask();

    /// <summary>
    /// 轮询等待队列中出现一条消息；超时返回 null。
    /// </summary>
    public async Task<BasicGetResult?> WaitForMessageAsync(
        string queue, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        while (!cts.IsCancellationRequested)
        {
            var result = await _channel.BasicGetAsync(queue, autoAck: true, cts.Token).ConfigureAwait(false);
            if (result is not null)
            {
                return result;
            }

            try
            {
                await Task.Delay(50, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        try { await _channel.DisposeAsync().ConfigureAwait(false); } catch { /* 忽略清理异常 */ }
        try { await _connection.DisposeAsync().ConfigureAwait(false); } catch { /* 忽略清理异常 */ }
    }
}
