using H.ToolBox.RabbitMQ.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace H.ToolBox.RabbitMQ
{
    /// <summary>
    /// 传输连接健康检查处理器
    /// 继承自 <see cref="BackgroundService"/>，通过定时检查 <see cref="IRabbitConsumerRegister"/> 的健康状态，
    /// 在消费者不健康时自动触发重启，确保消息消费服务的持续可用性
    /// </summary>
    /// <remarks>
    /// 工作机制：
    /// <list type="bullet">
    ///   <item><description>每 30 秒从 DI 容器中解析所有 <see cref="IRabbitConsumerRegister"/> 实例</description></item>
    ///   <item><description>调用 <see cref="IRabbitConsumerRegister.IsHealthy()"/> 检查每个注册器的健康状态</description></item>
    ///   <item><description>对不健康的注册器调用 <see cref="IRabbitConsumerRegister.ReStartAsync"/> 进行重启</description></item>
    ///   <item><description>记录检查和重启过程中的日志信息</description></item>
    /// </list>
    /// </remarks>
    public class TransportCheckProcessor : BackgroundService, IAsyncDisposable
    {
        private readonly ILogger<TransportCheckProcessor> _logger;
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// 健康检查间隔时间
        /// </summary>
        private readonly TimeSpan _checkInterval;

        /// <summary>
        /// 标识是否已释放资源
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// 初始化 TransportCheckProcessor 实例
        /// </summary>
        /// <param name="serviceProvider">服务提供者，用于解析消费者注册器</param>
        /// <param name="logger">日志记录器</param>
        /// <param name="checkInterval">健康检查间隔时间，由 AddRabbitMQ 传入</param>
        public TransportCheckProcessor(IServiceProvider serviceProvider, ILogger<TransportCheckProcessor> logger, TimeSpan checkInterval)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _checkInterval = checkInterval;
        }

        /// <summary>
        /// 执行后台健康检查的主要逻辑
        /// 以可配置的间隔周期性地检查所有消费者注册器的健康状态，并在发现不健康时自动重启
        /// </summary>
        /// <param name="stoppingToken">停止令牌</param>
        /// <returns>表示异步执行操作的任务</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("RabbitMQ传输健康检查处理器已启动，检查间隔: {Interval}秒", _checkInterval.TotalSeconds);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // 等待下一个检查周期
                    await Task.Delay(_checkInterval, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // 服务正在停止，跳出循环
                    break;
                }

                await CheckAndRestartConsumersAsync(stoppingToken).ConfigureAwait(false);
            }

            _logger.LogInformation("RabbitMQ传输健康检查处理器已停止");
        }

        /// <summary>
        /// 检查所有消费者注册器的健康状态，对不健康的注册器执行重启
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步检查操作的任务</returns>
        private async Task CheckAndRestartConsumersAsync(CancellationToken cancellationToken)
        {
            IEnumerable<IRabbitConsumerRegister> processors;
            try
            {
                processors = _serviceProvider.GetServices<IRabbitConsumerRegister>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "从DI容器解析消费者注册器时发生错误");
                return;
            }

            foreach (var processor in processors)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                try
                {
                    if (!processor.IsHealthy())
                    {
                        _logger.LogWarning("检测到消费者注册器不健康，正在尝试重启...");

                        await processor.ReStartAsync(force: true).ConfigureAwait(false);

                        _logger.LogInformation("消费者注册器重启成功");
                    }
                    else
                    {
                        _logger.LogDebug("消费者注册器健康检查通过");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "健康检查或重启消费者注册器时发生错误");
                }
            }
        }

        /// <summary>
        /// 停止后台服务，等待基类完成停止流程
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步停止操作的任务</returns>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("RabbitMQ传输健康检查处理器正在停止...");
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public override void Dispose()
        {
            if (_disposed) return;

            _disposed = true;
            base.Dispose();
        }

        /// <summary>
        /// 异步释放资源
        /// </summary>
        /// <returns>值任务</returns>
        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}