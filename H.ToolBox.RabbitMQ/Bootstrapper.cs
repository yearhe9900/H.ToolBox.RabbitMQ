using H.ToolBox.RabbitMQ.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace H.ToolBox.RabbitMQ
{
    /// <summary>
    /// RabbitMQ后台启动器
    /// 继承自 <see cref="BackgroundService"/>，负责在应用程序启动时初始化并启动所有RabbitMQ消费者注册器，
    /// 并在应用程序停止时优雅地释放相关资源
    /// </summary>
    public class Bootstrapper : BackgroundService, IAsyncDisposable
    {
        private readonly ILogger<Bootstrapper> _logger;
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// 消费者注册器集合
        /// </summary>
        private IEnumerable<IRabbitConsumerRegister> _processors = default!;

        /// <summary>
        /// 取消令牌源，用于控制服务停止
        /// </summary>
        private CancellationTokenSource? _cts;

        /// <summary>
        /// 标识是否已释放资源
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// 获取启动器是否已启动的状态
        /// </summary>
        public bool IsStarted => !_cts?.IsCancellationRequested ?? false;

        /// <summary>
        /// 初始化Bootstrapper实例
        /// </summary>
        /// <param name="serviceProvider">服务提供者，用于解析已注册的消费者注册器</param>
        /// <param name="logger">日志记录器</param>
        public Bootstrapper(IServiceProvider serviceProvider, ILogger<Bootstrapper> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        /// <summary>
        /// 执行后台服务的主要逻辑
        /// 从DI容器中解析所有已注册的消费者注册器，并逐一启动
        /// </summary>
        /// <param name="stoppingToken">停止令牌</param>
        /// <returns>表示异步执行操作的任务</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (_cts != null)
            {
                _logger.LogInformation("RabbitMQ启动器已启动");
            }
            _logger.LogDebug("RabbitMQ启动器正在执行...");

            // 创建链接的取消令牌源，关联外部停止令牌
            _cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            // 从DI容器中解析所有消费者注册器（在注册回调之前完成，确保回调中可以安全访问）
            _processors = _serviceProvider.GetServices<IRabbitConsumerRegister>();

            // 注册取消回调，在服务停止时释放所有消费者注册器
            _cts.Token.Register(() =>
            {
                foreach (var processor in _processors)
                {
                    try
                    {
                        processor.Dispose();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "停止RabbitMQ消费者时发生错误。");
                    }
                }
            });

            // 逐一启动每个消费者注册器
            foreach (var processor in _processors)
            {
                try
                {
                    _cts!.Token.ThrowIfCancellationRequested();

                    await processor.StartAsync(_cts!.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // 服务正在停止，忽略取消异常
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "启动RabbitMQ消费者时发生错误。");
                }
            }
            _logger.LogInformation("RabbitMQ启动器已成功启动");
        }

        /// <summary>
        /// 停止后台服务，等待基类完成停止流程
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        /// <returns>表示异步停止操作的任务</returns>
        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// 释放资源，取消并清理取消令牌源
        /// </summary>
        public override void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try { _cts?.Cancel(); } catch (ObjectDisposedException) { }
            try { _cts?.Dispose(); } catch (ObjectDisposedException) { }
            _cts = null;

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
