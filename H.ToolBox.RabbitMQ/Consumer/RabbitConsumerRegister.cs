using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Reflection;

namespace H.ToolBox.RabbitMQ.Consumer
{
    /// <summary>
    /// RabbitMQ 消费者注册器
    /// 负责管理消费者注册的完整生命周期，包括启动、消息处理、健康检查和资源释放
    /// </summary>
    /// <remarks>
    /// 核心职责：
    /// <list type="bullet">
    ///   <item><description>通过 <see cref="MethodMatcherCache"/> 扫描并缓存所有消费者方法</description></item>
    ///   <item><description>为每个消费者方法根据其 <see cref="RabbitMQConsumerAttribute.ConfigId"/> 自动选择对应集群的连接池</description></item>
    ///   <item><description>创建 <see cref="IRabbitMqConsumerClient"/> 实例并启动监听</description></item>
    ///   <item><description>根据 <see cref="RabbitMQConsumerAttribute.ListenerCount"/> 启动多个并发消费者实例</description></item>
    ///   <item><description>注册消息处理回调，通过反射调用目标消费者方法</description></item>
    ///   <item><description>监控消费者健康状态，支持重启和恢复</description></item>
    ///   <item><description>在服务停止时优雅地释放所有消费者资源</description></item>
    /// </list>
    /// </remarks>
    public class RabbitConsumerRegister : IRabbitConsumerRegister
    {
        /// <summary>
        /// 日志记录器，用于记录消费者注册、消息处理、错误等信息
        /// </summary>
        private readonly ILogger _logger;

        /// <summary>
        /// 方法匹配缓存，缓存消费者名称到扫描结果的映射关系
        /// 避免每次启动时重复进行反射扫描，提高性能
        /// </summary>
        private MethodMatcherCache _selector = default!;

        /// <summary>
        /// 服务提供者，用于从 DI 容器中解析服务实例（如 <see cref="MethodMatcherCache"/>）
        /// </summary>
        private readonly IServiceProvider _serviceProvider;

        /// <summary>
        /// 消费者客户端工厂，负责创建和初始化 <see cref="IRabbitMqConsumerClient"/> 实例
        /// </summary>
        private readonly IRabbitMqConsumerClientFactory _rabbitMqConsumerClientFactory;

        /// <summary>
        /// 连接通道池管理器，用于获取集群配置选项
        /// </summary>
        private readonly IConnectionChannelPoolManager _poolManager;

        /// <summary>
        /// 取消令牌源，用于控制所有消费者实例的停止
        /// </summary>
        private CancellationTokenSource? _cts = new();

        /// <summary>
        /// 外部停止令牌，用于在 ReStartAsync 中创建链接的取消令牌源
        /// </summary>
        private CancellationToken _externalStoppingToken;

        /// <summary>
        /// 当前批次的消费者任务集合，用于在停止时等待所有消费者退出
        /// </summary>
        private readonly List<Task> _consumerTasks = new();

        /// <summary>
        /// 健康状态锁，保护消费者计数器的线程安全
        /// </summary>
        private readonly object _healthLock = new();

        /// <summary>
        /// CTS 操作锁，保护 _cts 字段的读写和 Dispose 重入
        /// </summary>
        private readonly object _ctsLock = new();

        /// <summary>
        /// 标识是否已释放资源
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// 已启动的消费者总数
        /// </summary>
        private int _totalConsumers;

        /// <summary>
        /// 当前健康的消费者数量（已成功注册且未关闭/取消的）
        /// </summary>
        private int _healthyConsumers;

        /// <summary>
        /// 是否0消费者（即没有任何消费者方法被扫描到），用于快速判断是否需要执行重启逻辑
        /// </summary>
        private bool _zeroConsumers;

        /// <summary>
        /// 标识消费者是否正在启动中，用于避免在启动过程中触发不必要的重启
        /// </summary>
        private volatile bool _isStarting;

        /// <summary>
        /// 初始化消费者注册器实例
        /// </summary>
        /// <param name="logger">日志记录器</param>
        /// <param name="serviceProvider">服务提供者，用于解析 DI 容器中的服务</param>
        /// <param name="rabbitMqConsumerClientFactory">消费者客户端工厂</param>
        /// <param name="poolManager">连接通道池管理器</param>
        public RabbitConsumerRegister(
            ILogger<RabbitConsumerRegister> logger,
            IServiceProvider serviceProvider,
            IRabbitMqConsumerClientFactory rabbitMqConsumerClientFactory,
            IConnectionChannelPoolManager poolManager)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _rabbitMqConsumerClientFactory = rabbitMqConsumerClientFactory;
            _poolManager = poolManager;
        }

        /// <summary>
        /// 停止所有消费者并重置健康状态，等待消费者任务退出后清理任务列表。
        /// 此方法可安全地从 CTS 取消回调中调用（使用 Task.Run 避免重入死锁）
        /// </summary>
        private void StopConsumers()
        {
            CancellationTokenSource cts;
            lock (_ctsLock)
            {
                if (_cts is null) return;
                cts = _cts;
            }
            // 在锁外调用 Cancel，避免回调重入时死锁
            cts.Cancel();

            // 等待当前批次的消费者任务退出，避免停止后消费者任务仍在运行
            Task[] tasks;
            lock (_consumerTasks)
            {
                tasks = _consumerTasks.ToArray();
                _consumerTasks.Clear();
            }

            if (tasks.Length > 0)
            {
                try
                {
                    Task.WaitAll(tasks, TimeSpan.FromSeconds(5));
                }
                catch (AggregateException)
                {
                    // 消费者任务因取消而抛出 OperationCanceledException，属于预期行为
                }
            }
        }

        /// <summary>
        /// 释放资源，取消所有正在运行的消费者并释放 CTS
        /// 通过 _disposed 标志防止重入（CTS 取消回调会触发此方法）
        /// </summary>
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            lock (_healthLock)
            {
                _zeroConsumers = false;
                _healthyConsumers = 0;
                _totalConsumers = 0;
            }

            StopConsumers();

            lock (_ctsLock)
            {
                _cts?.Dispose();
                _cts = null;
            }
        }

        /// <summary>
        /// 检查消费者是否处于健康状态
        /// 只有当所有已启动的消费者都健康时才返回 true
        /// </summary>
        /// <returns>如果所有消费者都健康返回 true，否则返回 false</returns>
        public bool IsHealthy()
        {
            lock (_healthLock)
            {
                // 如果消费者正在启动中，认为是健康的，避免在启动过程中触发不必要的重启
                if (_isStarting) return true;

                return _zeroConsumers ? true : _totalConsumers > 0 && _healthyConsumers == _totalConsumers;
            }
        }

        /// <summary>
        /// 异步启动消费者注册
        /// 创建关联的取消令牌源，并通过 <see cref="ExecuteAsync"/> 执行实际的消费者注册逻辑
        /// </summary>
        /// <param name="stoppingToken">停止令牌，用于取消操作</param>
        /// <returns>表示异步启动操作的任务</returns>
        public async ValueTask StartAsync(CancellationToken stoppingToken)
        {
            _externalStoppingToken = stoppingToken;

            lock (_ctsLock)
            {
                // 创建关联的取消令牌源，当外部停止令牌触发时自动取消
                _cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

                // 注册取消回调，在令牌取消时停止消费者（不释放 CTS，避免重入）
                _cts.Token.Register(StopConsumers);
            }

            _logger.LogDebug("RabbitMQ消费者注册器正在启动...");

            // 从 DI 容器中获取方法匹配缓存（单例）
            _selector = _serviceProvider.GetRequiredService<MethodMatcherCache>();

            // 执行消费者注册逻辑
            await ExecuteAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// 异步重启消费者注册
        /// 当消费者处于不健康状态或强制重启时，先停止现有消费者再重新启动
        /// </summary>
        /// <param name="force">
        /// 是否强制重启。为 true 时无论健康状态都执行重启；
        /// 为 false 时仅在消费者不健康时才重启
        /// </param>
        /// <returns>表示异步重启操作的任务</returns>
        public async ValueTask ReStartAsync(bool force = false)
        {
            if (_disposed) return;

            if (!IsHealthy() || force)
            {
                // 先停止现有消费者
                Pulse();

                // 创建新的取消令牌源，链接到外部停止令牌以确保主机关闭时能传播取消信号
                lock (_ctsLock)
                {
                    _cts = CancellationTokenSource.CreateLinkedTokenSource(_externalStoppingToken);
                }

                // 重新执行消费者注册
                await ExecuteAsync().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 停止当前所有消费者（脉冲停止）
        /// 取消当前的取消令牌源，触发所有消费者实例停止
        /// </summary>
        public void Pulse()
        {
            StopConsumers();
        }

        /// <summary>
        /// 执行消费者注册的核心逻辑
        /// 通过方法匹配缓存获取所有消费者方法，为每个方法创建指定数量的消费者客户端并启动监听。
        /// 每个消费者根据其 <see cref="RabbitMQConsumerAttribute.ConfigId"/> 自动连接到对应的集群。
        /// </summary>
        /// <returns>表示异步执行操作的任务</returns>
        /// <remarks>
        /// 执行流程：
        /// <list type="number">
        ///   <item><description>通过 <see cref="MethodMatcherCache.TrySetExecutor()"/> 获取所有消费者方法</description></item>
        ///   <item><description>遍历每个消费者方法，根据其 ConfigId 创建对应集群的客户端</description></item>
        ///   <item><description>根据 <c>ListenerCount</c> 创建指定数量的消费者客户端实例</description></item>
        ///   <item><description>每个客户端在独立的 Task 中启动监听</description></item>
        /// </list>
        /// </remarks>
        private async ValueTask ExecuteAsync()
        {
            CancellationTokenSource cts;
            lock (_ctsLock)
            {
                if (_cts is null || _disposed) return;
                cts = _cts;
            }

            // 标记消费者正在启动中
            _isStarting = true;

            try
            {
                // 通过缓存获取所有消费者方法扫描结果
                var methodScanResults = _selector.TrySetExecutor();

                // 重置健康计数器，为本次启动做准备
                lock (_healthLock)
                {
                    _totalConsumers = 0;
                    _healthyConsumers = 0;
                    _zeroConsumers = methodScanResults.IsEmpty;
                }

                foreach (var methodScanResult in methodScanResults)
                {
                    var attr = methodScanResult.Value.ConsumerAttribute;

                    if (attr == null)
                        continue;

                    // 获取此消费者绑定的集群 ConfigId
                    var configId = attr.ConfigId ?? "default";
                    try
                    {

                        // 根据 ListenerCount 创建指定数量的并发消费者实例
                        for (var i = 0; i < attr.ListenerCount; i++)
                        {
                            // 捕获当前 configId 用于闭包
                            var consumerConfigId = configId;

                            lock (_healthLock)
                            {
                                _totalConsumers++;
                            }

                            // 在独立的 Task 中启动每个消费者实例，避免阻塞注册流程
                            var task = Task.Run(async () =>
                            {
                                try
                                {
                                    // 使用 using 确保客户端在任务结束时被正确释放
                                    // 通过消费者的 ConfigId 创建对应集群的客户端
                                    await using var client = await _rabbitMqConsumerClientFactory.CreateAsync(consumerConfigId).ConfigureAwait(false);

                                    // 注册消息处理和日志回调
                                    RegisterMessageProcessor(client);

                                    // 启动消息监听，此方法会阻塞直到取消令牌被触发
                                    await client.ListeningAsync(
                                        TimeSpan.FromSeconds(attr.WaitOneTimeOut),
                                        cts.Token,
                                        new ListeningOptions()
                                        {
                                            ConsumerName = attr.ConsumerName,
                                            Durable = attr.Durable,
                                            Exchange = attr.Exchange,
                                            ExchangeType = attr.ExchangeType,
                                            PrefetchCount = attr.PrefetchCount,
                                            QueueName = attr.QueueName,
                                            RoutingKey = attr.RoutingKey,
                                            AutoAck = attr.AutoAck,
                                            ConsumerConcurrent = attr.ConsumerConcurrent,
                                            QueueType = attr.QueueType,
                                            Exclusive = attr.Exclusive,
                                            AutoDelete = attr.AutoDelete,
                                            QuorumGroupSize = attr.QuorumGroupSize > 0
                                                ? attr.QuorumGroupSize
                                                : _poolManager.GetOptions(consumerConfigId).QuorumGroupSize,
                                        }).ConfigureAwait(false);
                                }
                                catch (OperationCanceledException)
                                {
                                    // 正常取消，不需要标记为不健康
                                }
                                catch (Exception ex)
                                {
                                    // 消费者任务异常（如连接断开），同步递减两个计数器使比率可自愈
                                    lock (_healthLock)
                                    {
                                        _healthyConsumers = Math.Max(0, _healthyConsumers - 1);
                                        _totalConsumers = Math.Max(0, _totalConsumers - 1);
                                    }
                                    _logger.LogError(ex, "消费者 {ConsumerName} 运行异常 (ConfigId: {ConfigId})", attr.ConsumerName, consumerConfigId);
                                }
                            });

                            lock (_consumerTasks)
                            {
                                _consumerTasks.Add(task);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // 记录异常但继续处理其他消费者方法
                        _logger.LogError(ex, "启动消费者 {ConsumerName} 时发生错误 (ConfigId: {ConfigId})", attr.ConsumerName, configId);
                        continue;
                    }
                }
            }
            finally
            {
                // 重置启动标志
                _isStarting = false;
            }
        }

        /// <summary>
        /// 处理消费者日志事件
        /// 根据日志类型更新消费者健康状态并记录相应的日志信息
        /// </summary>
        /// <param name="logmsg">日志事件参数，包含日志类型和原因信息</param>
        private void WriteLog(LogMessageEventArgs logmsg)
        {
            switch (logmsg.LogType)
            {
                case MqLogType.ConsumerCancelled:
                    // 消费者被服务器取消，减少健康计数
                    lock (_healthLock)
                    {
                        _healthyConsumers = Math.Max(0, _healthyConsumers - 1);
                    }
                    _logger.LogWarning("RabbitMQ消费者已被取消。 --> {Reason}", logmsg.Reason);
                    break;
                case MqLogType.ConsumerRegistered:
                    // 消费者成功注册，增加健康计数
                    lock (_healthLock)
                    {
                        _healthyConsumers++;
                    }
                    _logger.LogInformation("RabbitMQ消费者已注册。 --> {Reason}", logmsg.Reason);
                    break;
                case MqLogType.ConsumerUnregistered:
                    // 消费者取消注册（客户端发起），记录警告日志
                    _logger.LogWarning("RabbitMQ消费者已取消注册。 --> {Reason}", logmsg.Reason);
                    break;
                case MqLogType.ConsumerShutdown:
                    // 通道关闭，减少健康计数
                    lock (_healthLock)
                    {
                        _healthyConsumers = Math.Max(0, _healthyConsumers - 1);
                    }
                    _logger.LogWarning("RabbitMQ消费者已关闭。 --> {Reason}", logmsg.Reason);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        /// <summary>
        /// 为消费者客户端注册消息处理回调
        /// 当消费者收到消息时，通过反射调用目标方法进行业务处理
        /// </summary>
        /// <param name="client">消费者客户端实例</param>
        /// <remarks>
        /// 消息处理流程：
        /// <list type="number">
        ///   <item><description>通过 <see cref="MethodMatcherCache.TryGetExecutor"/> 根据消费者标签查找对应的方法信息</description></item>
        ///   <item><description>使用反射调用目标方法，传入消息内容和路由键作为参数</description></item>
        ///   <item><description>如果目标方法返回 Task，则等待其完成</description></item>
        ///   <item><description>处理成功后调用 <see cref="IRabbitMqConsumerClient.CommitAsync"/> 确认消息</description></item>
        ///   <item><description>处理失败时记录错误日志并调用 <see cref="IRabbitMqConsumerClient.RejectAsync"/> 拒绝消息</description></item>
        /// </list>
        /// </remarks>
        private void RegisterMessageProcessor(IRabbitMqConsumerClient client)
        {
            // 注册日志回调
            client.OnLogCallback = WriteLog;

            // 注册消息处理回调
            client.OnMessageCallback = async (message, sender, consumerTag) =>
            {
                try
                {
                    // 根据消费者标签（consumerTag）查找对应的方法执行器
                    var canFindSubscriber = _selector.TryGetExecutor(consumerTag, out var executor);
                    if (canFindSubscriber)
                    {
                        if (executor == null)
                            return;

                        var instance = executor.Instance;
                        var methodInfo = executor.MethodInfo;
                        try
                        {
                            // 校验方法签名：要求 (string, string) 或 (string, string, CancellationToken)
                            var parameters = methodInfo!.GetParameters();
                            if (parameters.Length < 2
                                || parameters[0].ParameterType != typeof(string)
                                || parameters[1].ParameterType != typeof(string))
                            {
                                _logger.LogError("消费者方法 {Method} 签名不符合要求，需要 (string, string) 参数", methodInfo.Name);
                                await client.CommitAsync(sender).ConfigureAwait(false);
                                return;
                            }

                            // 构造方法参数：[消息内容, 路由键]
#if NET10_0_OR_GREATER
                            object?[] invokeParams = parameters.Length >= 3 && parameters[2].ParameterType == typeof(CancellationToken)
                                ? [message.BodyContent, message.RoutingKey, CancellationToken.None]
                                : [message.BodyContent, message.RoutingKey];
#else
                            object?[] invokeParams = parameters.Length >= 3 && parameters[2].ParameterType == typeof(CancellationToken)
                                ? new object?[] { message.BodyContent, message.RoutingKey, CancellationToken.None }
                                : new object?[] { message.BodyContent, message.RoutingKey };
#endif
                            // 通过反射调用目标消费者方法
                            var result = methodInfo!.Invoke(instance, invokeParams);
                            var canCommit = true;

                            // 根据返回类型处理结果，用于决定消息确认或拒绝
                            if (result is Task<bool> boolTask)
                            {
                                canCommit = await boolTask.ConfigureAwait(false);
                            }
                            else if (result is Task taskResult)
                            {
                                await taskResult.ConfigureAwait(false);
                            }
                            else if (result is bool boolResult)
                            {
                                canCommit = boolResult;
                            }

                            // 根据处理结果决定确认或拒绝消息
                            if (canCommit)
                            {
                                await client.CommitAsync(sender).ConfigureAwait(false);
                            }
                            else
                            {
                                await client.RejectAsync(sender).ConfigureAwait(false);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "处理消息时发生异常");
                            await client.RejectAsync(sender).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        _logger.LogError("未找到消费者方法执行器，无法处理消息。 --> {ConsumerTag}", consumerTag);
                        await client.RejectAsync(sender).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    // 消息处理失败，记录错误并拒绝消息（消息将被重新入队）
                    _logger.LogError(ex, "处理消息时发生异常");
                    await client.RejectAsync(sender).ConfigureAwait(false);
                }
            };
        }
    }
}