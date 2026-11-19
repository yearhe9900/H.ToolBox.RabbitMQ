using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Linq.Expressions;
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
        /// 取消令牌源，用于控制所有消费者实例的停止。
        /// 由 <see cref="BuildCts"/> 统一创建/替换，避免 StartAsync 与 ReStartAsync 各自创建导致旧实例泄漏。
        /// </summary>
        private CancellationTokenSource? _cts;

        /// <summary>
        /// 外部停止令牌，用于在 ReStartAsync 中创建链接的取消令牌源
        /// </summary>
        private CancellationToken _externalStoppingToken;

        /// <summary>
        /// 当前批次的消费者任务集合，用于在停止时等待所有消费者退出
        /// </summary>
        private readonly List<Task> _consumerTasks = new();

        /// <summary>
        /// CTS 操作锁，保护 _cts 字段的读写和 Dispose 重入
        /// </summary>
        private readonly object _ctsLock = new();

        /// <summary>
        /// 标识是否已释放资源
        /// </summary>
        private bool _disposed;

        /// <summary>
        /// 每个消费者实例的健康状态：key 为实例唯一标识（ConsumerName-索引），value 为是否健康。
        /// 按实例判定取代原来的聚合计数器，避免"异常时 healthy 与 total 同步递减"导致
        /// healthy == total 恒成立、IsHealthy() 永真、故障消费者永不重启的问题。
        /// </summary>
        private readonly ConcurrentDictionary<string, bool> _consumerHealth = new(StringComparer.Ordinal);

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
        /// 创建链接到指定令牌的新 CTS，注册停止回调，并在锁外释放旧 CTS。
        /// StartAsync 与 ReStartAsync 共用此方法，确保每次重建的令牌源行为一致：
        /// 旧实现在 ReStartAsync 中直接替换 _cts，既没注册 StopConsumers 回调，也没释放旧 CTS。
        /// </summary>
        /// <param name="token">要链接的外部取消令牌</param>
        private void BuildCts(CancellationToken token)
        {
            CancellationTokenSource? previous;
            lock (_ctsLock)
            {
                previous = _cts;
                _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
                // 外部令牌取消时同样要停消费者、等待任务退出，与 StartAsync 行为一致
                _cts.Token.Register(StopConsumers);
            }

            // 旧 CTS 的取消回调已在 Pulse() 中同步执行完毕，此处可安全释放
            if (previous != null)
            {
                try { previous.Dispose(); }
                catch (ObjectDisposedException) { /* 已被其它路径释放 */ }
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

            _zeroConsumers = false;
            _consumerHealth.Clear();

            StopConsumers();

            lock (_ctsLock)
            {
                _cts?.Dispose();
                _cts = null;
            }
        }

        /// <summary>
        /// 检查消费者是否处于健康状态：按实例判定，只有全部已启动实例都健康时才返回 true。
        /// </summary>
        /// <returns>全部消费者实例健康、或无消费者需要启动时返回 true；任一实例异常返回 false</returns>
        /// <remarks>
        /// 旧实现用 _healthyConsumers == _totalConsumers 判断，但任务异常时两个计数器同步递减，
        /// 等式恒成立，导致 IsHealthy() 永真、故障实例永不重启。现改为按实例维护状态字典：
        /// 每个实例启动即登记为健康，异常时置为 false，任何一台不健康即触发重启。
        /// </remarks>
        public bool IsHealthy()
        {
            // 启动过程中不做判定，避免注册尚未完成就触发重启
            if (_isStarting) return true;

            // 没有任何消费者方法需要启动，视为健康（无需重启）
            if (_zeroConsumers) return true;

            // 期望有消费者却一个都不在运行（全部异常退出/被取消）时也应判定为不健康，
            // 由健康检查触发重启；否则要求所有已登记实例都健康。
            // 启动窗口由 _isStarting 覆盖，不会出现"尚未登记"的误判。
            return !_consumerHealth.IsEmpty && _consumerHealth.Values.All(healthy => healthy);
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

            // 统一创建链接令牌源并注册停止回调，避免与 ReStartAsync 各自创建导致旧 CTS 泄漏
            BuildCts(stoppingToken);

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
                // 先停止现有消费者（取消旧 CTS 并等待其任务退出）
                Pulse();

                // 重建链接到外部停止令牌的 CTS：注册停止回调 + 释放旧 CTS
                BuildCts(_externalStoppingToken);

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

                // 重置按实例的健康状态，为本次启动做准备
                _consumerHealth.Clear();
                _zeroConsumers = methodScanResults.IsEmpty;

                foreach (var methodScanResult in methodScanResults)
                {
                    // 显式把执行器绑定到本次启动的消费者，不再依赖 consumerTag 字符串回退查找
                    var executorForTag = methodScanResult.Value;
                    var attr = executorForTag.ConsumerAttribute;

                    if (attr == null)
                        continue;

                    // 编译委托，替代反射调用，提升消息处理性能
                    CompileDelegate(executorForTag);

                    // 获取此消费者绑定的集群 ConfigId
                    var configId = attr.ConfigId ?? "default";
                    try
                    {

                        // 根据 ListenerCount 创建指定数量的并发消费者实例
                        for (var i = 0; i < attr.ListenerCount; i++)
                        {
                            // 捕获当前 configId 和索引用于闭包
                            var consumerConfigId = configId;
                            var consumerIndex = i;

                            // 为每个消费者实例生成唯一的 consumer tag（RabbitMQ 要求同一连接内 tag 唯一）
                            // 格式: ConsumerName-索引（多实例时追加后缀，单实例保持原名）
                            var consumerTag = attr.ListenerCount > 1
                                ? $"{attr.ConsumerName}-{consumerIndex}"
                                : attr.ConsumerName ?? string.Empty;

                            // 启动即登记该实例为健康；后续由任务生命周期/日志事件更新
                            _consumerHealth[consumerTag] = true;

                            // 在独立的 Task 中启动每个消费者实例，避免阻塞注册流程
                            var task = Task.Run(async () =>
                            {
                                try
                                {
                                    // 使用 using 确保客户端在任务结束时被正确释放
                                    // 通过消费者的 ConfigId 创建对应集群的客户端
                                    await using var client = await _rabbitMqConsumerClientFactory.CreateAsync(consumerConfigId).ConfigureAwait(false);

                                    // 注册消息处理和日志回调：执行器由启动时直接绑定，避免同名类库互相串台
                                    RegisterMessageProcessor(client, executorForTag, consumerTag);

                                    // 启动消息监听，此方法会阻塞直到取消令牌被触发
                                    await client.ListeningAsync(
                                        TimeSpan.FromSeconds(attr.WaitOneTimeOut),
                                        cts.Token,
                                        new ListeningOptions()
                                        {
                                            ConsumerName = consumerTag,
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
                                    // 正常停机/重启导致的取消，移除实例登记，不再参与健康判定
                                    _consumerHealth.TryRemove(consumerTag, out _);
                                }
                                catch (Exception ex)
                                {
                                    // 消费者任务异常（如连接断开）：仅将该实例标记为不健康，
                                    // 不再像旧实现那样同步递减 total 与 healthy（那会让 IsHealthy 永真）。
                                    // 保留条目使 IsHealthy() 返回 false，由 TransportCheckProcessor 触发重启。
                                    _consumerHealth[consumerTag] = false;
                                    _logger.LogError(ex, "消费者 {ConsumerName} 运行异常 (ConfigId: {ConfigId})，已标记为不健康", attr.ConsumerName, consumerConfigId);
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
        /// 处理消费者日志事件，按实例更新健康状态并记录日志
        /// </summary>
        /// <param name="consumerTag">触发该事件的消费者实例标识</param>
        /// <param name="logmsg">日志事件参数，包含日志类型和原因信息</param>
        private void WriteLog(string consumerTag, LogMessageEventArgs logmsg)
        {
            switch (logmsg.LogType)
            {
                case MqLogType.ConsumerCancelled:
                    // 服务器取消消费者：标记该实例不健康
                    _consumerHealth[consumerTag] = false;
                    _logger.LogWarning("RabbitMQ消费者已被取消。 --> {Reason}", logmsg.Reason);
                    break;
                case MqLogType.ConsumerRegistered:
                    // 消费者成功注册：标记该实例健康
                    _consumerHealth[consumerTag] = true;
                    _logger.LogInformation("RabbitMQ消费者已注册。 --> {Reason}", logmsg.Reason);
                    break;
                case MqLogType.ConsumerUnregistered:
                    // 客户端主动取消注册，记录警告日志；健康状态由任务生命周期决定
                    _logger.LogWarning("RabbitMQ消费者已取消注册。 --> {Reason}", logmsg.Reason);
                    break;
                case MqLogType.ConsumerShutdown:
                    // 通道关闭：标记该实例不健康
                    _consumerHealth[consumerTag] = false;
                    _logger.LogWarning("RabbitMQ消费者已关闭。 --> {Reason}", logmsg.Reason);
                    break;
                default:
                    _logger.LogDebug("RabbitMQ消费者事件 {LogType}。 --> {Reason}", logmsg.LogType, logmsg.Reason);
                    break;
            }
        }

        /// <summary>
        /// 为消费者客户端注册消息处理回调
        /// 当消费者收到消息时，通过反射调用目标方法进行业务处理
        /// </summary>
        /// <param name="client">消费者客户端实例</param>
        /// <param name="executor">本次启动时直接绑定的方法执行器</param>
        /// <param name="consumerTag">消费者实例标识，用于将日志事件绑定到具体实例的健康状态</param>
        /// <remarks>
        /// 消息处理流程：
        /// <list type="number">
        ///   <item><description>启动期把 <see cref="MethodScanResult"/> 直接绑定到该消费者，避免按 consumerTag 字符串回退查找</description></item>
        ///   <item><description>使用反射调用目标方法，传入消息内容和路由键作为参数</description></item>
        ///   <item><description>如果目标方法返回 Task，则等待其完成</description></item>
        ///   <item><description>处理成功后调用 <see cref="IRabbitMqConsumerClient.CommitAsync"/> 确认消息</description></item>
        ///   <item><description>处理失败时记录错误日志并以 requeue:false 拒绝消息，使其进入死信队列</description></item>
        /// </list>
        /// </remarks>
        private void RegisterMessageProcessor(IRabbitMqConsumerClient client, MethodScanResult executor, string consumerTag)
        {
            // 注册日志回调，绑定当前实例标识，使健康事件能落到具体实例
            client.OnLogCallback = logMsg => WriteLog(consumerTag, logMsg);

            // 注册消息处理回调
            client.OnMessageCallback = async (message, sender, consumerTag) =>
            {
                try
                {
                    // 执行器在启动期由调用方直接绑定，不再按 consumerTag 字符串回退查找：
                    // 旧写法在多个类库存在同名 ConsumerName 时会把消息路由到错误的实现。
                    var canFindSubscriber = executor != null;
                    if (canFindSubscriber)
                    {
                        if (executor == null)
                            return;

                        try
                        {
                            // 优先使用编译后的委托，避免反射调用开销
                            if (executor.CompiledDelegate != null)
                            {
                                var canCommit = await executor.CompiledDelegate(message.BodyContent, message.RoutingKey).ConfigureAwait(false);

                                if (canCommit)
                                {
                                    await client.CommitAsync(sender).ConfigureAwait(false);
                                }
                                else
                                {
                                    await client.RejectAsync(sender).ConfigureAwait(false);
                                }
                            }
                            else
                            {
                                // 回退到反射调用（理论上不应走到这里）
                                var instance = executor.Instance;
                                var methodInfo = executor.MethodInfo;

                                // 校验方法签名：要求 (string, string) 或 (string, string, CancellationToken)
                                var parameters = methodInfo!.GetParameters();
                                if (parameters.Length < 2
                                    || parameters[0].ParameterType != typeof(string)
                                    || parameters[1].ParameterType != typeof(string))
                                {
                                    // 签名不合法属于配置错误，不能静默 Ack 丢消息；
                                    // 以 requeue:false 拒绝，让消息进入死信队列保留现场供排查。
                                    _logger.LogError("消费者方法 {Method} 签名不符合要求，需要 (string, string) 参数，消息将进入死信队列", methodInfo.Name);
                                    await client.RejectAsync(sender).ConfigureAwait(false);
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
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "处理消息时发生异常");
                            await client.RejectAsync(sender).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        _logger.LogError("消费者 {ConsumerTag} 未绑定方法执行器，无法处理消息。", consumerTag);
                        await client.RejectAsync(sender).ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    // 兜底：记录错误并以 requeue:false 拒绝，消息进入死信队列，避免无限重投
                    _logger.LogError(ex, "处理消息时发生异常，消息将进入死信队列");
                    await client.RejectAsync(sender).ConfigureAwait(false);
                }
            };
        }

        /// <summary>
        /// 编译消费者方法的委托，替代反射调用以提升性能
        /// 使用 Expression Tree 将方法调用编译为原生委托，避免每条消息的反射开销
        /// </summary>
        /// <param name="scanResult">方法扫描结果，包含实例、方法信息等</param>
        private void CompileDelegate(MethodScanResult scanResult)
        {
            try
            {
                var methodInfo = scanResult.MethodInfo;
                var instance = scanResult.Instance;

                if (methodInfo == null || instance == null)
                    return;

                // 校验方法签名
                var parameters = methodInfo.GetParameters();
                if (parameters.Length < 2
                    || parameters[0].ParameterType != typeof(string)
                    || parameters[1].ParameterType != typeof(string))
                {
                    return;
                }

                // 构建表达式树参数
                var sourceParam = Expression.Parameter(typeof(string), "source");
                var routingKeyParam = Expression.Parameter(typeof(string), "routingKey");
                var instanceConst = Expression.Constant(instance);

                // 构建方法调用表达式
                Expression[] callArgs;
                if (parameters.Length >= 3 && parameters[2].ParameterType == typeof(CancellationToken))
                {
                    callArgs = new Expression[] { sourceParam, routingKeyParam, Expression.Constant(CancellationToken.None) };
                }
                else
                {
                    callArgs = new Expression[] { sourceParam, routingKeyParam };
                }

                var callExpr = Expression.Call(instanceConst, methodInfo, callArgs);

                // 根据返回类型构建统一的 Func<string, string, Task<bool>> 委托
                var returnType = methodInfo.ReturnType;

                Expression body;

                if (returnType == typeof(Task<bool>))
                {
                    // 方法返回 Task<bool>，直接使用
                    body = callExpr;
                }
                else if (returnType == typeof(Task))
                {
                    // 方法返回 Task（无返回值），包装为 Task<bool> = true
                    var taskVar = Expression.Variable(typeof(Task), "task");
                    var block = Expression.Block(
                        new[] { taskVar },
                        Expression.Assign(taskVar, callExpr),
                        Expression.Call(typeof(CompileDelegateHelper), nameof(CompileDelegateHelper.AwaitTaskAndReturnTrue), null, taskVar)
                    );
                    body = block;
                }
                else if (returnType == typeof(bool))
                {
                    // 方法返回 bool，包装为 Task.FromResult<bool>
                    body = Expression.Call(typeof(Task), nameof(Task.FromResult), new[] { typeof(bool) }, callExpr);
                }
                else if (returnType == typeof(void))
                {
                    // 方法返回 void，调用后返回 Task.FromResult(true)
                    var block = Expression.Block(
                        callExpr,
                        Expression.Field(null, typeof(CompileDelegateHelper), nameof(CompileDelegateHelper.TrueTask))
                    );
                    body = block;
                }
                else
                {
                    // 不支持的返回类型，跳过编译
                    return;
                }

                // 编译为委托
                var lambda = Expression.Lambda<Func<string, string, Task<bool>>>(
                    body, sourceParam, routingKeyParam);

                scanResult.CompiledDelegate = lambda.Compile();
            }
            catch (Exception ex)
            {
                // 编译失败时记录警告，回退到反射调用
                _logger.LogWarning(ex, "编译消费者方法委托失败，将回退到反射调用。Method: {Method}", scanResult.MethodInfo?.Name);
            }
        }
    }

    /// <summary>
    /// 编译委托辅助类，提供 Expression Tree 中使用的静态方法
    /// </summary>
    internal static class CompileDelegateHelper
    {
        /// <summary>
        /// 预创建的 Task.FromResult(true)，避免重复创建
        /// </summary>
        public static readonly Task<bool> TrueTask = Task.FromResult(true);

        /// <summary>
        /// 等待 Task 完成后返回 true
        /// </summary>
        public static async Task<bool> AwaitTaskAndReturnTrue(Task task)
        {
            await task.ConfigureAwait(false);
            return true;
        }
    }
}