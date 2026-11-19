using H.ToolBox.RabbitMQ.Consumer;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 方法匹配缓存
    /// 缓存消费者名称（ConsumerName）到方法扫描结果（<see cref="MethodScanResult"/>）的映射关系，
    /// 避免每次消息到达时重复进行反射扫描，提高消息分发的性能
    /// </summary>
    /// <remarks>
    /// 工作流程：
    /// <list type="number">
    ///   <item><description>启动时调用 <see cref="TrySetExecutor()"/> 全量重建缓存</description></item>
    ///   <item><description>消息到达时调用 <see cref="TryGetExecutor"/> 根据消费者标签快速查找对应的方法执行器</description></item>
    /// </list>
    /// 每次重建都先构造一份新字典再原子替换，既清掉上一批次的残留条目，
    /// 也避免重启期间消费者读到半更新状态。
    /// </remarks>
    public class MethodMatcherCache
    {
        /// <summary>
        /// 消费者服务选择器，用于扫描和发现消费者方法
        /// </summary>
        private readonly IConsumerServiceSelector _selector;

        private ConcurrentDictionary<string, MethodScanResult> _entries =
            new(StringComparer.Ordinal);

        /// <summary>
        /// 初始化方法匹配缓存实例
        /// </summary>
        /// <param name="selector">消费者服务选择器，用于执行方法扫描</param>
        public MethodMatcherCache(IConsumerServiceSelector selector)
        {
            _selector = selector;
        }

        /// <summary>
        /// 获取内部缓存字典，键为消费者名称（ConsumerName），值为方法扫描结果
        /// </summary>
        private ConcurrentDictionary<string, MethodScanResult> Entries => _entries;

        /// <summary>
        /// 全量重建缓存：扫描所有消费者方法，按 ConsumerName 建立映射。
        /// </summary>
        /// <returns>本次扫描生成的缓存字典</returns>
        /// <exception cref="InvalidOperationException">
        /// 当两个不同的消费者方法使用了相同的 ConsumerName 时抛出（fail-fast），
        /// 避免旧实现中 <c>Entries[name] = ...</c> 静默覆盖导致后注册的类库消费者丢失。
        /// </exception>
        public ConcurrentDictionary<string, MethodScanResult> TrySetExecutor()
        {
            var methodScanResults = _selector.SelectCandidates();
            var rebuilt = new ConcurrentDictionary<string, MethodScanResult>(StringComparer.Ordinal);

            foreach (var methodScanResult in methodScanResults)
            {
                // 跳过无效的扫描结果（缺少方法信息或消费者特性）
                if (methodScanResult.MethodInfo == null || methodScanResult.ConsumerAttribute == null)
                {
                    continue;
                }

                var name = methodScanResult.ConsumerAttribute.ConsumerName;
                if (string.IsNullOrWhiteSpace(name))
                {
                    // 使用 FullName 而不是短类名：不同程序集里同名类（如各业务的 OrderConsumer）
                    // 用短类名会撞名，导致先注册的实现被静默覆盖。
                    var declaringType = methodScanResult.MethodInfo.DeclaringType;
                    name = declaringType?.FullName != null
                        ? $"{declaringType.FullName}.{methodScanResult.MethodInfo.Name}"
                        : methodScanResult.MethodInfo.Name;
                    methodScanResult.ConsumerAttribute.ConsumerName = name;
                }

                if (rebuilt.TryGetValue(name, out var existing))
                {
                    // 同一次扫描中重复出现的完全相同的条目（例如 DI 与程序集扫描兜底命中同一实例）不算冲突
                    if (ReferenceEquals(existing, methodScanResult)
                        || (existing.MethodInfo == methodScanResult.MethodInfo
                            && ReferenceEquals(existing.Instance, methodScanResult.Instance)))
                    {
                        continue;
                    }

                    throw new InvalidOperationException(
                        $"检测到重复的 ConsumerName \"{name}\"：" +
                        $"{Describe(existing)} 与 {Describe(methodScanResult)} 冲突。" +
                        "请为其中一个消费者方法显式设置唯一的 ConsumerName，" +
                        "否则消息会被路由到错误的处理器。");
                }

                rebuilt[name] = methodScanResult;
            }

            // 原子替换：清掉上一批次残留，且不会与 TryGetExecutor 产生半更新状态
            Interlocked.Exchange(ref _entries, rebuilt);
            return rebuilt;
        }

        /// <summary>
        /// 按指定的 ConfigId 筛选消费者方法，返回属于该集群的缓存字典
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <returns>筛选后的缓存字典，仅包含属于指定集群的消费者方法</returns>
        public ConcurrentDictionary<string, MethodScanResult> TrySetExecutor(string configId)
        {
            var allResults = TrySetExecutor();
            var filtered = new ConcurrentDictionary<string, MethodScanResult>(StringComparer.Ordinal);

            foreach (var kvp in allResults)
            {
                if (kvp.Value.ConsumerAttribute?.ConfigId == configId)
                {
                    filtered.TryAdd(kvp.Key, kvp.Value);
                }
            }

            return filtered;
        }

        /// <summary>
        /// 尝试根据消费者名称从缓存中获取方法扫描结果
        /// </summary>
        /// <param name="name">消费者名称（对应 <see cref="RabbitMQConsumerAttribute.ConsumerName"/>）</param>
        /// <param name="methodScanResult">
        /// 输出参数，如果找到则返回对应的方法扫描结果，否则为 null
        /// </param>
        /// <returns>如果找到匹配的方法扫描结果返回 true，否则返回 false</returns>
        public bool TryGetExecutor(string name,
            [NotNullWhen(true)] out MethodScanResult? methodScanResult)
        {
            if (Entries.TryGetValue(name, out var entry))
            {
                methodScanResult = entry;
                return methodScanResult != null;
            }

            methodScanResult = null;
            return false;
        }

        private static string Describe(MethodScanResult result)
        {
            var type = result.MethodInfo?.DeclaringType?.FullName ?? "?";
            var method = result.MethodInfo?.Name ?? "?";
            return $"{type}.{method}";
        }
    }
}
