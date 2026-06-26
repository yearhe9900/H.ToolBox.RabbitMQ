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
    ///   <item><description>启动时调用 <see cref="TrySetExecutor"/> 扫描所有消费者方法并缓存到字典中</description></item>
    ///   <item><description>消息到达时调用 <see cref="TryGetExecutor"/> 根据消费者标签快速查找对应的方法执行器</description></item>
    /// </list>
    /// 使用 <see cref="ConcurrentDictionary{TKey,TValue}"/> 作为底层存储，支持线程安全的并发访问。
    /// </remarks>
    public class MethodMatcherCache
    {
        /// <summary>
        /// 消费者服务选择器，用于扫描和发现消费者方法
        /// </summary>
        private readonly IConsumerServiceSelector _selector;

        /// <summary>
        /// 初始化方法匹配缓存实例
        /// </summary>
        /// <param name="selector">消费者服务选择器，用于执行方法扫描</param>
        public MethodMatcherCache(IConsumerServiceSelector selector)
        {
            _selector = selector;
            Entries = new ConcurrentDictionary<string, MethodScanResult>();
        }

        /// <summary>
        /// 获取内部缓存字典，键为消费者名称（ConsumerName），值为方法扫描结果
        /// </summary>
        private ConcurrentDictionary<string, MethodScanResult> Entries { get; }


        /// <summary>
        /// 初始化并填充缓存
        /// 通过 <see cref="IConsumerServiceSelector.SelectCandidates"/> 扫描所有消费者方法，
        /// 并将其按 ConsumerName 缓存到字典中
        /// </summary>
        /// <returns>填充完成的缓存字典，键为消费者名称，值为方法扫描结果</returns>
        /// <exception cref="InvalidOperationException">当存在重复的 ConsumerName 时抛出</exception>
        public ConcurrentDictionary<string, MethodScanResult> TrySetExecutor()
        {
            var methodScanResults = _selector.SelectCandidates();
            foreach (var methodScanResult in methodScanResults)
            {
                // 跳过无效的扫描结果（缺少方法信息或消费者特性）
                if (methodScanResult.MethodInfo == null || methodScanResult.ConsumerAttribute == null)
                {
                    continue;
                }

                var name = methodScanResult.ConsumerAttribute.ConsumerName;
                if (string.IsNullOrEmpty(name))
                {
                    // 自动生成稳定的消费者名称：类名.方法名
                    var declaringType = methodScanResult.MethodInfo.DeclaringType;
                    name = declaringType != null
                        ? $"{declaringType.Name}.{methodScanResult.MethodInfo.Name}"
                        : methodScanResult.MethodInfo.Name;
                    methodScanResult.ConsumerAttribute.ConsumerName = name;
                }

                // 使用索引器确保重启时更新已有条目（TryAdd 不会覆盖旧值）
                Entries[name] = methodScanResult;
            }
            return Entries;
        }

        /// <summary>
        /// 按指定的 ConfigId 筛选消费者方法，返回属于该集群的缓存字典
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <returns>筛选后的缓存字典，仅包含属于指定集群的消费者方法</returns>
        public ConcurrentDictionary<string, MethodScanResult> TrySetExecutor(string configId)
        {
            var allResults = TrySetExecutor();
            var filtered = new ConcurrentDictionary<string, MethodScanResult>();

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
        /// <exception cref="ArgumentNullException">当内部缓存字典为 null 时抛出（理论上不会发生）</exception>
        public bool TryGetExecutor(string name,
            [NotNullWhen(true)] out MethodScanResult? methodScanResult)
        {
            if (Entries == null) throw new ArgumentNullException(nameof(Entries));

            methodScanResult = null;

            if (Entries.TryGetValue(name, out var entry))
            {
                methodScanResult = entry;
                return methodScanResult != null;
            }

            return false;
        }
    }
}