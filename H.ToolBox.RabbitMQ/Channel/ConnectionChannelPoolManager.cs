using H.ToolBox.RabbitMQ.Model;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace H.ToolBox.RabbitMQ.Channel
{
    /// <summary>
    /// 连接通道池管理器实现
    /// 管理多个 RabbitMQ 集群的连接通道池，通过 ConfigId 区分不同集群
    /// </summary>
    public class ConnectionChannelPoolManager : IConnectionChannelPoolManager, IDisposable
    {
        private readonly ConcurrentDictionary<string, IConnectionChannelPool> _pools = new();
        private readonly ConcurrentDictionary<string, IConnectionChannelPool> _producerPools = new();
        private readonly ConcurrentDictionary<string, RabbitMQOptions> _options = new();
        private readonly ILogger<ConnectionChannelPool> _poolLogger;

        /// <summary>
        /// 初始化连接通道池管理器
        /// </summary>
        /// <param name="poolLogger">连接通道池的日志记录器</param>
        public ConnectionChannelPoolManager(ILogger<ConnectionChannelPool> poolLogger)
        {
            _poolLogger = poolLogger;
        }

        /// <summary>
        /// 根据 ConfigId 获取对应的连接通道池（消费者专用）
        /// 如果对应的池不存在，则使用提供的配置创建一个新的连接通道池
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <returns>对应集群的连接通道池</returns>
        /// <exception cref="KeyNotFoundException">当指定的 ConfigId 未注册时抛出</exception>
        public IConnectionChannelPool GetPool(string configId = "default")
        {
            if (_pools.TryGetValue(configId, out var pool))
            {
                return pool;
            }

            throw new KeyNotFoundException($"未找到 ConfigId 为 '{configId}' 的连接通道池，请确保已通过 AddRabbitMQ 注册该集群配置。");
        }

        /// <summary>
        /// 根据 ConfigId 获取对应的生产者专用连接通道池
        /// 生产者池与消费者池使用独立的 TCP 连接，避免连接级背压相互影响
        /// </summary>
        /// <param name="configId">集群配置标识，默认为 "default"</param>
        /// <returns>对应集群的生产者专用连接通道池</returns>
        /// <exception cref="KeyNotFoundException">当指定的 ConfigId 未注册时抛出</exception>
        public IConnectionChannelPool GetProducerPool(string configId = "default")
        {
            if (_producerPools.TryGetValue(configId, out var pool))
            {
                return pool;
            }

            throw new KeyNotFoundException($"未找到 ConfigId 为 '{configId}' 的生产者连接通道池，请确保已通过 AddRabbitMQ 注册该集群配置。");
        }

        /// <summary>
        /// 根据 ConfigId 获取对应的 RabbitMQ 配置选项
        /// </summary>
        /// <param name="configId">集群配置标识，默认为 "default"</param>
        /// <returns>对应集群的配置选项</returns>
        /// <exception cref="KeyNotFoundException">当指定的 ConfigId 未注册时抛出</exception>
        public RabbitMQOptions GetOptions(string configId = "default")
        {
            if (_options.TryGetValue(configId, out var options))
            {
                return options;
            }

            throw new KeyNotFoundException($"未找到 ConfigId 为 '{configId}' 的集群配置选项，请确保已通过 AddRabbitMQ 注册该集群配置。");
        }

        /// <summary>
        /// 注册指定 ConfigId 的连接通道池
        /// </summary>
        /// <param name="configId">集群配置标识</param>
        /// <param name="options">RabbitMQ 配置选项</param>
        /// <returns>注册的连接通道池实例</returns>
        /// <exception cref="ArgumentException">当 configId 为空时抛出</exception>
        /// <exception cref="InvalidOperationException">当 configId 已注册时抛出</exception>
        public IConnectionChannelPool RegisterPool(string configId, RabbitMQOptions options)
        {
            if (string.IsNullOrWhiteSpace(configId))
                throw new ArgumentException("ConfigId 不能为空。", nameof(configId));

            var pool = new ConnectionChannelPool(_poolLogger, options);
            var producerPool = new ConnectionChannelPool(_poolLogger, options);

            if (!_pools.TryAdd(configId, pool))
            {
                pool.Dispose();
                producerPool.Dispose();
                throw new InvalidOperationException($"ConfigId 为 '{configId}' 的连接通道池已注册，不可重复注册。");
            }

            if (!_producerPools.TryAdd(configId, producerPool))
            {
                _pools.TryRemove(configId, out _);
                pool.Dispose();
                producerPool.Dispose();
                throw new InvalidOperationException($"ConfigId 为 '{configId}' 的生产者连接通道池已注册，不可重复注册。");
            }

            _options.TryAdd(configId, options);

            return pool;
        }

        /// <summary>
        /// 释放所有连接通道池
        /// </summary>
        public void Dispose()
        {
            foreach (var kvp in _pools)
            {
                if (kvp.Value is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _pools.Clear();

            foreach (var kvp in _producerPools)
            {
                if (kvp.Value is IDisposable disposable)
                {
                    disposable.Dispose();
                }
            }
            _producerPools.Clear();
        }
    }
}