using H.ToolBox.RabbitMQ.Channel;
using RabbitMQ.Client;
using System.Collections.Generic;

namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// 死信（DLX）拓扑的统一构建与声明入口。
    /// </summary>
    /// <remarks>
    /// 生产者和消费者都会声明业务队列，而 <c>x-dead-letter-exchange</c> /
    /// <c>x-dead-letter-routing-key</c> 属于队列级参数：两次声明只要有一处不同，
    /// RabbitMQ 就会返回 PRECONDITION_FAILED 并关闭通道。因此所有声明路径
    /// （<see cref="Transport.IRabbitMqTransport"/>、<see cref="Consumer.IRabbitMqConsumerClient"/>、
    /// <see cref="Consumer.IConsumerService"/>）都必须经由本类，并传入同一个 ConfigId 下
    /// 的 <see cref="RabbitMQOptions"/> 实例。
    ///
    /// 拓扑约定：
    ///   · 死信交换机：Options.DeadLetterExchange，durable 的 direct 交换机；
    ///   · 死信队列：业务队列名 + Options.DeadLetterQueueSuffix；
    ///   · 绑定键：死信队列名本身（每个业务队列有独立 DLQ，不会互相混淆）；
    ///   · 死信交换机与死信队列固定按 durable=true 声明，与业务队列的 Durable 无关，
    ///     避免不同声明方传入不同 Durable 导致交换机声明冲突。
    ///   · 死信队列本身不再附加 DLX 参数，避免形成死信环路。
    /// </remarks>
    public static class DeadLetterTopology
    {
        /// <summary>
        /// 计算业务队列对应的死信队列名。
        /// </summary>
        /// <param name="queueName">业务队列名</param>
        /// <param name="options">集群配置（提供后缀）</param>
        /// <returns>死信队列名</returns>
        public static string GetDeadLetterQueueName(string queueName, RabbitMQOptions options)
        {
            return queueName + options.DeadLetterQueueSuffix;
        }

        /// <summary>
        /// 构建业务队列的声明参数：在队列类型参数（如 quorum）之上叠加死信参数。
        /// 当 <see cref="RabbitMQOptions.EnableDeadLetter"/> 为 false 时，行为与
        /// <see cref="QueueArgumentsHelper.BuildArguments(QueueType, int?)"/> 完全一致。
        /// </summary>
        /// <param name="queueName">业务队列名，用于推导死信队列名与 routing key</param>
        /// <param name="queueType">队列类型</param>
        /// <param name="quorumGroupSize">仲裁队列初始组大小</param>
        /// <param name="options">集群配置</param>
        /// <returns>队列声明参数；Classic 且未启用死信时返回 null</returns>
        public static IDictionary<string, object?>? BuildQueueArguments(
            string queueName, QueueType queueType, int? quorumGroupSize, RabbitMQOptions options)
        {
            var arguments = QueueArgumentsHelper.BuildArguments(queueType, quorumGroupSize);

            if (!options.EnableDeadLetter)
            {
                return arguments;
            }

            arguments ??= new Dictionary<string, object?>();
            arguments["x-dead-letter-exchange"] = options.DeadLetterExchange;
            arguments["x-dead-letter-routing-key"] = GetDeadLetterQueueName(queueName, options);
            return arguments;
        }

        /// <summary>
        /// 在原始通道上声明死信交换机、死信队列及其绑定（消费者路径，每次监听只调用一次）。
        /// </summary>
        public static async Task EnsureAsync(
            IChannel channel,
            string queueName,
            QueueType queueType,
            int? quorumGroupSize,
            RabbitMQOptions options,
            CancellationToken cancellationToken = default)
        {
            if (!options.EnableDeadLetter) return;

            var dlx = options.DeadLetterExchange;
            var dlq = GetDeadLetterQueueName(queueName, options);

            await channel.ExchangeDeclareAsync(
                exchange: dlx,
                type: options.DeadLetterExchangeType,
                durable: true,
                autoDelete: false,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            // 死信队列不附加 DLX 参数，避免死信消息再次被死信形成环路
            await channel.QueueDeclareAsync(
                queue: dlq,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: QueueArgumentsHelper.BuildArguments(queueType, quorumGroupSize),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await channel.QueueBindAsync(
                queue: dlq,
                exchange: dlx,
                routingKey: dlq,
                arguments: null,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// 在带声明缓存的通道上声明死信拓扑（生产者路径，避免每次发布都产生网络往返）。
        /// </summary>
        internal static async Task EnsureAsync(
            CachedDeclareChannel channel,
            string queueName,
            QueueType queueType,
            int? quorumGroupSize,
            RabbitMQOptions options,
            CancellationToken cancellationToken = default)
        {
            if (!options.EnableDeadLetter) return;

            var dlx = options.DeadLetterExchange;
            var dlq = GetDeadLetterQueueName(queueName, options);

            await channel.ExchangeDeclareAsync(
                dlx, options.DeadLetterExchangeType, durable: true, autoDelete: false,
                cancellationToken).ConfigureAwait(false);

            await channel.QueueDeclareAsync(
                dlq, durable: true, exclusive: false, autoDelete: false, queueType,
                QueueArgumentsHelper.BuildArguments(queueType, quorumGroupSize),
                cancellationToken).ConfigureAwait(false);

            await channel.QueueBindAsync(dlq, dlx, dlq, cancellationToken).ConfigureAwait(false);
        }
    }
}
