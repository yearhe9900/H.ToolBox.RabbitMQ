using H.ToolBox.RabbitMQ.Consumer;

namespace H.ToolBox.RabbitMQ.WorkerService
{
    /// <summary>
    /// 消费者后台服务
    /// 继承自 <see cref="BackgroundService"/>，用于演示如何使用 <see cref="IConsumerService"/>
    /// 的 <see cref="IConsumerService.ListenAsync"/> 和 <see cref="IConsumerService.ListenByExchangeAsync"/> 方法消费消息
    /// </summary>
    public class ConsumerWorker : BackgroundService
    {
        /// <summary>
        /// 消费者服务，提供队列消费和路由消费 API
        /// </summary>
        private readonly IConsumerService _consumerService;

        /// <summary>
        /// 日志记录器
        /// </summary>
        private readonly ILogger<ConsumerWorker> _logger;

        /// <summary>
        /// 初始化消费者后台服务实例
        /// </summary>
        /// <param name="consumerService">消费者服务</param>
        /// <param name="logger">日志记录器</param>
        public ConsumerWorker(IConsumerService consumerService, ILogger<ConsumerWorker> logger)
        {
            _consumerService = consumerService;
            _logger = logger;
        }

        /// <summary>
        /// 执行后台服务的主要逻辑
        /// 同时启动队列消费和路由消费两个监听任务
        /// </summary>
        /// <param name="stoppingToken">停止令牌，用于取消后台任务</param>
        /// <returns>表示异步执行操作的任务</returns>
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 同时启动两个消费监听任务
            var queueTask = Task.Run(() => TestListenAsync(stoppingToken), stoppingToken);
            var exchangeTask = Task.Run(() => TestListenByExchangeAsync(stoppingToken), stoppingToken);

            // 等待任意一个任务完成（正常情况下不会完成，除非被取消）
            return Task.WhenAny(queueTask, exchangeTask);
        }

        /// <summary>
        /// 测试队列消费：通过 <see cref="IConsumerService.ListenAsync"/> 监听指定队列
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        private async Task TestListenAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("[ConsumerWorker] 启动队列消费监听...");

            await _consumerService.ListenAsync<string>(
                configId: "default",
                queueName: "hzptest.toolbox.q",
                allwaysRunActionTask: async (body, bodyString, routingKey) =>
                {
                    _logger.LogInformation(
                        "[队列消费] 收到消息 - Body: {Body}, BodyString: {BodyString}, RoutingKey: {RoutingKey}",
                        body, bodyString, routingKey);

                    // 返回 true 表示确认消息（Ack），false 表示拒绝消息（Reject）并重新入队
                    return true;
                },
                prefetchCount: 10,
                autoAck: false,
                durable: true,
                cancellationToken: cancellationToken);
        }

        /// <summary>
        /// 测试路由消费：通过 <see cref="IConsumerService.ListenByExchangeAsync"/> 通过交换机和路由键监听消息
        /// </summary>
        /// <param name="cancellationToken">取消令牌</param>
        private async Task TestListenByExchangeAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("[ConsumerWorker] 启动路由消费监听...");

            await _consumerService.ListenByExchangeAsync<string>(
                configId: "default",
                exchange: "MBExchange",
                exchangeType: "topic",
                routingKey: "hzptest.toolbox.routingkey",
                queueName: "hzptest.toolbox.exchange.q",
                allwaysRunActionTask: async (body, bodyString, routingKey) =>
                {
                    _logger.LogInformation(
                        "[路由消费] 收到消息 - Body: {Body}, BodyString: {BodyString}, RoutingKey: {RoutingKey}",
                        body, bodyString, routingKey);

                    // 返回 true 表示确认消息（Ack），false 表示拒绝消息（Reject）并重新入队
                    return true;
                },
                prefetchCount: 10,
                autoAck: false,
                durable: true,
                cancellationToken: cancellationToken);
        }
    }
}