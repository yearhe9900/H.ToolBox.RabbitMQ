using H.ToolBox.RabbitMQ.Model;

namespace H.ToolBox.RabbitMQ.Application
{
    /// <summary>
    /// 测试消费者类
    /// 实现 <see cref="IToolBoxRabbitMQ"/> 标记接口，其方法通过 <see cref="RabbitMQConsumerAttribute"/> 特性
    /// 标记为 RabbitMQ 消费者，会被自动扫描并注册
    /// </summary>
    /// <remarks>
    /// 该类演示了两种消费模式的使用方式：
    /// <list type="bullet">
    ///   <item><description><see cref="TestQueue"/> - 队列模式：从默认集群消费消息</description></item>
    ///   <item><description><see cref="TestExchange"/> - 交换机模式：从默认集群消费消息</description></item>
    ///   <item><description><see cref="TestCluster2Queue"/> - 队列模式：从 cluster2 集群消费消息</description></item>
    /// </list>
    /// </remarks>
    public class TestClass : IToolBoxRabbitMQ
    {
        /// <summary>
        /// 队列模式消费者示例（默认集群）
        /// 直接从指定队列 "hzptest.toolbox.q" 消费消息，ConfigId 默认为 "default"
        /// </summary>
        /// <param name="source">消息内容（UTF-8 字符串）</param>
        /// <param name="routingKey">消息的路由键</param>
        [RabbitMQConsumer(QueueName = "hzptest.toolbox.q", ConsumerName = "hzptest.toolbox.q.consumer", AutoAck = false, Durable = true, ListenerCount = 1, PrefetchCount = 1000, WaitOneTimeOut = 30,ConsumerConcurrent =(byte)10)]
        public async Task<bool> TestQueue(string source, string routingKey)
        {
            Console.WriteLine($"[default]A TestQueue called with name: {source}, routing key: {routingKey},time：{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
            await Task.Delay(1000);
            Console.WriteLine($"[default]B TestQueue called with name: {source}, routing key: {routingKey},time：{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}");
            return true;
        }

        ///// <summary>
        ///// 交换机模式消费者示例（默认集群）
        ///// 通过 Topic 交换机 "MBExchange" 和路由键消费消息
        ///// </summary>
        ///// <param name="source">消息内容（UTF-8 字符串）</param>
        ///// <param name="routingKey">消息的路由键</param>
        //[RabbitMQConsumer(QueueName = "hzptest.toolbox.f", Exchange = "MBExchange", RoutingKey = "hzptest.toolbox.routingkey", ExchangeType = ExchangeTypeConst.Topic, ConsumerName = "hzptest.toolbox.routingkey.consumer", Durable = true, ListenerCount = 1, WaitOneTimeOut = 30)]
        //public void TestExchange(string source, string routingKey)
        //{
        //     Console.WriteLine($"[default] TestExchange called with name: {source}, routing key: {routingKey}");
        //}

        ///// <summary>
        ///// 队列模式消费者示例（default2 集群）
        ///// 通过 ConfigId = "default2" 指定连接到第二个 RabbitMQ 集群
        ///// </summary>
        ///// <param name="source">消息内容（UTF-8 字符串）</param>
        ///// <param name="routingKey">消息的路由键</param>
        //[RabbitMQConsumer(ConfigId = "default2", QueueName = "hzptest.cluster2.q", ConsumerName = "hzptest.cluster2.q.consumer", AutoAck = true, Durable = true, ListenerCount = 1, WaitOneTimeOut = 30)]
        //public void TestCluster2Queue(string source, string routingKey)
        //{
        //      Console.WriteLine($"[cluster2] TestCluster2Queue called with name: {source}, routing key: {routingKey}");
        //}
    }
}