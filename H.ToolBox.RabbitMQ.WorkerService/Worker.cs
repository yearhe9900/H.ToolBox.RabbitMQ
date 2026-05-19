using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.Transport;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace H.ToolBox.RabbitMQ.WorkerService
{
    /// <summary>
    /// Worker 后台服务
    /// 继承自 <see cref="BackgroundService"/>，用于演示如何使用 <see cref="IRabbitMqTransport"/>
    /// 发送消息到 RabbitMQ（生产者端），支持多集群发送
    /// </summary>
    public class Worker : BackgroundService
    {
        /// <summary>
        /// RabbitMQ 消息传输服务，用于发送消息到 RabbitMQ
        /// </summary>
        private readonly IRabbitMqTransport _rabbitMqTransport;

        /// <summary>
        /// 初始化 Worker 实例
        /// </summary>
        /// <param name="rabbitMqTransport">RabbitMQ 消息传输服务</param>
        public Worker(IRabbitMqTransport rabbitMqTransport)
        {
            _rabbitMqTransport = rabbitMqTransport;
        }

        /// <summary>
        /// 执行后台服务的主要逻辑
        /// 循环发送消息到 RabbitMQ，演示多集群发送方式
        /// </summary>
        /// <param name="stoppingToken">停止令牌，用于取消后台任务</param>
        /// <returns>表示异步执行操作的任务</returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 配置 JSON 序列化选项，使用 UnsafeRelaxedJsonEscaping 保留中文等非 ASCII 字符，避免被转义为 \uXXXX
            var jsonOptions = new JsonSerializerOptions
            {
                Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };

            // 1. 创建测试数据对象
            var user = new { Id = 1, Name = "张三", Age = 25 };

            // 2. 对象 → JSON 字符串 → 字节数组
            string json = JsonSerializer.Serialize(user, jsonOptions);
            byte[] bytes = Encoding.UTF8.GetBytes(json);

            // 3. 字节数组 → ReadOnlyMemory<byte>，作为消息体
            ReadOnlyMemory<byte> memory = bytes;

            await Parallel.ForEachAsync(Enumerable.Range(0, 20), async (i, ct) =>
               {
                   while (true)
                   {
                       // ========== 发送到默认集群（队列模式） ==========
                       // 默认 ConfigId = "default"
                       var result = await _rabbitMqTransport.SendAsync(new TransportMessage(memory)
                       {
                           Durable = true,
                           QueueName = "hzptest.toolbox.q",
                           Exchange = "",
                           RoutingKey = "hzptest.toolbox.q",
                       });
                       Console.WriteLine($"[default] 队列模式发送结果: {result.Succeeded}");
                   }
               });

            //while (true)
            //{
            //    // ========== 发送到默认集群（队列模式） ==========
            //    // 默认 ConfigId = "default"
            //    var result = await _rabbitMqTransport.SendAsync(new TransportMessage(memory)
            //    {
            //        Durable = true,
            //        QueueName = "hzptest.toolbox.q",
            //        Exchange = "",
            //        RoutingKey = "hzptest.toolbox.q",
            //    });
            //    Console.WriteLine($"[default] 队列模式发送结果: {result.Succeeded}");

            //    // ========== 发送到默认集群（交换机模式） ==========
            //    var result2 = await _rabbitMqTransport.SendAsync(new TransportMessage(memory)
            //    {
            //        Durable = true,
            //        QueueName = "",
            //        Exchange = "MBExchange",
            //        ExchangeType = ExchangeTypeConst.Topic,
            //        RoutingKey = "hzptest.toolbox.routingkey",
            //    });
            //    Console.WriteLine($"[default] 交换机模式发送结果: {result2.Succeeded}");


            //    // ========== 发送到 default2 集群 ==========
            //    // 通过 ConfigId = "default2" 指定发送到第二个集群
            //    var result3 = await _rabbitMqTransport.SendAsync(new TransportMessage(memory)
            //    {
            //        ConfigId = "default2",
            //        Durable = true,
            //        QueueName = "hzptest.cluster2.q",
            //        Exchange = "",
            //        RoutingKey = "hzptest.cluster2.q",
            //    });
            //    Console.WriteLine($"[default2] 队列模式发送结果: {result3.Succeeded}");

            //    Thread.Sleep(5000);
            //}
        }
    }
}