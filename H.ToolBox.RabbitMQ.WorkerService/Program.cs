using H.ToolBox.RabbitMQ;
using H.ToolBox.RabbitMQ.Application;
using H.ToolBox.RabbitMQ.Model;
using H.ToolBox.RabbitMQ.WorkerService;

// 创建应用程序主机构建器，自动加载 appsettings.json 等配置文件
var builder = Host.CreateApplicationBuilder(args);

// 注册消费者服务：将 TestClass 作为 IToolBoxRabbitMQ 的实现注册到 DI 容器
// Bootstrapper 启动时会自动扫描此接口的所有实现类，发现带有 [RabbitMQConsumer] 特性的方法并注册为消费者
builder.Services.AddSingleton<IToolBoxRabbitMQ, TestClass>();

// =============================================
// 多集群模式：从配置文件读取 RabbitMQConfig 数组
// 每个集群通过 ConfigId 区分，消费者特性通过 ConfigId 绑定到对应集群
// =============================================
builder.Services.AddRabbitMQ(configs =>
{
    // 从 appsettings.json 的 RabbitMQConfig 数组读取所有集群配置
    var clusterConfigs = builder.Configuration.GetSection("RabbitMQConfig").Get<List<RabbitMQOptions>>();
    if (clusterConfigs != null)
    {
        foreach (var config in clusterConfigs)
        {
            configs.Add(config);
        }
    }
});

// 注册 Worker 后台服务，用于演示消息发布（生产者）功能
builder.Services.AddHostedService<Worker>();

// 注册 ConsumerWorker 后台服务，用于演示 IConsumerService 的 ListenAsync 和 ListenByExchangeAsync 方法
// builder.Services.AddHostedService<ConsumerWorker>();

// 构建并运行主机，启动所有后台服务（Bootstrapper 负责启动消费者，Worker 负责发送消息）
var host = builder.Build();
host.Run();