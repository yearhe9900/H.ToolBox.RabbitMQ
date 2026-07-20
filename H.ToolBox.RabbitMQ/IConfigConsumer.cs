namespace H.ToolBox.RabbitMQ
{
    /// <summary>
    /// 配置驱动的RabbitMQ消费者服务标记接口
    /// 实现此接口的类将被自动扫描，通过配置文件中的ConfigId、QueueName、类名和方法名注册为RabbitMQ消费者
    /// </summary>
    public interface IConfigConsumer
    {
    }
}
