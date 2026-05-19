namespace H.ToolBox.RabbitMQ.Model
{
    /// <summary>
    /// RabbitMQ交换机类型常量定义
    /// 提供RabbitMQ支持的四种标准交换机类型
    /// </summary>
    public static class ExchangeTypeConst
    {
        /// <summary>
        /// 直连交换机：根据RoutingKey精确匹配进行路由
        /// </summary>
        public const string Direct = "direct";
        
        /// <summary>
        /// 扇形交换机：将消息广播到所有绑定的队列，忽略RoutingKey
        /// </summary>
        public const string Fanout = "fanout";
        
        /// <summary>
        /// 头交换机：根据消息头部属性（headers）进行匹配路由，而非RoutingKey
        /// </summary>
        public const string Headers = "headers";
        
        /// <summary>
        /// 主题交换机：使用通配符模式（*和#）对RoutingKey进行模式匹配路由
        /// </summary>
        public const string Topic = "topic";
    }
}