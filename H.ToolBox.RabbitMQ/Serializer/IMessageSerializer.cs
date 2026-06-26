namespace H.ToolBox.RabbitMQ.Serializer
{
    /// <summary>
    /// 消息序列化器接口，定义消息序列化与反序列化的标准契约。
    /// 默认使用 <see cref="SystemTextJsonMessageSerializer"/>（基于 System.Text.Json），
    /// 用户可通过依赖注入替换为自定义实现（如 Newtonsoft.Json 等）。
    /// </summary>
    public interface IMessageSerializer
    {
        /// <summary>
        /// 将消息对象序列化为 UTF-8 编码的字节数组
        /// </summary>
        /// <typeparam name="T">消息对象类型</typeparam>
        /// <param name="msg">待序列化的消息对象</param>
        /// <returns>序列化后的字节内存块</returns>
        ReadOnlyMemory<byte> Serialize<T>(T msg) where T : class;

        /// <summary>
        /// 将字节内容反序列化为目标类型
        /// </summary>
        /// <typeparam name="T">目标类型</typeparam>
        /// <param name="body">消息体字节内容</param>
        /// <returns>反序列化后的消息体对象</returns>
        T Deserialize<T>(ReadOnlySpan<byte> body);
    }
}
