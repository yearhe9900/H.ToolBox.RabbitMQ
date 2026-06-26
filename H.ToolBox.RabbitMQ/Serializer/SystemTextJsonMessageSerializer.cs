using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace H.ToolBox.RabbitMQ.Serializer
{
    /// <summary>
    /// 基于 System.Text.Json 的默认消息序列化器实现。
    /// 使用 <see cref="JavaScriptEncoder.UnsafeRelaxedJsonEscaping"/> 避免对中文等非 ASCII 字符进行转义。
    /// </summary>
    public class SystemTextJsonMessageSerializer : IMessageSerializer
    {
        private static readonly JsonSerializerOptions s_jsonOptions = new JsonSerializerOptions
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        /// <inheritdoc />
        public ReadOnlyMemory<byte> Serialize<T>(T msg) where T : class
        {
            string json = JsonSerializer.Serialize(msg, s_jsonOptions);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            return new ReadOnlyMemory<byte>(bytes);
        }

        /// <inheritdoc />
        public T Deserialize<T>(ReadOnlySpan<byte> body)
        {
            var targetType = typeof(T);

            if (targetType == typeof(string))
            {
                return (T)(object)Encoding.UTF8.GetString(body);
            }

            if (targetType == typeof(byte[]))
            {
                return (T)(object)body.ToArray();
            }

            return JsonSerializer.Deserialize<T>(body)
                ?? throw new JsonException($"无法将消息体反序列化为类型 {targetType.FullName}");
        }
    }
}
