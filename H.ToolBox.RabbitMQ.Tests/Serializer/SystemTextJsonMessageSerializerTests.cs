using H.ToolBox.RabbitMQ.Serializer;

namespace H.ToolBox.RabbitMQ.Tests.Serializer;

public class SystemTextJsonMessageSerializerTests
{
    private readonly SystemTextJsonMessageSerializer _serializer = new();

    [Fact]
    public void Serialize_ShouldReturnNonEmptyBytes()
    {
        // Arrange
        var obj = new TestMessage { Name = "test", Value = 42 };

        // Act
        var result = _serializer.Serialize(obj);

        // Assert
        Assert.False(result.IsEmpty);
        Assert.True(result.Length > 0);
    }

    [Fact]
    public void SerializeAndDeserialize_ShouldRoundTrip()
    {
        // Arrange
        var obj = new TestMessage { Name = "hello", Value = 123 };

        // Act
        var bytes = _serializer.Serialize(obj);
        var deserialized = _serializer.Deserialize<TestMessage>(bytes.Span);

        // Assert
        Assert.Equal(obj.Name, deserialized.Name);
        Assert.Equal(obj.Value, deserialized.Value);
    }

    [Fact]
    public void Serialize_ShouldPreserveChineseCharacters()
    {
        // Arrange
        var obj = new TestMessage { Name = "中文测试", Value = 1 };

        // Act
        var bytes = _serializer.Serialize(obj);
        var json = System.Text.Encoding.UTF8.GetString(bytes.Span);

        // Assert - UnsafeRelaxedJsonEscaping should not escape Chinese chars
        Assert.Contains("中文测试", json);
    }

    [Fact]
    public void Deserialize_WithUtf8Bytes_ShouldWork()
    {
        // Arrange
        var json = """{"Name":"test","Value":99}"""u8;

        // Act
        var result = _serializer.Deserialize<TestMessage>(json);

        // Assert
        Assert.Equal("test", result.Name);
        Assert.Equal(99, result.Value);
    }

    [Fact]
    public void Deserialize_ToString_ShouldReturnRawJson()
    {
        // Arrange
        var jsonBytes = "hello world"u8.ToArray();

        // Act
        var result = _serializer.Deserialize<string>(jsonBytes);

        // Assert
        Assert.Equal("hello world", result);
    }

    [Fact]
    public void Deserialize_ToByteArray_ShouldReturnSameBytes()
    {
        // Arrange
        var original = new byte[] { 1, 2, 3, 4, 5 };

        // Act
        var result = _serializer.Deserialize<byte[]>(original);

        // Assert
        Assert.Equal(original, result);
    }

    [Fact]
    public void Deserialize_InvalidJson_ShouldThrow()
    {
        // Arrange
        var invalidJson = "not valid json"u8.ToArray();

        // Act & Assert
        Assert.ThrowsAny<Exception>(() => _serializer.Deserialize<TestMessage>(invalidJson));
    }

    private class TestMessage
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }
}
