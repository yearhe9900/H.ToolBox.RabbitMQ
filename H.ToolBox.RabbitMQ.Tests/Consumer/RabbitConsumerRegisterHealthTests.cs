using H.ToolBox.RabbitMQ.Channel;
using H.ToolBox.RabbitMQ.Consumer;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Concurrent;
using System.Reflection;

namespace H.ToolBox.RabbitMQ.Tests.Consumer;

/// <summary>
/// 消费者健康判定回归测试。
/// 旧实现用 _healthyConsumers == _totalConsumers 判断，任务异常时两个计数器同步递减，
/// 等式恒成立导致 IsHealthy() 永真、故障消费者永不重启。新版改为按实例维护状态字典。
/// 这里用反射直接设置内部状态，避免依赖真实 Broker / 异步时序，精确验证聚合逻辑。
/// </summary>
public class RabbitConsumerRegisterHealthTests
{
    private static RabbitConsumerRegister CreateRegister() => new(
        new Mock<ILogger<RabbitConsumerRegister>>().Object,
        new Mock<IServiceProvider>().Object,
        new Mock<IRabbitMqConsumerClientFactory>().Object,
        new Mock<IConnectionChannelPoolManager>().Object);

    private static void SetField(object target, string name, object? value) =>
        target.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(target, value);

    private static ConcurrentDictionary<string, bool> HealthOf(RabbitConsumerRegister register) =>
        (ConcurrentDictionary<string, bool>)register.GetType()
            .GetField("_consumerHealth", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(register)!;

    [Fact]
    public void IsHealthy_WhenNoConsumersScanned_ShouldReturnTrue()
    {
        var register = CreateRegister();
        SetField(register, "_isStarting", false);
        SetField(register, "_zeroConsumers", true);

        Assert.True(register.IsHealthy());
    }

    [Fact]
    public void IsHealthy_WhenAllInstancesHealthy_ShouldReturnTrue()
    {
        var register = CreateRegister();
        SetField(register, "_isStarting", false);
        SetField(register, "_zeroConsumers", false);
        var health = HealthOf(register);
        health["consumer-a"] = true;
        health["consumer-b"] = true;

        Assert.True(register.IsHealthy());
    }

    [Fact]
    public void IsHealthy_WhenOneInstanceUnhealthy_ShouldReturnFalse()
    {
        // 核心回归：只要有一个实例异常，整体即不健康，必须触发重启。
        // 旧实现中两个计数器同步递减会让 IsHealthy() 仍返回 true。
        var register = CreateRegister();
        SetField(register, "_isStarting", false);
        SetField(register, "_zeroConsumers", false);
        var health = HealthOf(register);
        health["consumer-a"] = true;
        health["consumer-b"] = false; // 一个实例运行异常

        Assert.False(register.IsHealthy());
    }

    [Fact]
    public void IsHealthy_WhileStarting_ShouldReturnTrue()
    {
        // 启动窗口内不做判定，避免注册尚未完成就触发重启
        var register = CreateRegister();
        SetField(register, "_isStarting", true);
        SetField(register, "_zeroConsumers", false);
        var health = HealthOf(register);
        health["consumer-a"] = false;

        Assert.True(register.IsHealthy());
    }

    [Fact]
    public void IsHealthy_WhenAllInstancesFailed_ShouldReturnFalse()
    {
        var register = CreateRegister();
        SetField(register, "_isStarting", false);
        SetField(register, "_zeroConsumers", false);
        var health = HealthOf(register);
        health["consumer-a"] = false;
        health["consumer-b"] = false;

        Assert.False(register.IsHealthy());
    }

    [Fact]
    public void IsHealthy_WhenConsumersExpectedButNoneRegistered_ShouldReturnFalse()
    {
        // 期望有消费者但健康字典为空（全部实例异常退出/取消）必须判定不健康，触发重启
        var register = CreateRegister();
        SetField(register, "_isStarting", false);
        SetField(register, "_zeroConsumers", false);
        Assert.True(HealthOf(register).IsEmpty);

        Assert.False(register.IsHealthy());
    }
}
