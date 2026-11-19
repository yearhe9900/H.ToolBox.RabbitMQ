using System.Diagnostics;

namespace H.ToolBox.RabbitMQ.Tests.Integration;

/// <summary>
/// 仅在 Docker 可用时执行的测试事实。
/// 本机没有 Docker（或未启动 daemon）时自动跳过，保证 dotnet test 不因缺少容器运行时而失败。
/// 设置环境变量 MB_FORCE_INTEGRATION_TESTS=1 可强制执行（例如 CI 中通过远程 DOCKER_HOST 提供 daemon）。
/// </summary>
public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (!DockerEnvironment.IsAvailable)
        {
            Skip = "需要可用的 Docker daemon（Testcontainers 集成测试）。本机未检测到 Docker，已跳过。";
        }
    }
}

internal static class DockerEnvironment
{
    private static readonly Lazy<bool> s_available = new(Detect);

    public static bool IsAvailable => s_available.Value;

    private static bool Detect()
    {
        if (Environment.GetEnvironmentVariable("MB_FORCE_INTEGRATION_TESTS") == "1")
        {
            return true;
        }

        try
        {
            var startInfo = new ProcessStartInfo("docker", "info")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            using var process = Process.Start(startInfo);
            if (process is null)
            {
                return false;
            }

            if (!process.WaitForExit(5000))
            {
                try { process.Kill(entireProcessTree: true); } catch { /* 超时进程清理失败可忽略 */ }
                return false;
            }

            return process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
