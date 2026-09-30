using System.Diagnostics;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests.Transport;

public class ContainerMemoryLimitTests
{
    private static string Docker(params string[] arguments)
    {
        var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var argument in arguments)
            info.ArgumentList.Add(argument);
        using var process = Process.Start(info)!;
        var output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        Assert.True(process.ExitCode == 0, $"docker {string.Join(' ', arguments)}: {process.StandardError.ReadToEnd()}");
        return output.Trim();
    }

    [RequiresDockerFact]
    public void A_Limited_Container_Without_A_Prior_Limit_Gets_The_Host_Memory_And_Unlimited_Swap_Back()
    {
        var id = Docker("run", "-d", "alpine:3.20", "sleep", "300");
        try
        {
            var docker = new DockerDatabaseContainerLocator();
            using (var limit = new ContainerMemoryLimit(id))
                Assert.Equal((64L << 20, 64L << 20), limit.Apply(64L << 20));
            Assert.Equal((docker.ReadHostMemory(), -1L), docker.ReadMemoryLimits(id));
        }
        finally
        {
            Docker("rm", "-f", id);
        }
    }
}

public sealed class RequiresDockerFactAttribute : FactAttribute
{
    public RequiresDockerFactAttribute()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo("docker", "info") { RedirectStandardOutput = true, RedirectStandardError = true })!;
            process.WaitForExit();
            if (process.ExitCode != 0)
                Skip = "A running Docker daemon is required.";
        }
        catch (System.ComponentModel.Win32Exception)
        {
            Skip = "The docker CLI is required.";
        }
    }
}
