using System.Diagnostics;
using System.IO;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using Xunit;

namespace RavenBench.Tests.Scripts;

/// <summary>Runs the shared helper file's URL and array helpers under set -u, as the run scripts do.</summary>
[Trait("Category", "Unit")]
public class RunCommonUrlTests
{
    private static string Bash(string body)
    {
        var helper = Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "run-common.sh");
        var info = new ProcessStartInfo("bash") { RedirectStandardOutput = true, RedirectStandardError = true };
        info.ArgumentList.Add("-c");
        info.ArgumentList.Add($"set -u; COMPOSE_FILE=unused; source '{helper}'; {body}");
        using var process = Process.Start(info)!;
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        process.ExitCode.Should().Be(0, error);
        return output.Trim();
    }

    [Theory]
    [InlineData("http://[::1]:8080", "::1", "8080")]
    [InlineData("http://[::1]", "::1", "7")]
    [InlineData("http://[::1]/path:1", "::1", "7")]
    [InlineData("http://user:pass@[2001:db8::1]:9200/x", "2001:db8::1", "9200")]
    [InlineData("http://host:8081/x", "host", "8081")]
    [InlineData("http://host", "host", "7")]
    public void A_Url_Authority_Parses_To_Its_Host_And_Port_Or_The_Default(string url, string host, string port)
    {
        Bash($"echo \"$(host_of_url '{url}') $(port_of_url '{url}' 7)\"").Should().Be($"{host} {port}");
    }

    [Fact]
    public void The_Helpers_Run_Under_Set_U_With_Empty_Passthrough_And_Run_Dir_Arrays()
    {
        Bash("DEFAULT_URL=http://[::1]:5; DEFAULT_PORT=1; PASSTHROUGH=(); RUN_DIRS=(); resolve_endpoint; STARTED_COMPOSE=0; on_exit; ").Should().BeEmpty();
        Bash("DEFAULT_URL=http://h:5; DEFAULT_PORT=1; PASSTHROUGH=(); resolve_endpoint; echo \"$HOST $PORT $URL_WAS_GIVEN\"").Should().Be("h 5 0");
    }
}
