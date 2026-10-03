using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using RavenBench.Tests.Infrastructure;

namespace RavenBench.Tests.Diagnostics;

public class HostCommandTests
{
    [RequiresBashFact]
    public async Task Run_Returns_When_The_Child_Fills_The_Error_Pipe_Before_Closing_Standard_Output()
    {
        var run = Task.Run(() => HostCommand.Run("bash", "-c", "head -c 200000 /dev/zero | tr '\\0' x >&2; echo done"));

        // The timeout bounds a deadlock only.
        (await Task.WhenAny(run, Task.Delay(System.TimeSpan.FromMinutes(1)))).Should().BeSameAs(run, "Run must drain both pipes concurrently");
        var result = await run;
        result.StandardOutput.Should().Be("done");
        result.StandardError.Should().Be(new string('x', 200_000));
    }

    [RequiresBashFact]
    public void Run_Kills_A_Child_That_Outlives_The_Exit_Limit()
    {
        var act = () => HostCommand.Run(System.TimeSpan.Zero, "bash", "-c", "sleep 30");

        act.Should().Throw<System.TimeoutException>().WithMessage("*did not exit within 0 s*");
    }
}
