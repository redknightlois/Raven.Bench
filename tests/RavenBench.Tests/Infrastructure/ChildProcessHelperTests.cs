using System;
using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

[Trait("Category", "Unit")]
public class ChildProcessHelperTests
{
    // Each stream carries more than a pipe buffer, so a reader that drains one stream before the other blocks the child.
    private const string FloodBothPipes = "head -c 1048576 /dev/zero >&2; head -c 1048576 /dev/zero; exit 0";

    [RequiresBashFact]
    public void A_Probe_Child_That_Floods_Both_Pipes_Still_Exits()
    {
        ToolProbe.SkipReason("/bin/bash", $"-c \"{FloodBothPipes}\"", "missing", TimeSpan.FromMinutes(5)).Should().BeNull();
    }

    [RequiresBashFact]
    public void A_Probe_Child_That_Does_Not_Exit_Is_Killed_And_Gives_A_Skip_Reason()
    {
        var pidFile = Path.GetTempFileName();
        try
        {
            var reason = ToolProbe.SkipReason("/bin/bash", $"-c \"echo $$ > {pidFile}; exec sleep 1000\"", "missing", TimeSpan.Zero);

            reason.Should().Contain("did not exit");
            var pid = File.ReadAllText(pidFile).Trim();
            if (pid.Length > 0)
                Directory.Exists($"/proc/{pid}").Should().BeFalse("the probe kills the child it gave up on");
        }
        finally
        {
            File.Delete(pidFile);
        }
    }

    [Fact]
    public void A_Missing_Tool_Gives_The_Missing_Reason()
    {
        ToolProbe.SkipReason("ravenbench-no-such-tool", "", "missing").Should().Be("missing");
    }

    [RequiresBashFact]
    public void A_Script_That_Floods_Both_Pipes_Returns_Both_Outputs()
    {
        var script = Path.GetTempFileName();
        try
        {
            File.WriteAllText(script, FloodBothPipes);
            var (exitCode, output) = BashScript.Run(script, [], null);

            exitCode.Should().Be(0);
            output.Length.Should().Be(2 * 1048576);
        }
        finally
        {
            File.Delete(script);
        }
    }

    [RequiresBashFact]
    public void A_Script_That_Does_Not_Exit_Is_Killed_With_A_Timeout()
    {
        var script = Path.GetTempFileName();
        try
        {
            File.WriteAllText(script, "exec sleep 1000");
            var run = () => BashScript.Run(script, [], new Dictionary<string, string>(), limit: TimeSpan.Zero);

            run.Should().Throw<TimeoutException>();
        }
        finally
        {
            File.Delete(script);
        }
    }
}
