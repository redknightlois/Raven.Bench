using System.Collections.Generic;
using System.IO;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests.Cli;

[Trait("Category", "Unit")]
public class RunResultsOutputTests
{
    [Fact]
    public void Results_Are_Written_When_The_Output_Path_Holds_Markup_Brackets()
    {
        var dir = Directory.CreateTempSubdirectory("run [x]");
        try
        {
            var opts = new RunOptions
            {
                Url = "http://localhost:10101",
                Database = "bench",
                OutJson = Path.Combine(dir.FullName, "result [1].json"),
                OutCsv = Path.Combine(dir.FullName, "result [1].csv")
            };
            var summary = new BenchmarkSummary
            {
                Options = opts, Steps = new List<StepResult>(),
                Verdict = "v", ClientCompression = "identity", EffectiveHttpVersion = "1.1"
            };

            RunCommandBase<ClosedSettings>.WriteResults(opts, summary);

            File.Exists(opts.OutJson).Should().BeTrue();
            File.Exists(opts.OutCsv).Should().BeTrue();
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }
}
