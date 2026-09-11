using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Ycsb;
using RavenBench.Reporter;
using RavenBench.Reporting;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// A ycsb result is still a Raven.Bench summary: the schema version does not change and the
/// existing reporter loads it back, Ycsb block included.
/// </summary>
public class YcsbResultCompatibilityTests
{
    [Fact]
    public async Task Ycsb_Result_Round_Trips_Through_The_Existing_Summary_Loader()
    {
        var scenario = new YcsbScenario
        {
            Seed = 7,
            Target = "ravendb",
            DocumentCount = 1000,
            DocumentSize = "1KB",
            Concurrency = "8..64x2",
            Distribution = "uniform",
            Warmup = "1s",
            Duration = "1s"
        };

        var summary = new BenchmarkSummary
        {
            Options = new RunOptions { Url = "http://localhost:8081", Database = "ycsb", Profile = WorkloadProfile.Mixed },
            Steps = new List<StepResult>(),
            Verdict = "ycsb",
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1",
            Ycsb = new YcsbRunInfo
            {
                Run = "C",
                ResolvedScenario = scenario,
                ProductName = "RavenDB",
                ServerVersion = "7.2.5",
                Durability = new DurabilityParity { Setting = "durability", Value = "ravendb-default" }
            }
        };

        var path = Path.Combine(Path.GetTempPath(), $"ycsb-result-{Guid.NewGuid():N}.json");
        JsonResultsWriter.Write(path, summary);

        var loaded = await SummaryLoader.LoadAsync(path);

        loaded.SchemaVersion.Should().Be(SummaryLoader.ExpectedSchemaVersion);
        loaded.Ycsb.Should().NotBeNull();
        loaded.Ycsb!.Run.Should().Be("C");
        loaded.Ycsb.ResolvedScenario.Should().Be(scenario);
    }

    [Fact]
    public void A_Result_From_Another_Command_Carries_No_Ycsb_Block()
    {
        var summary = new BenchmarkSummary
        {
            Options = new RunOptions { Url = "http://localhost:8081", Database = "x", Profile = WorkloadProfile.Mixed },
            Steps = new List<StepResult>(),
            Verdict = "ok",
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1"
        };

        var path = Path.Combine(Path.GetTempPath(), $"plain-result-{Guid.NewGuid():N}.json");
        JsonResultsWriter.Write(path, summary);

        File.ReadAllText(path).Should().NotContain("\"Ycsb\"");
    }
}
