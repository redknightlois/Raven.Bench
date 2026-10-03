using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Analysis;
using RavenBench.Core.Reporting;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests;

public class InvariantsTests
{
    [Fact]
    public void Flags_Unreliable_Beyond_Knee()
    {
        var opts = new RunOptions { Url = "u", Database = "d", Profile = WorkloadProfile.Mixed };
        var knee = new StepResult { Concurrency = 16, KneeDegraded = true };
        var run = new BenchmarkRun
        {
            Steps = new List<StepResult> { new() { Concurrency = 8 }, knee },
            MaxNetworkUtilization = 0.5,
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1",
            EffectiveDatabase = "test-db"
        };
        var summary = new BenchmarkSummary
        {
            Options = opts,
            Steps = run.Steps,
            Knee = knee,
            Verdict = "v",
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1"
        };
        
        var analysis = ResultAnalyzer.Analyze(run, knee, opts, summary);
        analysis.UnreliableBeyondKnee.Should().BeTrue();
        analysis.Warnings.Should().NotBeEmpty();
    }

    [Fact]
    public void A_Single_Step_Run_Is_Not_Flagged_Unreliable()
    {
        var opts = new RunOptions { Url = "u", Database = "d", Profile = WorkloadProfile.Mixed };
        var run = new BenchmarkRun
        {
            Steps = new List<StepResult> { new() { Concurrency = 8, Throughput = 1000, Raw = new(1, 2, 3, 4, 5, 6) } },
            MaxNetworkUtilization = 0,
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1",
            EffectiveDatabase = "test-db"
        };
        var knee = KneeFinder.FindKnee(run.Steps, 0.005)!;
        var summary = new BenchmarkSummary { Options = opts, Steps = run.Steps, Knee = knee, Verdict = "v", ClientCompression = "identity", EffectiveHttpVersion = "1.1" };

        var analysis = ResultAnalyzer.Analyze(run, knee, opts, summary);

        knee.Reason.Should().Contain("no comparison between steps");
        analysis.UnreliableBeyondKnee.Should().BeFalse();
        analysis.Warnings.Should().BeEmpty();
    }
}
