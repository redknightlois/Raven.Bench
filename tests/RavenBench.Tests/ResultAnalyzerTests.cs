using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Analysis;
using RavenBench.Core.Reporting;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests;

public class ResultAnalyzerTests
{
    private static string VerdictFor(StepResult knee)
    {
        var opts = new RunOptions { Url = "http://localhost:8080", Database = "d", Profile = WorkloadProfile.Mixed };
        var run = new BenchmarkRun
        {
            Steps = new List<StepResult> { knee },
            MaxNetworkUtilization = knee.NetworkUtilization,
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1",
            EffectiveDatabase = "d"
        };
        var summary = new BenchmarkSummary
        {
            Options = opts, Steps = run.Steps, Knee = knee,
            Verdict = "v", ClientCompression = "identity", EffectiveHttpVersion = "1.1"
        };
        return ResultAnalyzer.Analyze(run, knee, opts, summary).Verdict;
    }

    [Fact]
    public void Server_Cpu_At_Threshold_Is_Server_Limited() =>
        VerdictFor(new StepResult { Concurrency = 16, Throughput = 1000, ProcessCpu = 90 }).Should().Be("server-limited (CPU)");

    [Fact]
    public void Falls_Back_To_ServerCpu_When_ProcessCpu_Absent() =>
        VerdictFor(new StepResult { Concurrency = 16, Throughput = 1000, ServerCpu = 88 }).Should().Be("server-limited (CPU)");

    [Fact]
    public void Client_Cpu_Takes_Precedence_Over_Server_Cpu() =>
        VerdictFor(new StepResult { Concurrency = 16, Throughput = 1000, ClientCpu = 0.9, ProcessCpu = 95 }).Should().Be("client-limited (CPU)");

    [Fact]
    public void Server_Counters_Present_But_Below_Threshold_Is_Not_Cpu_Bound() =>
        VerdictFor(new StepResult { Concurrency = 16, Throughput = 1000, ProcessCpu = 20 }).Should().Be("unknown (not CPU- or network-bound at the knee)");

    [Fact]
    public void No_Server_Counters_Asks_To_Collect_Them() =>
        VerdictFor(new StepResult { Concurrency = 16, Throughput = 1000 }).Should().Be("unknown (collect server counters for attribution)");
}
