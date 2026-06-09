using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class ClosedLoopRampTests
{
    [Fact]
    public async Task Ramp_Collects_Steps_And_Keeps_Invariants()
    {
        var opts = new RunOptions
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Writes = 100,
            Distribution = "uniform",
            Transport = "raw",
            Compression = "identity",
            Warmup = TimeSpan.FromMilliseconds(50),
            Duration = TimeSpan.FromMilliseconds(100),
            Step = new StepPlan(2, 8, 2),
            Profile = WorkloadProfile.Mixed
        };

        using var transport = new TestTransport(baseLatencyMs: 1);
        var workload = new MixedProfileWorkload(WorkloadMix.FromWeights(0, 100, 0), new UniformDistribution(), 1024);
        using var serverTracker = new ServerMetricsTracker(transport, opts);
        var executor = new BenchmarkExecutor(opts, transport, workload, new ProcessCpuTracker(), serverTracker);
        var rng = new Random(42);

        var steps = new List<StepResult>();
        for (int concurrency = 2; concurrency <= 8; concurrency *= 2)
        {
            var generator = new ClosedLoopLoadGenerator(transport, workload, concurrency, rng);
            var (_, step) = await executor.ExecuteStepAsync(generator, steps.Count, concurrency, CancellationToken.None);
            steps.Add(step);
        }

        steps.Count.Should().BeGreaterOrEqualTo(2);
        steps.All(s => s.Throughput >= 0).Should().BeTrue();
        steps.All(s => s.ErrorRate >= 0 && s.ErrorRate <= 1).Should().BeTrue();
        steps.All(s => s.NetworkUtilization >= 0 && s.NetworkUtilization <= 1).Should().BeTrue();
    }
}
