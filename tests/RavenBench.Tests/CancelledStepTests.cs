using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class CancelledStepTests
{
    private static readonly RunOptions Options = new()
    {
        Url = "http://localhost:8081",
        Database = "bench",
        Warmup = TimeSpan.Zero,
        Duration = TimeSpan.FromSeconds(10),
        Profile = WorkloadProfile.Mixed
    };

    private static IWorkload Workload() =>
        new MixedProfileWorkload(WorkloadMix.FromWeights(0, 100, 0), new UniformDistribution(), 1024, seed: 42);

    [Fact]
    public async Task A_Rate_Step_Cancelled_Mid_Measurement_Surfaces_Cancellation_Instead_Of_A_Step_Result()
    {
        using var transport = new TestTransport(baseLatencyMs: 0);
        var workload = Workload();
        var executor = new BenchmarkExecutor(Options, transport, workload, new ProcessCpuTracker());
        var generator = new RateLoadGenerator(transport, workload, 500, maxConcurrency: 16, new Random(1));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var act = () => executor.ExecuteStepAsync(generator, 0, 500, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task A_Step_That_Is_Not_Cancelled_Still_Returns_Its_Result()
    {
        using var transport = new TestTransport(baseLatencyMs: 0);
        var workload = Workload();
        var options = Options with { Duration = TimeSpan.FromMilliseconds(200) };
        var executor = new BenchmarkExecutor(options, transport, workload, new ProcessCpuTracker());
        var generator = new RateLoadGenerator(transport, workload, 500, maxConcurrency: 16, new Random(1));

        var (_, step) = await executor.ExecuteStepAsync(generator, 0, 500, CancellationToken.None);

        step.SampleCount.Should().BeGreaterThan(0);
    }
}
