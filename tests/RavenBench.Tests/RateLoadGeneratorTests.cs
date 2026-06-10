using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Workload;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests;

public sealed class RateLoadGeneratorTests
{
    [Fact]
    public async Task MaintainsRequestedRateWithinTolerance()
    {
        var transport = new TestTransport(baseLatencyMs: 0);
        var workload = new ConstantWorkload();
        var generator = new RateLoadGenerator(transport, workload, targetRps: 500, maxConcurrency: 64, new Random(42));

        var duration = TimeSpan.FromMilliseconds(600);
        var (_, metrics) = await generator.ExecuteMeasurementAsync(duration, CancellationToken.None);

        metrics.Throughput.Should().BeGreaterThan(300);
        metrics.Throughput.Should().BeApproximately(500, 120); // allow +/- 24%
        metrics.RollingRate.Should().NotBeNull();
        metrics.RollingRate!.HasSamples.Should().BeTrue();
        metrics.RollingRate!.Median.Should().BeApproximately(500, 120);
    }

    [Fact]
    public async Task StopsGracefullyWhenCancelled()
    {
        var transport = new TestTransport(baseLatencyMs: 0);
        var workload = new ConstantWorkload();
        const int targetRps = 1000;
        var generator = new RateLoadGenerator(transport, workload, targetRps, maxConcurrency: 64, new Random(123));

        var cancelAfter = TimeSpan.FromMilliseconds(150);
        using var cts = new CancellationTokenSource(cancelAfter);
        var (_, metrics) = await generator.ExecuteMeasurementAsync(TimeSpan.FromSeconds(5), cts.Token);

        cts.IsCancellationRequested.Should().BeTrue();
        metrics.ScheduledOperations.Should().BeLessThan((long)(targetRps * cancelAfter.TotalSeconds * 3));
        metrics.ScheduledOperations.Should().BeGreaterThan(0);
        metrics.RollingRate.Should().NotBeNull();
        metrics.RollingRate!.Median.Should().BeGreaterOrEqualTo(0.0);
    }

    [Fact]
    public async Task ScheduledOperationsExceedCompletedUnderSaturation()
    {
        var transport = new TestTransport(baseLatencyMs: 50);
        var workload = new ConstantWorkload();
        var generator = new RateLoadGenerator(transport, workload, targetRps: 500, maxConcurrency: 1, new Random(7));

        // Cancellation stops the workers before they drain the backlog, so the scheduled/completed
        // gap built up under saturation is observable in the metrics.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var (_, metrics) = await generator.ExecuteMeasurementAsync(TimeSpan.FromSeconds(5), cts.Token);

        metrics.OperationsCompleted.Should().BeGreaterThan(0);
        metrics.ScheduledOperations.Should().BeGreaterThan(metrics.OperationsCompleted);
    }

    private sealed class ConstantWorkload : IWorkload
    {
        public OperationBase NextOperation(Random rng) => new ReadOperation { Id = "users/1" };
    }
}
