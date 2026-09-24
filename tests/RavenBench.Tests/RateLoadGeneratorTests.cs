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
    public void PacerReleasesRateTimesElapsedOnAnInjectedClock()
    {
        var pacer = new RateLoadGenerator.TokenPacer(ratePerSecond: 500, burstCapacity: 256);
        long released = 0;
        for (var ms = 1; ms <= 600; ms++)
            released += pacer.Release(TimeSpan.FromMilliseconds(ms).Ticks);

        released.Should().Be(300);
        pacer.DroppedTokens.Should().Be(0);
    }

    [Fact]
    public void PacerUnderSaturationReleasesTheBurstAndCountsTheRestAsDropped()
    {
        var pacer = new RateLoadGenerator.TokenPacer(ratePerSecond: 1000, burstCapacity: 32);

        pacer.Release(TimeSpan.FromMilliseconds(100).Ticks).Should().Be(32);
        pacer.DroppedTokens.Should().BeApproximately(68, 1e-9);
        pacer.Release(TimeSpan.FromMilliseconds(100).Ticks).Should().Be(0, "a clock that does not advance owes nothing");
        pacer.Release(TimeSpan.FromMilliseconds(105).Ticks).Should().Be(5);
    }

    [Fact]
    public void RollingSamplerReportsTheRateOfInjectedSamples()
    {
        var sampler = new RateLoadGenerator.RollingRateSampler(TimeSpan.FromSeconds(3), TimeSpan.FromMilliseconds(250));
        for (var i = 0; i <= 8; i++)
            sampler.RecordSample(i * 0.25, i * 125);

        var stats = sampler.Snapshot();
        stats.HasSamples.Should().BeTrue();
        stats.Median.Should().BeApproximately(500, 1e-9);
        stats.Min.Should().BeApproximately(500, 1e-9);
        stats.Max.Should().BeApproximately(500, 1e-9);
    }

    [Fact]
    public async Task ScheduledOperationsNeverExceedRateTimesElapsed()
    {
        var transport = new TestTransport(baseLatencyMs: 0);
        var generator = new RateLoadGenerator(transport, new ConstantWorkload(), targetRps: 500, maxConcurrency: 64, new Random(42));

        var (_, metrics) = await generator.ExecuteMeasurementAsync(TimeSpan.FromMilliseconds(600), CancellationToken.None);

        metrics.OperationsCompleted.Should().BeGreaterThan(0);
        metrics.OperationsCompleted.Should().BeLessOrEqualTo(metrics.ScheduledOperations);
        metrics.ScheduledOperations.Should().BeLessOrEqualTo((long)(500 * metrics.Duration.TotalSeconds) + 1);
        metrics.RollingRate.Should().NotBeNull();
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
        metrics.ScheduledOperations.Should().BeLessOrEqualTo((long)(targetRps * metrics.Duration.TotalSeconds) + 1);
        metrics.ScheduledOperations.Should().BeGreaterThan(0);
        metrics.RollingRate.Should().NotBeNull();
        metrics.RollingRate!.Median.Should().BeGreaterOrEqualTo(0.0);
    }

    private sealed class ConstantWorkload : IWorkload
    {
        public OperationBase NextOperation(Random rng) => new ReadOperation { Id = "users/1" };
    }
}
