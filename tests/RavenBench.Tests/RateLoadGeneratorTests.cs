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
    private static readonly long Ms = TimeSpan.TicksPerMillisecond;

    private static RateLoadGenerator.TokenPacer Pacer(double rate, int burst) => new(rate, burst, TimeSpan.TicksPerSecond);

    [Fact]
    public void PacerReleasesRateTimesElapsedOnAnInjectedClock()
    {
        var pacer = Pacer(rate: 500, burst: 256);
        long released = 0;
        for (var ms = 0; ms < 600; ms++)
            released += pacer.Release(ms * Ms).Count;

        released.Should().Be(300);
        pacer.DroppedTokens.Should().Be(0);
    }

    [Fact]
    public void PacerUnderSaturationReleasesTheBurstAndCountsTheRestAsDropped()
    {
        var pacer = Pacer(rate: 1000, burst: 32);
        pacer.Release(0).Count.Should().Be(1);

        pacer.Release(100 * Ms).Count.Should().Be(32);
        pacer.DroppedTokens.Should().Be(68);
        pacer.Release(100 * Ms).Count.Should().Be(0, "a clock that does not advance owes nothing");
        pacer.Release(105 * Ms).Count.Should().Be(5);
        pacer.ReleasedTokens.Should().Be(38);
    }

    [Fact]
    public void ProducerWakingAtTheNextDueTimeReleasesEveryTokenOnTime()
    {
        // The producer's wake-up is the pacer's next due time, never a fixed interval, so no token waits for a later wake-up.
        var pacer = Pacer(rate: 20_000, burst: 256);
        var now = 3 * Ms;
        for (var wake = 0; wake < 10_000; wake++)
        {
            var (first, count) = pacer.Release(now);
            for (var sequence = first; sequence < first + count; sequence++)
            {
                (now - pacer.DueTicks(sequence)).Should().BeInRange(0, 0, "token {0} left late", sequence);
            }

            now = pacer.NextDueTicks;
        }

        pacer.ReleasedTokens.Should().Be(10_000);
    }

    [Fact]
    public void StartupDelayBetweenConstructionAndFirstReleaseDoesNotShiftTheSchedule()
    {
        long clock = 0;
        var pacer = Pacer(rate: 500, burst: 256);
        clock += 20 * Ms;

        var lateness = new System.Collections.Generic.List<long>();
        for (var ms = 0; ms < 200; ms++, clock += Ms)
        {
            var (first, count) = pacer.Release(clock);
            for (var sequence = first; sequence < first + count; sequence++)
                lateness.Add(clock - pacer.DueTicks(sequence));
        }

        lateness.Should().HaveCount(100);
        lateness.Should().OnlyContain(late => late == 0, "the schedule counts from the pacer's first reading of the clock, not from construction");
    }

    [Fact]
    public void PacerRejectsANonPositiveRate()
    {
        var zero = () => Pacer(rate: 0, burst: 32);
        var negative = () => Pacer(rate: -1, burst: 32);
        zero.Should().Throw<ArgumentOutOfRangeException>();
        negative.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void SendLatenessComparableToLatencyMarksTheStep()
    {
        var late = new RavenBench.Core.Reporting.Percentiles(1.0, 1.0, 1.0, 1.0, 2.0, 3.0);
        var onTime = new RavenBench.Core.Reporting.Percentiles(0, 0, 0, 0, 0.001, 0.002);

        RavenBench.Core.Metrics.SendLateness.MarkingFor(late, latencyP50Ms: 1.2).Should().StartWith("client-bound");
        RavenBench.Core.Metrics.SendLateness.MarkingFor(onTime, latencyP50Ms: 1.2).Should().BeNull();
        RavenBench.Core.Metrics.SendLateness.MarkingFor(null, latencyP50Ms: 1.2).Should().BeNull();
    }

    [Fact]
    public async Task MeasurementReportsSendLateness()
    {
        var transport = new TestTransport(baseLatencyMs: 0);
        var generator = new RateLoadGenerator(transport, new ConstantWorkload(), targetRps: 500, maxConcurrency: 64, new Random(42));

        var (_, metrics) = await generator.ExecuteMeasurementAsync(TimeSpan.FromMilliseconds(300), CancellationToken.None);

        metrics.SendLateness.Should().NotBeNull();
        metrics.SendLateness!.Value.P999.Should().BeGreaterOrEqualTo(metrics.SendLateness.Value.P50);
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
