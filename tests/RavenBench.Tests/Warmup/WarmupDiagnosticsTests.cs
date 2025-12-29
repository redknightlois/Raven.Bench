using System;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using Xunit;

namespace RavenBench.Tests.Warmup;

public sealed class WarmupDiagnosticsTests
{
    [Fact]
    public void FromRecorder_PopulatesPercentiles()
    {
        var recorder = new LatencyRecorder(recordLatencies: true);
        var now = DateTime.UtcNow;

        for (int i = 0; i < 10_000; i++)
        {
            recorder.Record(i + 1);
        }

        var metrics = new LoadGeneratorMetrics
        {
            Throughput = 1_000,
            ErrorRate = 0.01,
            OperationsCompleted = 10_000,
            BytesIn = 0,
            BytesOut = 0,
            NetworkUtilization = 0,
            Reason = null,
            RollingRate = null,
            ScheduledOperations = 10_000
        };

        var diagnostics = WarmupDiagnostics.FromRecorder(recorder, metrics, TimeSpan.FromSeconds(5));

        diagnostics.Throughput.Should().BeApproximately(1_000, 1e-9);
        diagnostics.ErrorRate.Should().BeApproximately(0.01, 1e-12);
        diagnostics.SampleCount.Should().Be(10_000);
        diagnostics.P50Micros.Should().BeGreaterThan(0);
        diagnostics.P95Micros.Should().BeGreaterThan(diagnostics.P50Micros);
        diagnostics.MaxMicros.Should().BeGreaterThan(diagnostics.P99Micros);
        diagnostics.Duration.Should().Be(TimeSpan.FromSeconds(5));
    }
}
