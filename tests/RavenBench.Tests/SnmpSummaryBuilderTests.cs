using System;
using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using Xunit;

namespace RavenBench.Tests;

public class SnmpSummaryBuilderTests
{
    private static readonly DateTime T0 = new(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static ServerMetrics At(double seconds, double readOps) => new() { Timestamp = T0.AddSeconds(seconds), SnmpIoReadOpsPerSec = readOps };

    private static MeasurementWindow Window(double from, double to) => new(T0.AddSeconds(from), T0.AddSeconds(to));

    [Fact]
    public void Io_Totals_Integrate_Only_Time_Inside_The_Measurement_Windows()
    {
        var history = new List<ServerMetrics> { At(0, 100), At(1, 100), At(31, 100), At(32, 100) };

        var (_, aggregations) = SnmpSummaryBuilder.Build(history, new[] { Window(0, 1), Window(31, 32) });

        aggregations!.TotalSnmpIoReadOps.Should().BeApproximately(200, 1e-9);
        aggregations.AverageSnmpIoReadOpsPerSec.Should().BeApproximately(100, 1e-9);
    }

    [Theory]
    [InlineData(0, 32)]
    [InlineData(0.5, 31.5)]
    [InlineData(2, 3)]
    public void The_Total_Is_The_Average_Times_The_Integrated_Window_Time(double from, double to)
    {
        var history = new List<ServerMetrics> { At(0, 10), At(1, 20), At(31, 40), At(32, 80) };

        var (_, aggregations) = SnmpSummaryBuilder.Build(history, new[] { Window(from, to) });

        aggregations!.TotalSnmpIoReadOps.Should().BeApproximately(aggregations.AverageSnmpIoReadOpsPerSec!.Value * (Math.Min(to, 32) - Math.Max(from, 0)), 1e-6);
    }
}
