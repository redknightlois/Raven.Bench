using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using FluentAssertions;
using Lextm.SharpSnmpLib;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Metrics;
using RavenBench.Core.Metrics.Snmp;
using Xunit;

namespace RavenBench.Tests;

/// <summary>Invariant data parses and formats the same under any current culture.</summary>
public class CultureTests
{
    private static T Under<T>(string culture, Func<T> action)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo(culture);
            return action();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    private static ServerMetrics ParseServerSample(string workingSet, string totalProcessorTime) =>
        RavenServerMetricsCollector.Parse(
            $$$"""{"MemoryInformation":{"WorkingSet":"{{{workingSet}}}"}}""",
            $$$"""{"CpuStats":[{"TotalProcessorTime":"{{{totalProcessorTime}}}"}]}""",
            serverCores: 4);

    [Fact]
    public void WorkingSet_Parses_Under_A_Comma_Decimal_Culture()
    {
        Under("de-DE", () => ParseServerSample("3.231 GBytes", "00:00:00")).MemoryUsageMB.Should().Be(3308);
    }

    [Fact]
    public void TotalProcessorTime_Parses_Under_A_Comma_Decimal_Culture()
    {
        var start = ParseServerSample("1 MBytes", "00:00:00") with { Timestamp = DateTime.UnixEpoch };
        var end = Under("de-DE", () => ParseServerSample("1 MBytes", "00:00:01.5000000")) with { Timestamp = DateTime.UnixEpoch.AddSeconds(1) };
        var invariantEnd = ParseServerSample("1 MBytes", "00:00:01.5000000") with { Timestamp = end.Timestamp };

        end.ServerProcessorTime.Should().Be(TimeSpan.FromSeconds(1.5));
        ServerMetricsTracker.CpuPercent(start, end).Should().Be(ServerMetricsTracker.CpuPercent(start, invariantEnd));
    }

    [Fact]
    public void Snmp_LoadAverage_Parses_Under_A_Comma_Decimal_Culture()
    {
        var values = new Dictionary<string, Variable>
        {
            [SnmpOids.Load1Min] = new Variable(new ObjectIdentifier(SnmpOids.Load1Min), new OctetString("0.52"))
        };

        Under("de-DE", () => SnmpMetricMapper.MapToSample(values)).Load1Min.Should().Be(0.52);
    }

    [Fact]
    public void Aggregate_Timestamp_And_Checksum_Do_Not_Depend_On_The_Calendar()
    {
        var spec = new AggregateDataSpec(1, 3, 256, 5, 5, GroupDistribution.Parse("uniform", null));
        var invariant = Under("", () => new AggregateDataSet(spec).Summarize());
        var (first, thai) = Under("th-TH", () => (new AggregateDataSet(spec).Generate().First(), new AggregateDataSet(spec).Summarize()));

        first.Timestamp.Should().Be("2025-01-01T00:00:00Z");
        thai.Checksum.Should().Be(invariant.Checksum);
    }
}
