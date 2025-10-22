using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Reporting;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests;

public class TableMetricsTests
{
    [Fact]
    public void NormalizedTailColumns_Should_Not_Reprint_Raw_Tail_Values()
    {
        // INVARIANT: Normalized latency table should reflect normalized tails,
        //            not simply echo the raw p99.99/pMax figures.
        var step = new StepResult
        {
            Concurrency = 8,
            Throughput = 1_000,
            ErrorRate = 0,
            BytesOut = 1,
            BytesIn = 1,
            Raw = new Percentiles(10, 20, 30, 40, 50, 60),
            Normalized = new Percentiles(1, 2, 3, 4, 5, 6),
            P9999 = 150,    // Raw tail in ms
            PMax = 225,     // Raw max in ms
            NormalizedP9999 = 12.5,
            NormalizedPMax = 18.75,
            SampleCount = 100,
            CorrectedCount = 100,
            ClientCpu = 0.1,
            NetworkUtilization = 0.1
        };

        var summary = new BenchmarkSummary
        {
            Options = new RunOptions
            {
                Url = "http://localhost:8080",
                Database = "db",
                Profile = WorkloadProfile.Mixed
            },
            Steps = new List<StepResult> { step },
            Verdict = "ok",
            ClientCompression = "identity",
            EffectiveHttpVersion = "1.1"
        };

        var normalizedColumns = TableMetrics.GetVisibleColumns(summary, TableScope.Normalized);

        var p9999Column = normalizedColumns.Single(c => c.Header == "p99.99 ms");
        var pMaxColumn = normalizedColumns.Single(c => c.Header == "pMax ms");

        var rawP9999Formatted = step.P9999.ToString("F1");
        var rawPMaxFormatted = step.PMax.ToString("F1");
        var normalizedP9999Formatted = step.NormalizedP9999.ToString("F1");
        var normalizedPMaxFormatted = step.NormalizedPMax.ToString("F1");

        // Expectation: normalized table should print the normalized values, not the raw tails.
        p9999Column.ValueSelector(step).Should().Be(normalizedP9999Formatted,
            "normalized tail columns should display the normalized p99.99 values");
        pMaxColumn.ValueSelector(step).Should().Be(normalizedPMaxFormatted,
            "normalized tail columns should display the normalized pMax values");

        // Regression guard: explicitly ensure raw tails are not rendered in the normalized scope.
        p9999Column.ValueSelector(step).Should().NotBe(rawP9999Formatted,
            "normalized tail columns should display normalized data, not raw p99.99 values");

        pMaxColumn.ValueSelector(step).Should().NotBe(rawPMaxFormatted,
            "normalized tail columns should display normalized data, not raw pMax values");
    }
}
