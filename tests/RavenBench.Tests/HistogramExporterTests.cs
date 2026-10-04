using System;
using System.IO;
using System.Linq;
using FluentAssertions;
using HdrHistogram;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using Xunit;

namespace RavenBench.Tests;

public class HistogramExporterTests
{
    private static HistogramSnapshot Snapshot(params long[] micros)
    {
        using var recorder = new LatencyRecorder(recordLatencies: true);
        foreach (var value in micros)
            recorder.Record(value);
        return recorder.Snapshot();
    }

    [Fact]
    public void Two_Steps_At_One_Concurrency_Write_Distinct_Files_Each_Holding_Its_Own_Distribution()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"ravenbench-hlog-{Guid.NewGuid():N}");
        try
        {
            var prefix = Path.Combine(dir, "run");
            // Rate steps share the worker count, so the concurrency alone never tells two steps apart.
            var first = HistogramExporter.BuildHistogramArtifact(Snapshot(100, 200, 300), 0, 8, prefix, HistogramExportFormat.Both)!;
            var second = HistogramExporter.BuildHistogramArtifact(Snapshot(5_000, 6_000), 1, 8, prefix, HistogramExportFormat.Both)!;

            first.HlogPath.Should().NotBe(second.HlogPath);
            first.CsvPath.Should().NotBe(second.CsvPath);
            foreach (var artifact in new[] { first, second })
            {
                using var stream = File.OpenRead(artifact.HlogPath!);
                var read = new HistogramLogReader(stream).ReadHistograms().Single();
                read.TotalCount.Should().Be(artifact.TotalCount);
                read.GetValueAtPercentile(100).Should().BeGreaterOrEqualTo(artifact.LatencyInMicroseconds[^1] * 999 / 1000);
            }
        }
        finally
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
    }
}
