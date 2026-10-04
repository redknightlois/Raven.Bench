using System.Globalization;
using HdrHistogram;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;

namespace RavenBench;

internal static class HistogramExporter
{
    /// <summary>
    /// Build histogram data for JSON. Always creates the full percentile distribution.
    /// Optionally writes hlog/csv files if outputPrefix is specified. The file names carry the step index, so no two
    /// steps of a run share a path; the .hlog file is an HdrHistogram interval log that <see cref="HistogramLogReader"/> reads.
    /// </summary>
    internal static HistogramArtifact? BuildHistogramArtifact(
        HistogramSnapshot snapshot,
        int stepIndex,
        int concurrency,
        string? outputPrefix,
        HistogramExportFormat format)
    {
        var histogram = snapshot.GetHistogram();
        if (histogram == null || histogram.TotalCount == 0)
            return null;

        string? hlogPath = null;
        string? csvPath = null;

        if (string.IsNullOrEmpty(outputPrefix) == false)
        {
            var outputDir = Path.GetDirectoryName(outputPrefix);
            if (string.IsNullOrEmpty(outputDir) == false)
            {
                Directory.CreateDirectory(outputDir);
            }

            if (format == HistogramExportFormat.Hlog || format == HistogramExportFormat.Both)
            {
                hlogPath = $"{StepPath(outputPrefix, stepIndex, concurrency)}.hlog";

                try
                {
                    using var fs = File.Create(hlogPath);
                    using var writer = new HistogramLogWriter(fs);
                    writer.Write(DateTime.UtcNow, histogram);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Raven.Bench] Warning: Failed to export hlog for concurrency {concurrency}: {ex.Message}");
                    hlogPath = null;
                }
            }

            if (format == HistogramExportFormat.Csv || format == HistogramExportFormat.Both)
            {
                csvPath = $"{StepPath(outputPrefix, stepIndex, concurrency)}.csv";

                try
                {
                    using var fs = File.Create(csvPath);
                    using var writer = new StreamWriter(fs);

                    writer.WriteLine("Percentile,LatencyMicros,LatencyMs");

                    var csvPercentiles = new[] { 0.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.999, 100.0 };
                    foreach (var p in csvPercentiles)
                    {
                        var valueMicros = histogram.GetValueAtPercentile(p);
                        var valueMs = valueMicros / 1000.0;
                        writer.WriteLine(string.Create(CultureInfo.InvariantCulture, $"{p:F3},{valueMicros},{valueMs:F3}"));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Raven.Bench] Warning: Failed to export CSV for concurrency {concurrency}: {ex.Message}");
                    csvPath = null;
                }
            }
        }

        var percentiles = HistogramArtifact.StandardPercentiles.ToArray();
        var latencyInMicroseconds = new long[percentiles.Length];
        var latencyInMilliseconds = new double[percentiles.Length];

        for (int i = 0; i < percentiles.Length; i++)
        {
            var valueMicros = histogram.GetValueAtPercentile(percentiles[i]);
            latencyInMicroseconds[i] = valueMicros;
            latencyInMilliseconds[i] = Math.Round(valueMicros / 1000.0, 4);
        }

        var binEdgesList = new List<long>();
        var binCountsList = new List<long>();

        foreach (var bucket in histogram.RecordedValues())
        {
            binEdgesList.Add(bucket.ValueIteratedTo);
            binCountsList.Add(bucket.CountAddedInThisIterationStep);
        }

        return new HistogramArtifact
        {
            StepIndex = stepIndex,
            Concurrency = concurrency,
            TotalCount = histogram.TotalCount,
            MaxValueInMicroseconds = snapshot.MaxMicros,
            Percentiles = percentiles,
            LatencyInMicroseconds = latencyInMicroseconds,
            LatencyInMilliseconds = latencyInMilliseconds,
            BinEdges = binEdgesList.ToArray(),
            BinCounts = binCountsList.ToArray(),
            HlogPath = hlogPath,
            CsvPath = csvPath
        };
    }

    private static string StepPath(string outputPrefix, int stepIndex, int concurrency) =>
        string.Create(CultureInfo.InvariantCulture, $"{outputPrefix}-step{stepIndex + 1:D3}-c{concurrency:D4}");
}
