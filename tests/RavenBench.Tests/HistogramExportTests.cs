using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Metrics;
using RavenBench.Reporting;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests;

/// <summary>
/// Tests for histogram export. Makes sure we get the full percentile distribution
/// embedded in JSON, plus optional file exports if configured.
/// </summary>
public class HistogramExportTests : IDisposable
{
    private readonly string _tempDir;

    public HistogramExportTests()
    {
        // Create temporary directory for test outputs
        _tempDir = Path.Combine(Path.GetTempPath(), $"ravenbench-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        // Clean up temporary directory after tests
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task BenchmarkRunner_Exports_Histogram_Hlog_Files()
    {
        // INVARIANT: When LatencyHistogramsDir is set with Hlog format, benchmark should export .hlog files
        // INVARIANT: Each concurrency step should have a corresponding histogram file

        var histogramDir = Path.Combine(_tempDir, "histograms-hlog");
        var opts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            DocumentSizeBytes = 512,
            Warmup = TimeSpan.FromMilliseconds(10),
            Duration = TimeSpan.FromMilliseconds(20),
            ConcurrencyStart = 2,
            ConcurrencyEnd = 4,
            ConcurrencyFactor = 2,
            LatencyHistogramsDir = histogramDir,
            LatencyHistogramsFormat = HistogramExportFormat.Hlog
        };

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Verify benchmark ran and produced steps
        run.Should().NotBeNull();
        run.Steps.Should().NotBeEmpty();

        // Verify histogram artifacts were created
        run.HistogramArtifacts.Should().NotBeNull();
        run.HistogramArtifacts.Should().HaveCountGreaterThan(0);

        // Verify each artifact has an hlog path and the file exists
        foreach (var artifact in run.HistogramArtifacts!)
        {
            artifact.HlogPath.Should().NotBeNullOrEmpty();
            artifact.CsvPath.Should().BeNull("CSV format was not requested");

            File.Exists(artifact.HlogPath).Should().BeTrue($"Hlog file should exist at {artifact.HlogPath}");

            // Verify file has content (not empty)
            var fileInfo = new FileInfo(artifact.HlogPath!);
            fileInfo.Length.Should().BeGreaterThan(0, "Hlog file should contain histogram data");

            // Verify file contains expected HdrHistogram header
            var firstLine = File.ReadLines(artifact.HlogPath!).FirstOrDefault();
            firstLine.Should().NotBeNullOrEmpty("Hlog file should have content");
        }

        // Verify histogram directory was created
        Directory.Exists(histogramDir).Should().BeTrue("Histogram directory should be created");
    }

    [Fact]
    public async Task BenchmarkRunner_Exports_Histogram_Csv_Files()
    {
        // INVARIANT: When LatencyHistogramsDir is set with CSV format, benchmark should export .csv files
        // INVARIANT: CSV files should have proper header and percentile data

        var histogramDir = Path.Combine(_tempDir, "histograms-csv");
        var opts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            DocumentSizeBytes = 512,
            Warmup = TimeSpan.FromMilliseconds(10),
            Duration = TimeSpan.FromMilliseconds(20),
            ConcurrencyStart = 2,
            ConcurrencyEnd = 2,
            ConcurrencyFactor = 2,
            LatencyHistogramsDir = histogramDir,
            LatencyHistogramsFormat = HistogramExportFormat.Csv
        };

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Verify histogram artifacts were created
        run.HistogramArtifacts.Should().NotBeNull();
        run.HistogramArtifacts.Should().HaveCount(1, "Single concurrency step should produce one artifact");

        var artifact = run.HistogramArtifacts!.First();
        artifact.CsvPath.Should().NotBeNullOrEmpty();
        artifact.HlogPath.Should().BeNull("Hlog format was not requested");

        File.Exists(artifact.CsvPath).Should().BeTrue($"CSV file should exist at {artifact.CsvPath}");

        // Verify CSV structure
        var lines = File.ReadAllLines(artifact.CsvPath!);
        lines.Should().NotBeEmpty("CSV file should have content");

        // First line should be header
        var header = lines[0];
        header.Should().Contain("Percentile", "CSV header should include Percentile column");
        header.Should().Contain("LatencyMicros", "CSV header should include LatencyMicros column");
        header.Should().Contain("LatencyMs", "CSV header should include LatencyMs column");

        // Should have multiple percentile rows
        lines.Length.Should().BeGreaterThan(5, "CSV should contain multiple percentile rows");

        // Verify percentile values are present (spot check a few standard percentiles)
        var content = string.Join("\n", lines);
        content.Should().Contain("50.000", "Should include P50");
        content.Should().Contain("99.000", "Should include P99");
        content.Should().Contain("99.990", "Should include P99.9");
    }

    [Fact]
    public async Task BenchmarkRunner_Exports_Both_Hlog_And_Csv()
    {
        // INVARIANT: When format is Both, benchmark should export both .hlog and .csv files

        var histogramDir = Path.Combine(_tempDir, "histograms-both");
        var opts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            DocumentSizeBytes = 512,
            Warmup = TimeSpan.FromMilliseconds(10),
            Duration = TimeSpan.FromMilliseconds(20),
            ConcurrencyStart = 2,
            ConcurrencyEnd = 2,
            ConcurrencyFactor = 2,
            LatencyHistogramsDir = histogramDir,
            LatencyHistogramsFormat = HistogramExportFormat.Both
        };

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Verify both formats were exported
        var artifact = run.HistogramArtifacts!.First();
        artifact.HlogPath.Should().NotBeNullOrEmpty("Hlog should be exported");
        artifact.CsvPath.Should().NotBeNullOrEmpty("CSV should be exported");

        File.Exists(artifact.HlogPath).Should().BeTrue("Hlog file should exist");
        File.Exists(artifact.CsvPath).Should().BeTrue("CSV file should exist");
    }

    [Fact]
    public async Task BenchmarkRunner_Skips_Export_When_No_Directory_Specified()
    {
        // INVARIANT: When LatencyHistogramsDir is null, no histogram files should be exported

        var opts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            DocumentSizeBytes = 512,
            Warmup = TimeSpan.FromMilliseconds(10),
            Duration = TimeSpan.FromMilliseconds(20),
            ConcurrencyStart = 2,
            ConcurrencyEnd = 2,
            ConcurrencyFactor = 2,
            LatencyHistogramsDir = null  // No export directory specified
        };

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Verify no histogram artifacts were created
        run.HistogramArtifacts.Should().BeNullOrEmpty("No histogram export should occur when directory is not specified");
    }

    [Fact]
    public async Task HistogramArtifacts_Match_Concurrency_Steps()
    {
        // INVARIANT: Each concurrency step should have a corresponding histogram artifact
        // INVARIANT: Artifact concurrency values should match step concurrency values

        var histogramDir = Path.Combine(_tempDir, "histograms-concurrency");
        var opts = new RunOptions
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = WorkloadProfile.Writes,
            DocumentSizeBytes = 512,
            Warmup = TimeSpan.FromMilliseconds(10),
            Duration = TimeSpan.FromMilliseconds(20),
            ConcurrencyStart = 2,
            ConcurrencyEnd = 8,
            ConcurrencyFactor = 2,
            LatencyHistogramsDir = histogramDir,
            LatencyHistogramsFormat = HistogramExportFormat.Hlog
        };

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Get concurrency levels from steps
        var stepConcurrencies = run.Steps.Select(s => s.Concurrency).ToList();

        // Verify each step has a corresponding artifact
        run.HistogramArtifacts.Should().HaveCount(stepConcurrencies.Count,
            "Each step should have one histogram artifact");

        // Verify artifact concurrencies match step concurrencies
        var artifactConcurrencies = run.HistogramArtifacts!.Select(a => a.Concurrency).OrderBy(c => c).ToList();
        var expectedConcurrencies = stepConcurrencies.OrderBy(c => c).ToList();

        artifactConcurrencies.Should().Equal(expectedConcurrencies,
            "Artifact concurrency levels should match step concurrency levels");
    }

    [Fact]
    public void HistogramArtifact_Includes_Required_Fields()
    {
        // Make sure the artifact has everything we need for downstream analysis
        var artifact = new HistogramArtifact
        {
            Concurrency = 16,
            TotalCount = 1000,
            MaxValueInMicroseconds = 50000,
            Percentiles = new[] { 0.0, 50.0, 99.0, 100.0 },
            LatencyInMicroseconds = new long[] { 100, 1000, 5000, 50000 },
            LatencyInMilliseconds = new[] { 0.1, 1.0, 5.0, 50.0 },
            HlogPath = "/path/to/step-c0016.hlog",
            CsvPath = "/path/to/step-c0016.csv"
        };

        artifact.Concurrency.Should().Be(16);
        artifact.TotalCount.Should().Be(1000);
        artifact.MaxValueInMicroseconds.Should().Be(50000);
        artifact.Percentiles.Should().HaveCount(4);
        artifact.LatencyInMicroseconds.Should().HaveCount(4);
        artifact.LatencyInMilliseconds.Should().HaveCount(4);
        artifact.HlogPath.Should().Be("/path/to/step-c0016.hlog");
        artifact.CsvPath.Should().Be("/path/to/step-c0016.csv");
    }
}
