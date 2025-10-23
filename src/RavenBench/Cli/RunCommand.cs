using RavenBench.Analysis;
using RavenBench.Reporting;
using RavenBench.Util;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RunCommand : AsyncCommand<RunSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, RunSettings settings)
    {
        // Validate URL (always required)
        if (string.IsNullOrWhiteSpace(settings.Url))
        {
            AnsiConsole.MarkupLine("[red]--url is required.[/]");
            return -1;
        }

        // Database is optional when using dataset (will be auto-generated)
        if (string.IsNullOrWhiteSpace(settings.Database) &&
            string.IsNullOrWhiteSpace(settings.Dataset) &&
            string.IsNullOrWhiteSpace(settings.DatasetProfile))
        {
            AnsiConsole.MarkupLine("[red]--database is required (or use --dataset with --dataset-profile to auto-generate).[/]");
            return -1;
        }
        var opts = settings.ToRunOptions();
        if (string.Equals(opts.Mode, "closed", StringComparison.OrdinalIgnoreCase) == false)
        {
            AnsiConsole.MarkupLine("[yellow]Only closed-loop mode implemented in v0. Use --mode closed.[/]");
            return -2;
        }

        // If using --output-prefix, derive all output paths from it
        if (string.IsNullOrEmpty(opts.OutputDir) == false)
        {
            var prefix = opts.OutputDir;
            var jsonPath = $"{prefix}.json";
            var csvPath = $"{prefix}.csv";
            var histogramPrefix = prefix; // Histograms will use: {prefix}-step-c0004.hlog

            opts = new RunOptions
            {
                Url = opts.Url,
                Database = opts.Database,
                Reads = opts.Reads,
                Writes = opts.Writes,
                Updates = opts.Updates,
                Distribution = opts.Distribution,
                DocumentSizeBytes = opts.DocumentSizeBytes,
                Transport = opts.Transport,
                Compression = opts.Compression,
                Mode = opts.Mode,
                ConcurrencyStart = opts.ConcurrencyStart,
                ConcurrencyEnd = opts.ConcurrencyEnd,
                ConcurrencyFactor = opts.ConcurrencyFactor,
                Warmup = opts.Warmup,
                Duration = opts.Duration,
                MaxErrorRate = opts.MaxErrorRate,
                KneeThroughputDelta = opts.KneeThroughputDelta,
                KneeP95Delta = opts.KneeP95Delta,
                OutJson = jsonPath,
                OutCsv = csvPath,
                Seed = opts.Seed,
                Preload = opts.Preload,
                RawEndpoint = opts.RawEndpoint,
                ThreadPoolWorkers = opts.ThreadPoolWorkers,
                ThreadPoolIOCP = opts.ThreadPoolIOCP,
                Notes = opts.Notes,
                ExpectedCores = opts.ExpectedCores,
                NetworkLimitedMode = opts.NetworkLimitedMode,
                LinkMbps = opts.LinkMbps,
                HttpVersion = opts.HttpVersion,
                StrictHttpVersion = opts.StrictHttpVersion,
                Verbose = opts.Verbose,
                LatencyDisplay = opts.LatencyDisplay,
                Snmp = opts.Snmp,
                Profile = opts.Profile,
                QueryProfile = opts.QueryProfile,
                BulkBatchSize = opts.BulkBatchSize,
                BulkDepth = opts.BulkDepth,
                Dataset = opts.Dataset,
                DatasetProfile = opts.DatasetProfile,
                DatasetSize = opts.DatasetSize,
                DatasetSkipIfExists = opts.DatasetSkipIfExists,
                DatasetCacheDir = opts.DatasetCacheDir,
                OutputDir = opts.OutputDir,
                LatencyHistogramsDir = histogramPrefix,
                LatencyHistogramsFormat = opts.LatencyHistogramsFormat
            };

            AnsiConsole.MarkupLine($"[dim]Output prefix: {prefix}[/]");
            AnsiConsole.MarkupLine($"[dim]  JSON: {jsonPath}[/]");
            AnsiConsole.MarkupLine($"[dim]  CSV: {csvPath}[/]");
            AnsiConsole.MarkupLine($"[dim]  Histograms: {histogramPrefix}-step-cXXXX.hlog[/]");
        }
        // Auto-enable histogram export when you specify --out-csv
        else if (string.IsNullOrEmpty(opts.LatencyHistogramsDir) &&
                 string.IsNullOrEmpty(opts.OutCsv) == false)
        {
            var outputPath = opts.OutCsv;
            var outputDir = Path.GetDirectoryName(outputPath) ?? ".";
            var outputName = Path.GetFileNameWithoutExtension(outputPath);
            var histogramPrefix = Path.Combine(outputDir, outputName);

            opts = new RunOptions
            {
                Url = opts.Url,
                Database = opts.Database,
                Reads = opts.Reads,
                Writes = opts.Writes,
                Updates = opts.Updates,
                Distribution = opts.Distribution,
                DocumentSizeBytes = opts.DocumentSizeBytes,
                Transport = opts.Transport,
                Compression = opts.Compression,
                Mode = opts.Mode,
                ConcurrencyStart = opts.ConcurrencyStart,
                ConcurrencyEnd = opts.ConcurrencyEnd,
                ConcurrencyFactor = opts.ConcurrencyFactor,
                Warmup = opts.Warmup,
                Duration = opts.Duration,
                MaxErrorRate = opts.MaxErrorRate,
                KneeThroughputDelta = opts.KneeThroughputDelta,
                KneeP95Delta = opts.KneeP95Delta,
                OutJson = opts.OutJson,
                OutCsv = opts.OutCsv,
                Seed = opts.Seed,
                Preload = opts.Preload,
                RawEndpoint = opts.RawEndpoint,
                ThreadPoolWorkers = opts.ThreadPoolWorkers,
                ThreadPoolIOCP = opts.ThreadPoolIOCP,
                Notes = opts.Notes,
                ExpectedCores = opts.ExpectedCores,
                NetworkLimitedMode = opts.NetworkLimitedMode,
                LinkMbps = opts.LinkMbps,
                HttpVersion = opts.HttpVersion,
                StrictHttpVersion = opts.StrictHttpVersion,
                Verbose = opts.Verbose,
                LatencyDisplay = opts.LatencyDisplay,
                Snmp = opts.Snmp,
                Profile = opts.Profile,
                QueryProfile = opts.QueryProfile,
                BulkBatchSize = opts.BulkBatchSize,
                BulkDepth = opts.BulkDepth,
                Dataset = opts.Dataset,
                DatasetProfile = opts.DatasetProfile,
                DatasetSize = opts.DatasetSize,
                DatasetSkipIfExists = opts.DatasetSkipIfExists,
                DatasetCacheDir = opts.DatasetCacheDir,
                OutputDir = opts.OutputDir,
                LatencyHistogramsDir = histogramPrefix,
                LatencyHistogramsFormat = opts.LatencyHistogramsFormat
            };

            AnsiConsole.MarkupLine($"[dim]Histogram export (hlog): {histogramPrefix}-step-cXXXX.hlog[/]");
        }

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Knee detection and analysis
        var knee = KneeFinder.FindKnee(run.Steps, opts.KneeThroughputDelta, opts.KneeP95Delta, opts.MaxErrorRate);

        // Build SNMP time series and aggregations from history
        var (snmpTimeSeries, snmpAggregations) = BuildSnmpData(run.ServerMetricsHistory);

        // Create a temporary summary for analysis
        var tempSummary = new BenchmarkSummary
        {
            Options = opts,
            Steps = run.Steps,
            Knee = knee,
            Verdict = "temporary",
            ClientCompression = run.ClientCompression,
            EffectiveHttpVersion = run.EffectiveHttpVersion,
            StartupCalibration = run.StartupCalibration,
            Notes = opts.Notes,
            SnmpTimeSeries = snmpTimeSeries,
            SnmpAggregations = snmpAggregations
        };

        // Perform complete result analysis
        var analysis = ResultAnalyzer.Analyze(run, knee, opts, tempSummary);

        // Create final summary with analysis results
        var summary = new BenchmarkSummary
        {
            Options = opts,
            Steps = run.Steps,
            Knee = knee,
            Verdict = analysis.Verdict,
            ClientCompression = run.ClientCompression,
            EffectiveHttpVersion = run.EffectiveHttpVersion,
            StartupCalibration = run.StartupCalibration,
            Notes = opts.Notes,
            SnmpTimeSeries = snmpTimeSeries,
            SnmpAggregations = snmpAggregations,
            HistogramArtifacts = run.HistogramArtifacts
        };

        // Write outputs
        if (string.IsNullOrWhiteSpace(opts.OutJson) == false)
        {
            AnsiConsole.MarkupLine($"[dim]Attempting to write JSON to: {opts.OutJson}[/]");
            JsonResultsWriter.Write(opts.OutJson!, summary);
        }
        if (string.IsNullOrWhiteSpace(opts.OutCsv) == false)
        {
            AnsiConsole.MarkupLine($"[dim]Attempting to write CSV to: {opts.OutCsv}[/]");
            CsvResultsWriter.Write(opts.OutCsv!, summary, opts.SnmpEnabled);
        }

        // Render console report
        RenderResults(summary, run.MaxNetworkUtilization, analysis, opts.LatencyDisplay);

        return 0;
    }

    private static void RenderResults(BenchmarkSummary summary, double maxNetUtil, ResultAnalyzer.Report analysis, LatencyDisplayType latencyDisplay)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine();

        if (latencyDisplay == LatencyDisplayType.Normalized || latencyDisplay == LatencyDisplayType.Both)
        {
            var normalizedTable = BuildTable(summary, "Per-step metrics (RTT-normalized)", TableScope.Normalized);
            AnsiConsole.Write(normalizedTable);
        }

        if (latencyDisplay == LatencyDisplayType.Raw || latencyDisplay == LatencyDisplayType.Both)
        {
            if (latencyDisplay == LatencyDisplayType.Both)
                AnsiConsole.WriteLine();

            var rawTable = BuildTable(summary, "Per-step metrics (raw)", TableScope.Raw);
            AnsiConsole.Write(rawTable);
        }
    
        if (summary.Knee is { } knee)
        {
            var kneeMessage = latencyDisplay switch
            {
                LatencyDisplayType.Raw => $"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Raw.P95:F1}ms\nReason: {knee.Reason}",
                LatencyDisplayType.Both => $"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Normalized.P95:F1}ms (raw:{knee.Raw.P95:F1}ms)\nReason: {knee.Reason}",
                _ => $"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Normalized.P95:F1}ms\nReason: {knee.Reason}"
            };
            var panel = new Panel(kneeMessage)
                .Header("Knee Summary").BorderColor(Color.Yellow);
            AnsiConsole.Write(panel);
        }

        AnsiConsole.MarkupLine($"[bold]Verdict:[/]\n {summary.Verdict}");

        if (maxNetUtil >= 0.80 && summary.Options.NetworkLimitedMode == false)
        {
            AnsiConsole.MarkupLine("[yellow]WARNING: Network-limited. >80% link utilization. Identity runs may be hose-limited.[/]");
        }

        foreach (var w in analysis.Warnings)
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/]\n {w}");

        foreach (var w in CheckForSnmpDiscrepancy(summary))
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/]\n {w}");

        if (analysis.UnreliableBeyondKnee)
            AnsiConsole.MarkupLine("[italic]Beyond limits = unreliable.[/]");
    }

    private static Table BuildTable(BenchmarkSummary summary, string title, TableScope scope)
    {
        var table = new Table().Border(TableBorder.Rounded).Caption($"[grey]{title}[/]");
        var visibleColumns = TableMetrics.GetVisibleColumns(summary, scope);

        foreach (var column in visibleColumns)
        {
            table.AddColumn(column.Header);
        }

        for (var i = 0; i < visibleColumns.Count; i++)
        {
            var column = visibleColumns[i];
            var tableColumn = table.Columns[i];
            column.ConfigureColumn?.Invoke(tableColumn);
        }

        foreach (var step in summary.Steps)
        {
            var row = visibleColumns.Select(c => c.ValueSelector(step)).ToArray();
            table.AddRow(row);
        }

        return table;
    }

    private static IEnumerable<string> CheckForSnmpDiscrepancy(BenchmarkSummary summary)
    {
        if (summary.Options.SnmpEnabled == false || summary.Options.Snmp.Profile != Util.SnmpProfile.Extended)
            yield break;

        foreach (var s in summary.Steps)
        {
            if (s.Throughput > 0 && s.ServerSnmpRequestsPerSec.HasValue && s.ServerSnmpRequestsPerSec > 0)
            {
                var clientRate = s.Throughput;
                var serverRate = s.ServerSnmpRequestsPerSec.Value;

                var diff = Math.Abs(clientRate - serverRate);
                var avg = (clientRate + serverRate) / 2;

                if (avg > 0 && (diff / avg) > 0.10)
                {
                    yield return $"SNMP request rate discrepancy detected at concurrency {s.Concurrency}. Client: {clientRate:F0} req/s, Server: {serverRate:F0} req/s. This may indicate that the server-side metrics are not reliable.";
                }
            }
        }
    }

    private static (List<Reporting.SnmpTimeSeries>?, Reporting.SnmpAggregations?) BuildSnmpData(List<Metrics.ServerMetrics>? history)
    {
        if (history == null || history.Count == 0)
            return (null, null);

        var timeSeries = new List<Reporting.SnmpTimeSeries>();

        // Build time series from server metrics history
        foreach (var snapshot in history)
        {
            timeSeries.Add(new Reporting.SnmpTimeSeries
            {
                Timestamp = snapshot.Timestamp,
                MachineCpu = snapshot.MachineCpu,
                ProcessCpu = snapshot.ProcessCpu,
                ManagedMemoryMb = snapshot.ManagedMemoryMb,
                UnmanagedMemoryMb = snapshot.UnmanagedMemoryMb,
                DirtyMemoryMb = snapshot.DirtyMemoryMb,
                Load1Min = snapshot.Load1Min,
                SnmpIoReadOpsPerSec = snapshot.SnmpIoReadOpsPerSec,
                SnmpIoWriteOpsPerSec = snapshot.SnmpIoWriteOpsPerSec,
                SnmpIoReadBytesPerSec = snapshot.SnmpIoReadBytesPerSec,
                SnmpIoWriteBytesPerSec = snapshot.SnmpIoWriteBytesPerSec,
                ServerSnmpRequestsPerSec = snapshot.ServerSnmpRequestsPerSec
            });
        }

        // Compute IO aggregations
        var totalReadOps = history.Where(h => h.SnmpIoReadOpsPerSec.HasValue).Sum(h => h.SnmpIoReadOpsPerSec!.Value);
        var totalWriteOps = history.Where(h => h.SnmpIoWriteOpsPerSec.HasValue).Sum(h => h.SnmpIoWriteOpsPerSec!.Value);
        var totalReadBytes = history.Where(h => h.SnmpIoReadBytesPerSec.HasValue).Sum(h => h.SnmpIoReadBytesPerSec!.Value);
        var totalWriteBytes = history.Where(h => h.SnmpIoWriteBytesPerSec.HasValue).Sum(h => h.SnmpIoWriteBytesPerSec!.Value);

        var countReadOps = history.Count(h => h.SnmpIoReadOpsPerSec.HasValue);
        var countWriteOps = history.Count(h => h.SnmpIoWriteOpsPerSec.HasValue);
        var countReadBytes = history.Count(h => h.SnmpIoReadBytesPerSec.HasValue);
        var countWriteBytes = history.Count(h => h.SnmpIoWriteBytesPerSec.HasValue);

        var aggregations = new Reporting.SnmpAggregations
        {
            TotalSnmpIoReadOps = countReadOps > 0 ? totalReadOps : null,
            AverageSnmpIoReadOpsPerSec = countReadOps > 0 ? totalReadOps / countReadOps : null,
            TotalSnmpIoWriteOps = countWriteOps > 0 ? totalWriteOps : null,
            AverageSnmpIoWriteOpsPerSec = countWriteOps > 0 ? totalWriteOps / countWriteOps : null,
            TotalSnmpIoReadBytes = countReadBytes > 0 ? totalReadBytes : null,
            AverageSnmpIoReadBytesPerSec = countReadBytes > 0 ? totalReadBytes / countReadBytes : null,
            TotalSnmpIoWriteBytes = countWriteBytes > 0 ? totalWriteBytes : null,
            AverageSnmpIoWriteBytesPerSec = countWriteBytes > 0 ? totalWriteBytes / countWriteBytes : null
        };

        return (timeSeries, aggregations);
    }
}