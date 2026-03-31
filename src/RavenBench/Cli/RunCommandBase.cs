using System;
using System.Collections.Generic;
using System.Linq;
using RavenBench.Analysis;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Reporting;
using Spectre.Console;
using Spectre.Console.Cli;
using RecallResult = RavenBench.Core.Metrics.RecallResult;

namespace RavenBench.Cli;

public abstract class RunCommandBase<TSettings> : AsyncCommand<TSettings> where TSettings : BaseRunSettings
{
    protected abstract RunOptions BuildRunOptions(TSettings settings);

    public override async Task<int> ExecuteAsync(CommandContext context, TSettings settings)
    {
        if (ValidateRequiredSettings(settings) == false)
            return -1;

        var opts = CliParsing.ApplyOutputOptions(BuildRunOptions(settings));

        PrintOutputPaths(opts);

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        var knee = KneeFinder.FindKnee(run.Steps, opts.KneeThroughputDelta, opts.KneeP95Delta, opts.MaxErrorRate);
        var (snmpTimeSeries, snmpAggregations) = SnmpSummaryBuilder.Build(run.ServerMetricsHistory);

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

        var analysis = ResultAnalyzer.Analyze(run, knee, opts, tempSummary);

        // Run recall@K measurement if requested and vector metadata is available
        RecallResult? recallResult = null;
        Dictionary<int, RecallResult>? recallSweep = null;
        if (opts.VectorRecallKs is { Length: > 0 } && run.VectorMetadata != null)
        {
            var recall = new Dataset.RecallMeasurement();
            var httpVersion = opts.HttpVersion != "auto" ? HttpHelper.ParseHttpVersion(HttpHelper.NormalizeHttpVersion(opts.HttpVersion)) : null;

            if (opts.VectorRecallEfSweep is { Length: > 0 })
            {
                recallSweep = await recall.MeasureSweepAsync(
                    opts.Url,
                    run.EffectiveDatabase,
                    run.VectorMetadata,
                    opts.VectorRecallKs,
                    opts.VectorRecallEfSweep,
                    opts.VectorQuantization,
                    opts.SearchEngine,
                    httpVersion);
                // Use the last (highest) efSearch result as the primary recall
                recallResult = recallSweep[recallSweep.Keys.Max()];
            }
            else
            {
                recallResult = await recall.MeasureAsync(
                    opts.Url,
                    run.EffectiveDatabase,
                    run.VectorMetadata,
                    opts.VectorRecallKs,
                    opts.VectorQuantization,
                    opts.SearchEngine,
                    httpVersion);
            }
        }

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
            HistogramArtifacts = run.HistogramArtifacts,
            Recall = recallResult,
            RecallSweep = recallSweep
        };

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

        RenderResults(summary, run.MaxNetworkUtilization, analysis, opts.LatencyDisplay);

        return 0;
    }

    private static bool ValidateRequiredSettings(BaseRunSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Url))
        {
            AnsiConsole.MarkupLine("[red]--url is required.[/]");
            return false;
        }

        if (string.IsNullOrWhiteSpace(settings.Database) &&
            string.IsNullOrWhiteSpace(settings.Dataset) &&
            string.IsNullOrWhiteSpace(settings.DatasetProfile))
        {
            AnsiConsole.MarkupLine("[red]--database is required (or use --dataset with --dataset-profile to auto-generate).[/]");
            return false;
        }

        return true;
    }

    private static void PrintOutputPaths(RunOptions opts)
    {
        if (string.IsNullOrEmpty(opts.OutputDir))
            return;

        var prefix = opts.OutputDir;
        AnsiConsole.MarkupLine($"[dim]Output prefix: {prefix}[/]");
        AnsiConsole.MarkupLine($"[dim]  JSON: {prefix}.json[/]");
        AnsiConsole.MarkupLine($"[dim]  CSV: {prefix}.csv[/]");
        AnsiConsole.MarkupLine($"[dim]  Histograms: {prefix}-step-cXXXX.hlog[/]");
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
            var kneeValue = knee.TargetThroughput.HasValue ? $"{knee.TargetThroughput.Value:F0} RPS" : $"{knee.Concurrency}";
            var kneeMessage = latencyDisplay switch
            {
                LatencyDisplayType.Raw => $"KNEE at [bold]{kneeValue}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Raw.P95:F1}ms\nReason: {knee.Reason}",
                LatencyDisplayType.Both => $"KNEE at [bold]{kneeValue}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Normalized.P95:F1}ms (raw:{knee.Raw.P95:F1}ms)\nReason: {knee.Reason}",
                _ => $"KNEE at [bold]{kneeValue}[/]: Thr={knee.Throughput:F0}/s, p95={knee.Normalized.P95:F1}ms\nReason: {knee.Reason}"
            };
            var panel = new Panel(kneeMessage).Header("Knee Summary").BorderColor(Color.Yellow);
            AnsiConsole.Write(panel);
        }

        AnsiConsole.MarkupLine($"[bold]Verdict:[/]\n {summary.Verdict}");

        // Render recall@K results if available
        if (summary.RecallSweep is { Count: > 0 } sweep)
        {
            // Sweep mode: show a table of efSearch × recall@K
            var table = new Table().Border(TableBorder.Rounded).Title("[blue]Recall@K by efSearch[/]");
            table.AddColumn("efSearch");
            var ks = sweep.Values.First().RecallAtK.Keys.OrderBy(k => k).ToList();
            foreach (var k in ks)
                table.AddColumn($"recall@{k}");
            table.AddColumn("time");

            foreach (var (ef, result) in sweep.OrderBy(kvp => kvp.Key))
            {
                var row = new List<string> { ef.ToString() };
                foreach (var k in ks)
                    row.Add(result.RecallAtK.TryGetValue(k, out var v) ? $"{v:P2}" : "-");
                row.Add($"{result.MeasurementTime.TotalSeconds:F1}s");
                table.AddRow(row.ToArray());
            }

            AnsiConsole.Write(table);
        }
        else if (summary.Recall?.RecallAtK is { Count: > 0 } recallAtK)
        {
            var recallLines = recallAtK
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => $"recall@{kvp.Key} = {kvp.Value:P2}");
            var recallText = string.Join(" | ", recallLines);
            var cached = summary.Recall.GroundTruthCached ? " (ground truth cached)" : $" (ground truth computed in {summary.Recall.GroundTruthComputeTime.TotalSeconds:F1}s)";
            var recallPanel = new Panel($"{recallText}\n[dim]{summary.Recall.QueryCount} queries, measurement: {summary.Recall.MeasurementTime.TotalSeconds:F1}s{cached}[/]")
                .Header("Recall@K")
                .BorderColor(Color.Blue);
            AnsiConsole.Write(recallPanel);
        }

        if (maxNetUtil >= 0.80 && summary.Options.NetworkLimitedMode == false)
        {
            AnsiConsole.MarkupLine("[yellow]WARNING: Network-limited. >80% link utilization. Identity runs may be hose-limited.[/]");
        }

        foreach (var warning in analysis.Warnings)
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/]\n {warning}");

        foreach (var warning in CheckForSnmpDiscrepancy(summary))
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/]\n {warning}");

        if (analysis.UnreliableBeyondKnee)
            AnsiConsole.MarkupLine("[italic]Beyond limits = unreliable.[/]");
    }

    private static Table BuildTable(BenchmarkSummary summary, string title, TableScope scope)
    {
        var table = new Table().Border(TableBorder.Rounded).Caption($"[grey]{title}[/]");
        var visibleColumns = TableMetrics.GetVisibleColumns(summary, scope);

        foreach (var column in visibleColumns)
            table.AddColumn(column.Header);

        for (var i = 0; i < visibleColumns.Count; i++)
        {
            var column = visibleColumns[i];
            column.ConfigureColumn?.Invoke(table.Columns[i]);
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
        if (summary.Options.SnmpEnabled == false || summary.Options.Snmp.Profile != SnmpProfile.Extended)
            yield break;

        foreach (var step in summary.Steps)
        {
            if (step.Throughput <= 0 || step.ServerSnmpRequestsPerSec.HasValue == false || step.ServerSnmpRequestsPerSec <= 0)
                continue;

            var clientRate = step.Throughput;
            var serverRate = step.ServerSnmpRequestsPerSec.Value;
            var diff = Math.Abs(clientRate - serverRate);
            var avg = (clientRate + serverRate) / 2;

            if (avg > 0 && (diff / avg) > 0.10)
            {
                var rateInfo = step.TargetThroughput.HasValue ? $"{step.TargetThroughput.Value:F0} RPS" : $"concurrency {step.Concurrency}";
                yield return $"SNMP request rate discrepancy detected at {rateInfo}. Client: {clientRate:F0} req/s, Server: {serverRate:F0} req/s. This may indicate that the server-side metrics are not reliable.";
            }
        }
    }

}
