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
        if (string.IsNullOrWhiteSpace(settings.Url) || string.IsNullOrWhiteSpace(settings.Database))
        {
            AnsiConsole.MarkupLine("[red]--url and --database are required.[/]");
            return -1;
        }
        var opts = settings.ToRunOptions();
        if (string.Equals(opts.Mode, "closed", StringComparison.OrdinalIgnoreCase) == false)
        {
            AnsiConsole.MarkupLine("[yellow]Only closed-loop mode implemented in v0. Use --mode closed.[/]");
            return -2;
        }

        var runner = new BenchmarkRunner(opts);
        var run = await runner.RunAsync();

        // Knee detection and analysis
        var knee = KneeFinder.FindKnee(run.Steps, opts.KneeThroughputDelta, opts.KneeP95Delta, opts.MaxErrorRate);

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
            Notes = opts.Notes
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
            Notes = opts.Notes
        };

        // Write outputs
        if (string.IsNullOrWhiteSpace(opts.OutJson) == false)
            JsonResultsWriter.Write(opts.OutJson!, summary);
        if (string.IsNullOrWhiteSpace(opts.OutCsv) == false)
            CsvResultsWriter.Write(opts.OutCsv!, summary, opts.SnmpEnabled);

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
}