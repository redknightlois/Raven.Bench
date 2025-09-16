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
        if (!string.Equals(opts.Mode, "closed", StringComparison.OrdinalIgnoreCase))
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
        if (!string.IsNullOrWhiteSpace(opts.OutJson))
            JsonResultsWriter.Write(opts.OutJson!, summary);
        if (!string.IsNullOrWhiteSpace(opts.OutCsv))
            CsvResultsWriter.Write(opts.OutCsv!, summary);

        // Render console report
        RenderResults(summary, run.MaxNetworkUtilization, analysis, opts.LatencyDisplay);

        return 0;
    }

    private static void RenderResults(BenchmarkSummary summary, double maxNetUtil, ResultAnalyzer.Report analysis, LatencyDisplayType latencyDisplay)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine();

        // Display tables based on latency type
        if (latencyDisplay == LatencyDisplayType.Normalized || latencyDisplay == LatencyDisplayType.Both)
        {
            var normalizedTable = new Table().Border(TableBorder.Rounded).Caption("[grey]Per-step metrics (RTT-normalized)[/]");
            normalizedTable.AddColumn("Concurrency");
            normalizedTable.AddColumn("Thr/s");
            normalizedTable.AddColumn("p50 ms");
            normalizedTable.AddColumn("p95 ms");
            normalizedTable.AddColumn("Errors %");
            normalizedTable.AddColumn("Client CPU %");
            normalizedTable.AddColumn("Client Net %");
            normalizedTable.AddColumn("Server CPU %");
            normalizedTable.AddColumn("Server Mem MB");
            normalizedTable.AddColumn("Server IO R");
            normalizedTable.AddColumn("Server IO W");
            foreach (var s in summary.Steps)
            {
                normalizedTable.AddRow(
                    s.Concurrency.ToString(),
                    s.Throughput.ToString("F0"),
                    s.Normalized.P50.ToString("F1"),
                    s.Normalized.P95.ToString("F1"),
                    (s.ErrorRate * 100).ToString("F2"),
                    (s.ClientCpu * 100).ToString("F1"),
                    (s.NetworkUtilization * 100).ToString("F1"),
                    s.ServerCpu?.ToString("F1") ?? "N/A",
                    s.ServerMemoryMB?.ToString() ?? "N/A",
                    s.ServerIoReadOps?.ToString() ?? "N/A",
                    s.ServerIoWriteOps?.ToString() ?? "N/A");
            }
            AnsiConsole.Write(normalizedTable);
        }

        if (latencyDisplay == LatencyDisplayType.Raw || latencyDisplay == LatencyDisplayType.Both)
        {
            if (latencyDisplay == LatencyDisplayType.Both)
                AnsiConsole.WriteLine();

            var rawTable = new Table().Border(TableBorder.Rounded).Caption("[grey]Per-step metrics (raw)[/]");
            rawTable.AddColumn("Concurrency");
            rawTable.AddColumn("Thr/s");
            rawTable.AddColumn("p50 ms");
            rawTable.AddColumn("p95 ms");
            rawTable.AddColumn("Errors %");
            rawTable.AddColumn("Client CPU %");
            rawTable.AddColumn("Client Net %");
            rawTable.AddColumn("Server CPU %");
            rawTable.AddColumn("Server Mem MB");
            rawTable.AddColumn("Server IO R");
            rawTable.AddColumn("Server IO W");
            foreach (var s in summary.Steps)
            {
                rawTable.AddRow(
                    s.Concurrency.ToString(),
                    s.Throughput.ToString("F0"),
                    s.Raw.P50.ToString("F1"),
                    s.Raw.P95.ToString("F1"),
                    (s.ErrorRate * 100).ToString("F2"),
                    (s.ClientCpu * 100).ToString("F1"),
                    (s.NetworkUtilization * 100).ToString("F1"),
                    s.ServerCpu?.ToString("F1") ?? "N/A",
                    s.ServerMemoryMB?.ToString() ?? "N/A",
                    s.ServerIoReadOps?.ToString() ?? "N/A",
                    s.ServerIoWriteOps?.ToString() ?? "N/A");
            }
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

        AnsiConsole.MarkupLine($"[bold]Verdict:[/] {summary.Verdict}");

        if (maxNetUtil >= 0.80 && !summary.Options.NetworkLimitedMode)
        {
            AnsiConsole.MarkupLine("[yellow]WARNING: Network-limited. >80% link utilization. Identity runs may be hose-limited.[/]");
        }

        foreach (var w in analysis.Warnings)
            AnsiConsole.MarkupLine($"[yellow]WARNING:[/] {w}");
        if (analysis.UnreliableBeyondKnee)
            AnsiConsole.MarkupLine("[italic]Beyond limits = unreliable.[/]");
    }
}

