using RavenBench.Analysis;
using RavenBench.Reporting;
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

        BenchmarkRun run;
        await AnsiConsole.Status()
            .StartAsync("Running ramp…", async _ =>
            {
                var runner = new BenchmarkRunner(opts);
                run = await runner.RunAsync();

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
                    Notes = opts.Notes
                };

                // Write outputs
                if (!string.IsNullOrWhiteSpace(opts.OutJson))
                    JsonResultsWriter.Write(opts.OutJson!, summary);
                if (!string.IsNullOrWhiteSpace(opts.OutCsv))
                    CsvResultsWriter.Write(opts.OutCsv!, summary);

                // Render console report
                RenderResults(summary, run.MaxNetworkUtilization, analysis);
            });

        return 0;
    }

    private static void RenderResults(BenchmarkSummary summary, double maxNetUtil, ResultAnalyzer.Report analysis)
    {
        var table = new Table().Border(TableBorder.Rounded).Caption("[grey]Per-step metrics[/]");
        table.AddColumn("Concurrency");
        table.AddColumn("Thr/s");
        table.AddColumn("p50 ms");
        table.AddColumn("p95 ms");
        table.AddColumn("Errors %");
        table.AddColumn("CPU %");
        table.AddColumn("NetUtil %");
        foreach (var s in summary.Steps)
        {
            table.AddRow(
                s.Concurrency.ToString(),
                s.Throughput.ToString("F0"),
                s.P50Ms.ToString("F1"),
                s.P95Ms.ToString("F1"),
                (s.ErrorRate * 100).ToString("F2"),
                (s.ClientCpu * 100).ToString("F1"),
                (s.NetworkUtilization * 100).ToString("F1"));
        }
        AnsiConsole.Write(table);

        if (summary.Knee is { } knee)
        {
            var panel = new Panel($"KNEE at [bold]{knee.Concurrency}[/]: Thr={knee.Throughput:F0}/s, p95={knee.P95Ms:F1} ms\nReason: {knee.Reason}")
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

