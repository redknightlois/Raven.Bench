using System.Net;
using Spectre.Console;
using Spectre.Console.Cli;
using RavenBench.Cli;

namespace RavenBench;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);

        var app = new CommandApp();
        app.Configure(Configure);

        try
        {
            return await app.RunAsync(args);
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteException(ex);

            if (ex is CommandParseException && ex.Message.Contains("Unknown option"))
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[yellow]Hint:[/] Use [cyan]--out[/] (not --out-json) for JSON output, [cyan]--out-csv[/] for CSV output");
                AnsiConsole.WriteLine();

                await app.RunAsync(new[] { "closed", "--help" });
            }
            else if (ex.Message.Contains("concurrency"))
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[yellow]Hint:[/] Concurrency format is [cyan]start..end[/] or [cyan]start..endxfactor[/] (e.g., [cyan]8..512x2[/])");
            }
            else if (ex.Message.Contains("step"))
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[yellow]Hint:[/] Step format is [cyan]start..end[/] or [cyan]start..endxfactor[/] (e.g., [cyan]200..20000x1.5[/])");
            }

            return -1;
        }
    }

    /// <summary>
    /// Registers every command and example. Separated from <see cref="Main"/> so the startup
    /// validation of the examples can be exercised without launching the application.
    /// </summary>
    internal static void Configure(IConfigurator cfg)
    {
        cfg.SetApplicationName("Raven.Bench");
        cfg.ValidateExamples();
        cfg.Settings.StrictParsing = true;
        cfg.PropagateExceptions(); // Let exceptions bubble up to our catch block
        cfg.AddCommand<ClosedCommand>("closed")
            .WithDescription("Run a closed-loop benchmark ramp and detect the knee.")
            .WithExample("closed", "--url", "http://localhost:10101", "--database", "ycsb", "--profile", "query-by-id", "--preload", "100000", "--compression", "identity", "--concurrency", "8..512x2");
        cfg.AddCommand<RateCommand>("rate")
            .WithDescription("Run a rate-based benchmark with constant RPS steps.")
            .WithExample("rate", "--url", "http://localhost:10101", "--database", "ycsb", "--profile", "query-by-id", "--preload", "100000", "--compression", "identity", "--step", "200..20000x1.5");
        cfg.AddCommand<RecallCommand>("recall")
            .WithDescription("Measure recall@K only (no throughput benchmark). Requires data already imported.")
            .WithExample("recall", "--url", "http://localhost:10101", "--dataset", "sphere", "--dataset-profile", "100k", "--vector-quantization", "Int2", "--vector-recall-ef-sweep", "64,128,256,512");
        cfg.AddCommand<IndexBuildCommand>("index-build")
            .WithDescription("Build a static index from scratch and report build time and docs/s. Leaves the database indexed for reuse.")
            .WithExample("index-build", "--url", "http://localhost:10101", "--dataset", "stackoverflow", "--dataset-profile", "small", "--index-kind", "fanout");
        cfg.AddCommand<YcsbCommand>("ycsb")
            .WithDescription("Run the ycsb scenario's load, C, A, B and insert-stream sequence, one result per run.")
            .WithExample("ycsb", "--url", "http://localhost:10101", "--database", "ycsb", "--scenario", "benchmarks/ycsb/scenario.json");
    }
}
