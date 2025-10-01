using System.Net;
using Spectre.Console;
using Spectre.Console.Cli;
using RavenBench.Cli;

namespace RavenBench;

internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var app = new CommandApp();
        app.Configure(cfg =>
        {
            cfg.SetApplicationName("Raven.Bench");
            cfg.ValidateExamples();
            cfg.Settings.StrictParsing = true;
            cfg.PropagateExceptions(); // Let exceptions bubble up to our catch block
            cfg.AddCommand<RunCommand>("run")
                .WithDescription("Run a benchmark ramp and detect the knee.")
                .WithExample("run", "--url", "http://localhost:10101", "--database", "ycsb", "--reads", "75", "--writes", "25", "--compression", "raw:identity", "--concurrency", "8..512x2");
        });

        try
        {
            return await app.RunAsync(args);
        }
        catch (Exception ex)
        {
            // Show the error message
            AnsiConsole.MarkupLine($"[red]Error:[/] {ex.Message}");

            // Add helpful hint for common mistakes
            if (ex is CommandParseException && ex.Message.Contains("Unknown option"))
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[yellow]Hint:[/] Use [cyan]--out[/] (not --out-json) for JSON output, [cyan]--out-csv[/] for CSV output");
                AnsiConsole.WriteLine();

                // Show the help
                await app.RunAsync(new[] { "run", "--help" });
            }
            else if (ex.Message.Contains("concurrency"))
            {
                AnsiConsole.WriteLine();
                AnsiConsole.MarkupLine("[yellow]Hint:[/] Concurrency format is [cyan]start..end[/] or [cyan]start..endxfactor[/] (e.g., [cyan]8..512x2[/])");
            }

            return -1;
        }
    }
}

