using RavenBench.Core.Ycsb;
using RavenBench.Reporting;
using RavenBench.Ycsb;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

/// <summary>
/// Runs the ycsb scenario's full sequence (load, C, A, B, insert-stream) through the existing
/// closed-loop/rate load generators, and leaves one result per run.
/// </summary>
public sealed class YcsbCommand : AsyncCommand<YcsbSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, YcsbSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Url))
        {
            AnsiConsole.MarkupLine("[red]--url is required.[/]");
            return -1;
        }

        if (string.IsNullOrWhiteSpace(settings.Database))
        {
            AnsiConsole.MarkupLine("[red]--database is required.[/]");
            return -1;
        }

        if (string.IsNullOrWhiteSpace(settings.Scenario))
        {
            AnsiConsole.MarkupLine("[red]--scenario is required.[/]");
            return -1;
        }

        var scenario = YcsbScenario.Load(settings.Scenario);
        var resolved = YcsbScenarioResolver.Resolve(scenario, settings, Environment.GetCommandLineArgs());

        var runner = new YcsbRunner(resolved, settings);
        var results = await runner.RunAsync();

        foreach (var (kind, summary) in results)
        {
            var name = kind.ToResultName();
            var throughput = summary.Steps.Count > 0 ? summary.Steps[^1].Throughput : 0.0;
            AnsiConsole.MarkupLine($"[bold]{name}[/]: {summary.Steps.Count} step(s), last throughput {throughput:F0}/s");

            var outPath = OutputPathFor(settings, name);
            if (outPath != null)
            {
                JsonResultsWriter.Write(outPath, summary);
                AnsiConsole.MarkupLine($"[dim]  wrote {outPath}[/]");
            }
        }

        return 0;
    }

    private static string? OutputPathFor(YcsbSettings settings, string runName)
    {
        if (string.IsNullOrWhiteSpace(settings.OutputDir) == false)
            return $"{settings.OutputDir}-{runName}.json";

        if (string.IsNullOrWhiteSpace(settings.OutJson) == false)
        {
            var dir = Path.GetDirectoryName(settings.OutJson) ?? ".";
            var name = Path.GetFileNameWithoutExtension(settings.OutJson);
            return Path.Combine(dir, $"{name}-{runName}.json");
        }

        return null;
    }
}
