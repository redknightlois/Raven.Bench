using System.ComponentModel;
using System.Globalization;
using RavenBench.Core.Vector;
using RavenBench.Reporting;
using RavenBench.VectorBench;
using RavenBench.Ycsb;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

/// <summary>
/// The vector command's options. Every option that overrides a scenario key is nullable, so its
/// presence alone says it was given: an explicit value wins over the file, the file wins over nothing.
/// </summary>
public sealed class VectorSettings : CommandSettings
{
    [CommandOption("--scenario")]
    [Description("Path to the vector scenario file (JSON).")]
    public string? Scenario { get; init; }

    [CommandOption("--target")]
    [Description("ravendb, ravendb-7, pgvector or elasticsearch.")]
    public string? Target { get; init; }

    [CommandOption("--url")]
    [Description("The target endpoint: a RavenDB or Elasticsearch URL, or a PostgreSQL connection string.")]
    public string? Url { get; init; }

    [CommandOption("--database")]
    [Description("A fresh database the run loads into; for Elasticsearch, the index name.")]
    public string? Database { get; init; }

    [CommandOption("--output-prefix")]
    [Description("Result files are written as <prefix>-<run>.json, histograms beside them.")]
    public string? OutputPrefix { get; init; }

    [CommandOption("--node-exporter-url")]
    [Description("node_exporter metrics endpoint on the database host (e.g. http://dbhost:9100/metrics).")]
    public string? NodeExporterUrl { get; init; }

    [CommandOption("--keep-data")]
    [Description("Leave the loaded database behind instead of removing it.")]
    public bool KeepData { get; init; }

    [CommandOption("--dataset")]
    [Description("Overrides the scenario key Dataset.")]
    public string? Dataset { get; init; }
    [CommandOption("--vector-count-cap")]
    [Description("Overrides the scenario key VectorCountCap.")]
    public int? VectorCountCap { get; init; }
    [CommandOption("--data-dir")]
    [Description("Overrides the scenario key DataDirectory.")]
    public string? DataDirectory { get; init; }
    [CommandOption("--seed")]
    [Description("Overrides the scenario key Seed.")]
    public int? Seed { get; init; }
    [CommandOption("--query-count")]
    [Description("Overrides the scenario key QueryCount.")]
    public int? QueryCount { get; init; }
    [CommandOption("--top-k")]
    [Description("Overrides the scenario key K.")]
    public int? K { get; init; }
    [CommandOption("--truth-depth")]
    [Description("Overrides the scenario key TruthDepth.")]
    public int? TruthDepth { get; init; }
    [CommandOption("--recall-threshold")]
    [Description("Overrides the scenario key RecallThreshold.")]
    public double? RecallThreshold { get; init; }
    [CommandOption("--readers")]
    [Description("Overrides the scenario key Readers.")]
    public int? Readers { get; init; }
    [CommandOption("--filter-selectivity")]
    [Description("Overrides the scenario key FilterSelectivity.")]
    public double? FilterSelectivity { get; init; }
    [CommandOption("--insert-rate")]
    [Description("Overrides the scenario key InsertRate.")]
    public double? InsertRate { get; init; }
    [CommandOption("--under-insert-query-rate")]
    [Description("Overrides the scenario key UnderInsertQueryRate.")]
    public double? UnderInsertQueryRate { get; init; }
    [CommandOption("--warmup")]
    [Description("Overrides the scenario key Warmup.")]
    public string? Warmup { get; init; }
    [CommandOption("--duration")]
    [Description("Overrides the scenario key Duration.")]
    public string? Duration { get; init; }
    [CommandOption("--elasticsearch-index-kind")]
    [Description("Overrides the scenario key ElasticsearchIndexKind.")]
    public string? ElasticsearchIndexKind { get; init; }
    [CommandOption("--ravendb-embedding-type")]
    [Description("Overrides the scenario key RavenDbEmbeddingType.")]
    public string? RavenDbEmbeddingType { get; init; }
}

internal static class VectorScenarioResolver
{
    /// <summary>The resolved scenario and every override, by option name and the value it set.</summary>
    public static (VectorScenario Scenario, Dictionary<string, string> Overrides) Resolve(VectorScenario file, VectorSettings s, string[] commandLineArgs)
    {
        var overrides = new Dictionary<string, string>(StringComparer.Ordinal);
        T Pick<T>(string option, T? given, T fileValue) where T : struct
        {
            if (YcsbScenarioResolver.IsExplicit(commandLineArgs, option) == false || given is not { } value)
                return fileValue;
            overrides[option] = Convert.ToString(value, CultureInfo.InvariantCulture)!;
            return value;
        }
        string PickText(string option, string? given, string fileValue)
        {
            if (YcsbScenarioResolver.IsExplicit(commandLineArgs, option) == false || given is null)
                return fileValue;
            overrides[option] = given;
            return given;
        }

        var resolved = file with
        {
            Dataset = PickText("--dataset", s.Dataset, file.Dataset),
            VectorCountCap = s.VectorCountCap is { } cap ? Pick("--vector-count-cap", s.VectorCountCap, cap) : file.VectorCountCap,
            DataDirectory = PickText("--data-dir", s.DataDirectory, file.DataDirectory),
            Seed = Pick("--seed", s.Seed, file.Seed),
            QueryCount = Pick("--query-count", s.QueryCount, file.QueryCount),
            K = Pick("--top-k", s.K, file.K),
            TruthDepth = Pick("--truth-depth", s.TruthDepth, file.TruthDepth),
            RecallThreshold = Pick("--recall-threshold", s.RecallThreshold, file.RecallThreshold),
            Readers = Pick("--readers", s.Readers, file.Readers),
            FilterSelectivity = Pick("--filter-selectivity", s.FilterSelectivity, file.FilterSelectivity),
            InsertRate = Pick("--insert-rate", s.InsertRate, file.InsertRate),
            UnderInsertQueryRate = Pick("--under-insert-query-rate", s.UnderInsertQueryRate, file.UnderInsertQueryRate),
            Warmup = PickText("--warmup", s.Warmup, file.Warmup),
            Duration = PickText("--duration", s.Duration, file.Duration),
            ElasticsearchIndexKind = PickText("--elasticsearch-index-kind", s.ElasticsearchIndexKind, file.ElasticsearchIndexKind),
            RavenDbEmbeddingType = PickText("--ravendb-embedding-type", s.RavenDbEmbeddingType, file.RavenDbEmbeddingType)
        };
        resolved.Validate();
        return (resolved, overrides);
    }
}

/// <summary>Runs load, recall, readers, filtered and under-insert against one target, one result per run.</summary>
public sealed class VectorCommand : AsyncCommand<VectorSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, VectorSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Scenario))
            throw new VectorScenarioException("--scenario is required.");

        var (scenario, overrides) = VectorScenarioResolver.Resolve(VectorScenario.Load(settings.Scenario), settings, Environment.GetCommandLineArgs());
        var results = await new VectorRunner(scenario, overrides, settings).RunAsync();

        foreach (var (run, summary) in results)
        {
            AnsiConsole.MarkupLine($"[bold]{run}[/]: {summary.Steps.Count} step(s), {summary.Verdict}");
            if (settings.OutputPrefix != null)
            {
                var path = $"{settings.OutputPrefix}-{run}.json";
                JsonResultsWriter.Write(path, summary);
                AnsiConsole.MarkupLine($"[dim]  wrote {path}[/]");
            }
        }
        return 0;
    }
}
