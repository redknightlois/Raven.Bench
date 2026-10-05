using System.ComponentModel;
using System.Globalization;
using RavenBench.Aggregate;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Diagnostics;
using RavenBench.Reporting;
using RavenBench.Ycsb;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

/// <summary>
/// The aggregate command's options. Every option that overrides a scenario key is nullable, so its
/// presence alone says it was given.
/// </summary>
public sealed class AggregateSettings : CommandSettings
{
    [CommandOption("--scenario")]
    [Description("Path to the aggregate scenario file (JSON).")]
    public string? Scenario { get; init; }

    [CommandOption("--target")]
    [Description("ravendb, ravendb-7, mongodb or mongodb-indexed.")]
    public string? Target { get; init; }

    [CommandOption("--url")]
    [Description("The target endpoint: a RavenDB URL or a MongoDB connection string.")]
    public string? Url { get; init; }

    [CommandOption("--database")]
    [Description("A fresh database the run loads into.")]
    public string? Database { get; init; }

    [CommandOption("--output-prefix")]
    [Description("Result files are written as <prefix>-<run>.json, histograms beside them.")]
    public string? OutputPrefix { get; init; }

    [CommandOption("--node-exporter-url")]
    [Description("node_exporter metrics endpoint on the database host (e.g. http://dbhost:9100/metrics).")]
    public string? NodeExporterUrl { get; init; }

    [CommandOption("--keep-data")]
    [Description("Leave the loaded data behind instead of removing it.")]
    public bool KeepData { get; init; }

    [CommandOption("--seed")] [Description("Overrides the scenario key seed.")] public int? Seed { get; init; }
    [CommandOption("--documents")] [Description("Overrides the scenario key documentCount.")] public long? DocumentCount { get; init; }
    [CommandOption("--document-size")] [Description("Overrides the scenario key documentSize (bytes).")] public int? DocumentSize { get; init; }
    [CommandOption("--categories")] [Description("Overrides the scenario key categoryCardinality.")] public int? CategoryCardinality { get; init; }
    [CommandOption("--regions")] [Description("Overrides the scenario key regionCardinality.")] public int? RegionCardinality { get; init; }
    [CommandOption("--distribution")] [Description("Overrides the scenario key distribution.")] public string? Distribution { get; init; }
    [CommandOption("--distribution-exponent")] [Description("Overrides the scenario key distributionExponent.")] public double? DistributionExponent { get; init; }
    [CommandOption("--count-top-n")] [Description("Overrides the scenario key countTopN.")] public int? CountTopN { get; init; }
    [CommandOption("--region-top-n")] [Description("Overrides the scenario key regionTopN.")] public int? RegionTopN { get; init; }
    [CommandOption("--filter-selectivity")] [Description("Overrides the scenario key filterSelectivity.")] public double? FilterSelectivity { get; init; }
    [CommandOption("--concurrency")] [Description("Overrides the scenario key concurrency.")] public int? Concurrency { get; init; }
    [CommandOption("--write-rate")] [Description("Overrides the scenario key writeRate.")] public double? WriteRate { get; init; }
    [CommandOption("--writers")] [Description("Overrides the scenario key writers.")] public int? Writers { get; init; }
    [CommandOption("--under-write-query-rate")] [Description("Overrides the scenario key underWriteQueryRate.")] public double? UnderWriteQueryRate { get; init; }
    [CommandOption("--warmup")] [Description("Overrides the scenario key warmup.")] public string? Warmup { get; init; }
    [CommandOption("--duration")] [Description("Overrides the scenario key duration.")] public string? Duration { get; init; }
    [CommandOption("--non-stale-timeout")] [Description("Overrides the scenario key nonStaleTimeout.")] public string? NonStaleTimeout { get; init; }
    [CommandOption("--data-dir")] [Description("Overrides the scenario key dataDirectory.")] public string? DataDirectory { get; init; }
}

internal static class AggregateScenarioResolver
{
    /// <summary>The resolved, validated scenario and every override, by option name and the value it set.</summary>
    public static (AggregateScenario Scenario, Dictionary<string, string> Overrides) Resolve(AggregateScenario file, AggregateSettings s, string[] commandLineArgs)
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
            Seed = Pick("--seed", s.Seed, file.Seed),
            DocumentCount = Pick("--documents", s.DocumentCount, file.DocumentCount),
            DocumentSize = Pick("--document-size", s.DocumentSize, file.DocumentSize),
            CategoryCardinality = Pick("--categories", s.CategoryCardinality, file.CategoryCardinality),
            RegionCardinality = Pick("--regions", s.RegionCardinality, file.RegionCardinality),
            Distribution = PickText("--distribution", s.Distribution, file.Distribution),
            DistributionExponent = s.DistributionExponent is { } e ? Pick("--distribution-exponent", s.DistributionExponent, e) : file.DistributionExponent,
            CountTopN = Pick("--count-top-n", s.CountTopN, file.CountTopN),
            RegionTopN = Pick("--region-top-n", s.RegionTopN, file.RegionTopN),
            FilterSelectivity = Pick("--filter-selectivity", s.FilterSelectivity, file.FilterSelectivity),
            Concurrency = Pick("--concurrency", s.Concurrency, file.Concurrency),
            WriteRate = Pick("--write-rate", s.WriteRate, file.WriteRate),
            Writers = Pick("--writers", s.Writers, file.Writers),
            UnderWriteQueryRate = Pick("--under-write-query-rate", s.UnderWriteQueryRate, file.UnderWriteQueryRate),
            Warmup = PickText("--warmup", s.Warmup, file.Warmup),
            Duration = PickText("--duration", s.Duration, file.Duration),
            NonStaleTimeout = PickText("--non-stale-timeout", s.NonStaleTimeout, file.NonStaleTimeout),
            DataDirectory = PickText("--data-dir", s.DataDirectory, file.DataDirectory)
        };
        resolved.Validate();
        foreach (var (name, value) in new[] { ("warmup", resolved.Warmup), ("duration", resolved.Duration), ("nonStaleTimeout", resolved.NonStaleTimeout) })
        {
            try
            {
                CliParsing.ParseDuration(value);
            }
            catch (FormatException ex)
            {
                throw new AggregateScenarioException(name, $"'{value}' is not a duration: {ex.Message}");
            }
        }
        resolved.ResolveDataDirectory(RepositoryRootLocator.Find());
        return (resolved, overrides);
    }
}

/// <summary>Runs build, count-by-category, sum-by-region, filtered-group and under-write against one target, one result per run.</summary>
public sealed class AggregateCommand : AsyncCommand<AggregateSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, AggregateSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Scenario))
            throw new ArgumentException("--scenario is required.", "--scenario");

        var (scenario, overrides) = AggregateScenarioResolver.Resolve(AggregateScenario.Load(settings.Scenario), settings, Environment.GetCommandLineArgs());
        var results = await new AggregateRunner(scenario, overrides, settings).RunAsync();

        foreach (var (run, summary) in results)
        {
            AnsiConsole.MarkupLine($"[bold]{Markup.Escape(run)}[/]: {summary.Steps.Count} step(s), {Markup.Escape(summary.Verdict)}");
            if (settings.OutputPrefix != null)
            {
                var path = $"{settings.OutputPrefix}-{run}.json";
                JsonResultsWriter.Write(path, summary);
                AnsiConsole.MarkupLine($"[dim]  wrote {Markup.Escape(path)}[/]");
            }
        }
        return 0;
    }
}
