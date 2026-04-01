using System.ComponentModel;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RecallSettings : CommandSettings
{
    [CommandOption("--url")]
    [Description("RavenDB server URL")]
    public string Url { get; init; } = "";

    [CommandOption("--dataset")]
    [Description("Dataset: sphere")]
    public string? Dataset { get; init; }

    [CommandOption("--dataset-profile")]
    [Description("Dataset size profile (sphere): 100k, 1m, etc.")]
    public string? DatasetProfile { get; init; }

    [CommandOption("--vector-quantization")]
    [Description("Vector quantization: none, int8, int4, int3, int2, binary")]
    public VectorQuantization VectorQuantization { get; init; } = VectorQuantization.None;

    [CommandOption("--vector-recall-ks")]
    [Description("Comma-separated K values for recall@K (default: 1,5,10)")]
    public string? VectorRecallKs { get; init; } = "1,5,10";

    [CommandOption("--vector-recall-ef-sweep")]
    [Description("Comma-separated efSearch values to sweep (e.g., 64,128,256,512)")]
    public string? VectorRecallEfSweep { get; init; }

    [CommandOption("--engine")]
    [Description("Search engine: corax or lucene (default: corax)")]
    public IndexingEngine SearchEngine { get; init; } = IndexingEngine.Corax;
}

public sealed class RecallCommand : AsyncCommand<RecallSettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, RecallSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Url))
        {
            AnsiConsole.MarkupLine("[red]--url is required.[/]");
            return -1;
        }

        if (string.IsNullOrWhiteSpace(settings.Dataset))
        {
            AnsiConsole.MarkupLine("[red]--dataset is required (e.g., sphere).[/]");
            return -1;
        }

        // Parse recall Ks
        var recallKs = CliParsing.ParseRecallKsRaw(settings.VectorRecallKs ?? "1,5,10");
        if (recallKs.Length == 0)
        {
            AnsiConsole.MarkupLine("[red]Invalid --vector-recall-ks.[/]");
            return -1;
        }

        int[]? efSweep = null;
        if (!string.IsNullOrWhiteSpace(settings.VectorRecallEfSweep))
            efSweep = CliParsing.ParseEfSweepRaw(settings.VectorRecallEfSweep);

        // Load vector metadata (query vectors)
        var metadata = await LoadVectorMetadataAsync(settings);
        if (metadata == null)
        {
            AnsiConsole.MarkupLine("[red]Failed to load vector metadata.[/]");
            return -1;
        }

        AnsiConsole.MarkupLine($"[blue]Measuring recall on {metadata.IndexName} ({metadata.QueryVectorCount} queries)[/]");

        var recall = new RecallMeasurement();

        if (efSweep is { Length: > 0 })
        {
            var sweep = await recall.MeasureSweepAsync(
                settings.Url,
                GetDatabaseName(settings),
                metadata,
                recallKs,
                efSweep,
                settings.VectorQuantization,
                settings.SearchEngine);

            // Render sweep table
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
        else
        {
            var result = await recall.MeasureAsync(
                settings.Url,
                GetDatabaseName(settings),
                metadata,
                recallKs,
                settings.VectorQuantization,
                settings.SearchEngine);

            var lines = result.RecallAtK
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => $"recall@{kvp.Key} = {kvp.Value:P2}");
            AnsiConsole.MarkupLine(string.Join(" | ", lines));
        }

        return 0;
    }

    private static string GetDatabaseName(RecallSettings settings)
    {
        if (settings.Dataset?.StartsWith("sphere", StringComparison.OrdinalIgnoreCase) == true)
        {
            var profile = settings.DatasetProfile ?? "100k";
            var provider = new SphereDatasetProvider(profile);
            return provider.GetDatabaseName(profile);
        }
        return "RavenBench";
    }

    private static async Task<VectorWorkloadMetadata?> LoadVectorMetadataAsync(RecallSettings settings)
    {
        var engineSuffix = settings.SearchEngine == IndexingEngine.Lucene ? "-lucene" : "-corax";

        if (settings.Dataset?.StartsWith("sphere", StringComparison.OrdinalIgnoreCase) == true)
        {
            var profile = settings.DatasetProfile ?? "100k";
            var provider = new SphereDatasetProvider(profile);
            var dbName = provider.GetDatabaseName(profile);
            var metadata = await provider.GenerateQueryVectorsAsync(settings.Url, dbName, count: 1000);
            metadata.IndexName = VectorIndexNaming.GetIndexName(SphereDatasetProvider.CollectionName, settings.VectorQuantization, engineSuffix);
            metadata.CollectionName = SphereDatasetProvider.CollectionName;
            metadata.IndexedFieldName = "Vector";
            return metadata;
        }

        return null;
    }
}
