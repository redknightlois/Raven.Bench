using System.ComponentModel;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RecallSettings : CommandSettings
{
    [CommandOption("--url")]
    [Description("RavenDB server URL")]
    public string Url { get; init; } = "";

    [CommandOption("--dataset")]
    [Description("Dataset: sphere, or a published set (glove-100-angular, dbpedia-openai-1000k-angular, cohere-768-100k, cohere-768-1m)")]
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

    [CommandOption("--seed")]
    [Description("Seed that draws the query vectors held out of the load (default: 42, the run default)")]
    public int Seed { get; init; } = 42;

    [CommandOption("--dataset-cache-dir")]
    [Description("Data directory holding the pinned set files (default: ./datasets)")]
    public string? DatasetCacheDir { get; init; }

    [CommandOption("--node-exporter-url")]
    [Description("node_exporter metrics endpoint on the database host (e.g. http://dbhost:9100/metrics). Fills host-wide server CPU and memory per effort row.")]
    public string? NodeExporterUrl { get; init; }

    [CommandOption("--index-name")]
    [Description("Override the index name to query (default: derived from collection/quantization/engine)")]
    public string? IndexNameOverride { get; init; }
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

        var recallKs = CliParsing.ParseRecallKsRaw(settings.VectorRecallKs ?? "1,5,10");
        if (recallKs.Length == 0)
        {
            AnsiConsole.MarkupLine("[red]Invalid --vector-recall-ks.[/]");
            return -1;
        }

        int[]? efSweep = null;
        if (string.IsNullOrWhiteSpace(settings.VectorRecallEfSweep) == false)
            efSweep = CliParsing.ParseEfSweepRaw(settings.VectorRecallEfSweep);

        if (await LoadVectorMetadataAsync(settings) is not var (metadata, files, selection))
        {
            AnsiConsole.MarkupLine("[red]Failed to load vector metadata.[/]");
            return -1;
        }

        using (var store = HttpHelper.Create(settings.Url, GetDatabaseName(settings), httpVersion: null))
            await HeldOutManifest.EnsureLoadedAsync(store, files, selection);

        AnsiConsole.MarkupLine($"[blue]Measuring recall on {Markup.Escape(metadata.IndexName ?? throw new InvalidOperationException("The vector metadata names no index."))} ({metadata.QueryVectorCount} queries)[/]");

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
                settings.SearchEngine,
                nodeExporterUrl: CliParsing.ParseNodeExporterUrl(settings.NodeExporterUrl));

            var table = new Table().Border(TableBorder.Rounded).Title("[blue]Recall@K by efSearch[/]");
            table.AddColumn("efSearch");
            var ks = sweep.Values.First().RecallAtK.Keys.OrderBy(k => k).ToList();
            foreach (var k in ks)
                table.AddColumn($"recall@{k}");
            table.AddColumn("time");
            table.AddColumn("server CPU");

            foreach (var (ef, result) in sweep.OrderBy(kvp => kvp.Key))
            {
                var row = new List<string> { ef.ToString() };
                foreach (var k in ks)
                    row.Add(result.RecallAtK.TryGetValue(k, out var v) ? $"{v:P2}" : "-");
                row.Add($"{result.MeasurementTime.TotalSeconds:F1}s");
                row.Add(result.ServerCpu is { } cpu ? $"{cpu:F1}% host-wide" : "-");
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
                settings.SearchEngine,
                nodeExporterUrl: CliParsing.ParseNodeExporterUrl(settings.NodeExporterUrl));

            var lines = result.RecallAtK
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => $"recall@{kvp.Key} = {kvp.Value:P2}");
            AnsiConsole.MarkupLine(string.Join(" | ", lines));
        }

        return 0;
    }

    private static string GetDatabaseName(RecallSettings settings)
    {
        if (VectorSets.FindPublished(settings.Dataset!) is { } published)
            return PublishedSetImport.DatabaseName(published);
        if (settings.Dataset?.StartsWith("sphere", StringComparison.OrdinalIgnoreCase) == true)
        {
            var profile = settings.DatasetProfile ?? "100k";
            var provider = new SphereDatasetProvider(profile);
            return provider.GetDatabaseName(profile);
        }
        return "RavenBench";
    }

    /// <summary>The query metadata, with the files and the held-out selection the database must have been loaded under.</summary>
    private static async Task<(VectorWorkloadMetadata Metadata, VerifiedFiles Files, QuerySelection? Selection)?> LoadVectorMetadataAsync(RecallSettings settings)
    {
        var engineSuffix = VectorIndexMapping.GetEngineSuffix(settings.SearchEngine);
        var dataDirectory = settings.DatasetCacheDir ?? Path.Combine(Directory.GetCurrentDirectory(), "datasets");
        var selection = new QuerySelection(settings.Seed, DatasetImportCoordinator.VectorQueryCount);
        var depth = CliParsing.ParseRecallKsRaw(settings.VectorRecallKs ?? "1,5,10").Max();

        if (VectorSets.FindPublished(settings.Dataset!) is { } published)
        {
            UnsupportedVectorMetricException.ThrowIfUnsupported(RawHttpTransport.RavenDbProductName, RavenDbVectorMetrics.Supported, published.Metric);
            var files = await PinnedFiles.EnsureAsync(published, dataDirectory);
            var metadata = await PublishedSetImport.MetadataAsync(published, files, selection, depth, settings.VectorQuantization, settings.SearchEngine, null, null);
            if (string.IsNullOrWhiteSpace(settings.IndexNameOverride) == false)
                metadata.IndexName = settings.IndexNameOverride;
            return (metadata, files, null);
        }

        if (settings.Dataset?.StartsWith("sphere", StringComparison.OrdinalIgnoreCase) == true)
        {
            var profile = settings.DatasetProfile ?? "100k";
            var provider = new SphereDatasetProvider(profile);
            UnsupportedVectorMetricException.ThrowIfUnsupported(RawHttpTransport.RavenDbProductName, RavenDbVectorMetrics.Supported, provider.Metric);
            var files = await PinnedFiles.EnsureAsync(provider, dataDirectory);
            var metadata = await provider.GenerateQueryVectorsAsync(files, selection, depth);
            metadata.IndexName = string.IsNullOrWhiteSpace(settings.IndexNameOverride) == false
                ? settings.IndexNameOverride
                : VectorIndexNaming.GetIndexName(SphereDatasetProvider.CollectionName, settings.VectorQuantization, engineSuffix);
            metadata.CollectionName = SphereDatasetProvider.CollectionName;
            metadata.IndexedFieldName = "Vector";
            metadata.EnsureIndexExists = async (storeObj, indexName) =>
            {
                var s = (Raven.Client.Documents.IDocumentStore)storeObj;
                await SphereDatasetProvider.CreateVectorIndexAsync(
                    s, settings.VectorQuantization, false, settings.SearchEngine);
            };
            return (metadata, files, selection);
        }

        return null;
    }
}
