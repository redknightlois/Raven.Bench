using System.ComponentModel;
using System.Diagnostics;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Operations.TimeSeries;
using RavenBench.Core;
using RavenBench.Dataset;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class IndexBuildSettings : CommandSettings
{
    [CommandOption("--url")]
    [Description("RavenDB server URL")]
    public string Url { get; init; } = "";

    [CommandOption("--database")]
    [Description("Target database name (derived from --dataset-profile/--dataset-size when a dataset is specified)")]
    public string? Database { get; init; }

    [CommandOption("--index-kind")]
    [Description("Index kind to build and time: map, map-reduce, full-text, fanout, timeseries")]
    public string? IndexKind { get; init; }

    [CommandOption("--dataset")]
    [Description("Dataset to import if missing: stackoverflow")]
    public string? Dataset { get; init; }

    [CommandOption("--dataset-profile")]
    [Description("Dataset size profile: small/half/full")]
    public string? DatasetProfile { get; init; }

    [CommandOption("--dataset-size")]
    [Description("Custom dataset size: 0=full, N=use N post dump files")]
    public int DatasetSize { get; init; } = 0;

    [CommandOption("--dataset-cache-dir")]
    [Description("Directory for caching downloaded dataset files")]
    public string? DatasetCacheDir { get; init; }

    [CommandOption("--search-engine")]
    [Description("Search engine for the index: corax (default), lucene")]
    public string SearchEngine { get; init; } = "corax";

    [CommandOption("--ts-docs")]
    [Description("Documents to seed with time series for --index-kind timeseries (default: 1000)")]
    public int TsDocs { get; init; } = 1000;

    [CommandOption("--ts-entries")]
    [Description("Time-series entries per seeded document for --index-kind timeseries (default: 100)")]
    public int TsEntries { get; init; } = 100;
}

/// <summary>
/// One-shot command that isolates and reports index build cost: ensures the dataset is
/// imported, creates the requested index fresh, times it to non-stale, and leaves the
/// database indexed for reuse by subsequent query runs.
/// </summary>
public sealed class IndexBuildCommand : AsyncCommand<IndexBuildSettings>
{
    private const string SeededTimeSeriesName = "BenchValues";

    public override async Task<int> ExecuteAsync(CommandContext context, IndexBuildSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Url))
        {
            AnsiConsole.MarkupLine("[red]--url is required.[/]");
            return -1;
        }

        StackOverflowIndex index;
        try
        {
            index = ParseIndexKind(settings.IndexKind);
        }
        catch (ArgumentException ex)
        {
            AnsiConsole.MarkupLine($"[red]{ex.Message}[/]");
            return -1;
        }

        var searchEngine = settings.SearchEngine.Trim().ToLowerInvariant() switch
        {
            "lucene" => IndexingEngine.Lucene,
            "corax" => IndexingEngine.Corax,
            _ => throw new ArgumentException($"Invalid search engine: {settings.SearchEngine}. Valid options: corax, lucene")
        };

        string database;
        if (string.IsNullOrEmpty(settings.Dataset) == false)
        {
            var importOptions = new RunOptions
            {
                Url = settings.Url,
                Database = settings.Database ?? "temp-placeholder",
                Dataset = settings.Dataset,
                DatasetProfile = settings.DatasetProfile,
                DatasetSize = settings.DatasetSize,
                DatasetCacheDir = settings.DatasetCacheDir
            };
            database = await DatasetImportCoordinator.ImportDatasetAsync(importOptions);
        }
        else if (string.IsNullOrEmpty(settings.Database) == false)
        {
            database = settings.Database;
        }
        else
        {
            AnsiConsole.MarkupLine("[red]Either --dataset or --database is required.[/]");
            return -1;
        }

        using var store = new DocumentStore
        {
            Urls = [settings.Url],
            Database = database
        };
        store.Initialize();

        // Dataset-shipped or previously created indexes must be done indexing first so the
        // timed build is not competing for indexing resources.
        await DatasetImportCoordinator.WaitForNonStaleIndexesAsync(settings.Url, database, System.Net.HttpVersion.Version11);

        if (index == StackOverflowIndex.UsersTimeseries)
            await SeedTimeSeriesAsync(store, settings.TsDocs, settings.TsEntries);

        var indexName = StackOverflowIndexes.GetName(index, searchEngine);
        var definition = StackOverflowIndexes.GetDefinition(index, searchEngine);

        // A pre-existing index would make the timed build a no-op; recreate it from scratch.
        await store.Maintenance.SendAsync(new DeleteIndexOperation(indexName));

        AnsiConsole.MarkupLine($"[blue]Building index '{indexName}' (engine: {searchEngine})...[/]");

        var sw = Stopwatch.StartNew();
        await store.Maintenance.SendAsync(new PutIndexesOperation(definition));

        var maxWait = TimeSpan.FromHours(2);
        while (sw.Elapsed < maxWait)
        {
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            var info = stats.Indexes.FirstOrDefault(i => i.Name == indexName);

            if (info == null)
            {
                AnsiConsole.MarkupLine($"[red]Index '{indexName}' disappeared during build.[/]");
                return -1;
            }

            if (info.State == Raven.Client.Documents.Indexes.IndexState.Error)
            {
                AnsiConsole.MarkupLine($"[red]Index '{indexName}' entered the Error state during build.[/]");
                return -1;
            }

            if (info.IsStale == false)
                break;

            Console.WriteLine($"[IndexBuild] '{indexName}' still indexing... ({sw.Elapsed.TotalSeconds:F0}s elapsed)");
            await Task.Delay(2000);
        }
        sw.Stop();

        var dbStats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        if (dbStats.Indexes.FirstOrDefault(i => i.Name == indexName)?.IsStale != false)
        {
            AnsiConsole.MarkupLine($"[red]Index '{indexName}' is still stale after {maxWait.TotalMinutes:F0} minutes.[/]");
            return -1;
        }

        var indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(indexName));
        var docCount = dbStats.CountOfDocuments;
        var seconds = sw.Elapsed.TotalSeconds;

        var table = new Table().Border(TableBorder.Rounded).Title("[blue]Index build result[/]");
        table.AddColumn("Index");
        table.AddColumn("Engine");
        table.AddColumn("Build time");
        table.AddColumn("Docs in DB");
        table.AddColumn("Docs/s");
        table.AddColumn("Index entries");
        table.AddRow(
            indexName,
            searchEngine.ToString(),
            $"{seconds:F1}s",
            docCount.ToString("N0"),
            seconds > 0 ? (docCount / seconds).ToString("N0") : "-",
            indexStats.EntriesCount.ToString("N0"));
        AnsiConsole.Write(table);

        return 0;
    }

    private static StackOverflowIndex ParseIndexKind(string? indexKind)
    {
        if (string.IsNullOrWhiteSpace(indexKind))
            throw new ArgumentException("--index-kind is required. Valid options: map, map-reduce, full-text, fanout, timeseries");

        return indexKind.Trim().ToLowerInvariant() switch
        {
            "map" => StackOverflowIndex.UsersByDisplayName,
            "map-reduce" or "mapreduce" => StackOverflowIndex.QuestionsByViewCountGrouped,
            "full-text" or "fulltext" => StackOverflowIndex.QuestionsByTitleSearch,
            "fanout" => StackOverflowIndex.QuestionsAnswers,
            "timeseries" or "time-series" => StackOverflowIndex.UsersTimeseries,
            _ => throw new ArgumentException($"Invalid index kind: {indexKind}. Valid options: map, map-reduce, full-text, fanout, timeseries")
        };
    }

    /// <summary>
    /// Appends deterministic time-series entries to existing Users documents so the
    /// time-series index has data to build over. Re-running overwrites the same timestamps.
    /// </summary>
    private static async Task SeedTimeSeriesAsync(IDocumentStore store, int docCount, int entriesPerDoc)
    {
        Console.WriteLine($"[IndexBuild] Seeding time series on {docCount} Users documents ({entriesPerDoc} entries each)...");

        var ids = new List<string>();
        using (var session = store.OpenAsyncSession())
        {
            var query = session.Advanced.AsyncRawQuery<dynamic>($"from Users limit {docCount}");
            await using var stream = await session.Advanced.StreamAsync(query);
            while (await stream.MoveNextAsync())
                ids.Add(stream.Current.Id);
        }

        if (ids.Count == 0)
            throw new InvalidOperationException("No Users documents found to seed time series on. Import the dataset first.");

        var baseTime = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var options = new ParallelOptions { MaxDegreeOfParallelism = 16 };

        await Parallel.ForEachAsync(ids, options, async (id, ct) =>
        {
            var op = new TimeSeriesOperation { Name = SeededTimeSeriesName };
            for (int j = 0; j < entriesPerDoc; j++)
            {
                op.Append(new TimeSeriesOperation.AppendOperation
                {
                    Timestamp = baseTime.AddMinutes(j),
                    Values = new[] { (double)j },
                    Tag = "bench"
                });
            }
            await store.Operations.SendAsync(new TimeSeriesBatchOperation(id, op), token: ct);
        });

        Console.WriteLine($"[IndexBuild] Time-series seeding complete ({ids.Count} documents).");
    }
}
