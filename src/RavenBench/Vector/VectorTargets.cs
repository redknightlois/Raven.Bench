using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Vector;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;

namespace RavenBench.VectorBench;

/// <summary>One vector to load or insert: the set's base id, the vector and its filter label.</summary>
public sealed record LabelledVector(string Id, float[] Vector, string Label);

/// <summary>
/// One product under the vector benchmark. The typed vector search operation runs through
/// <see cref="Transport"/>; this surface carries only what the operation does not: the load, the
/// index build, the product's own knob, and the settings the product reports.
/// </summary>
public interface IVectorTarget : IDisposable
{
    IYcsbTransport Transport { get; }

    /// <summary>The key of the scenario's effort settings for this product.</summary>
    string EffortFamily { get; }

    string FieldName { get; }
    string FilterField { get; }
    string? ExpectedIndex { get; }

    /// <summary>The vector storage the row runs: full float32, or a named quantized mode. It is unique per configuration of one product.</summary>
    string VectorStorage { get; }

    /// <summary>The prefix the product puts before a set's base id.</summary>
    string IdPrefix { get; }

    /// <summary>What an acknowledged insert guarantees about its visibility to a later search, stated for the under-insert truth.</summary>
    string InsertVisibility { get; }

    DurabilityParity Durability { get; }

    SearchEffort Effort(double value);

    /// <summary>Loads the vectors into an empty store and returns when the index answers queries.</summary>
    Task LoadAsync(IAsyncEnumerable<LabelledVector> vectors, CancellationToken ct);

    OperationBase InsertOperation(LabelledVector vector);

    /// <summary>The used size of the whole store and of the vector index alone, as the product reports them; allocated but free space is not counted.</summary>
    Task<(OnDiskSize Total, OnDiskSize Index)> StoredSizesAsync();

    /// <summary>The index definition and server settings in force, as the product reports them.</summary>
    Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct);

    /// <summary>Removes what the run created.</summary>
    Task CleanupAsync();
}

/// <summary>
/// RavenDB over the raw HTTP transport, the published path. A search returns ids only, the same payload pgvector returns. The vectors are stored as float32 and the
/// index holds them in the destination embedding type the scenario names, built with the scenario's HNSW parameters, or the server defaults without them.
/// </summary>
public sealed class RavenDbVectorTarget : IVectorTarget
{
    public const string CollectionName = PublishedSetImport.CollectionName;

    private static readonly IReadOnlyDictionary<string, (VectorEmbeddingType Type, string IndexName, string Storage)> EmbeddingTypes =
        new Dictionary<string, (VectorEmbeddingType, string, string)>(StringComparer.Ordinal)
        {
            ["Single"] = (VectorEmbeddingType.Single, "VectorBench/Float32", "float32, unquantized"),
            ["Int8"] = (VectorEmbeddingType.Int8, "VectorBench/Int8", "int8, quantized"),
            ["Binary"] = (VectorEmbeddingType.Binary, "VectorBench/Binary", "binary 1-bit, quantized")
        };

    private readonly string _url;
    private readonly string _database;
    private readonly RawHttpTransport _transport;
    private readonly VectorEmbeddingType _destination;
    private readonly string _indexName;
    private readonly HnswBuild? _build;

    /// <param name="embeddingType">The destination embedding type: Single, Int8 or Binary; any other value is refused by name.</param>
    public RavenDbVectorTarget(string url, string database, VectorMetric metric, string embeddingType, HnswBuild? build = null)
    {
        _build = build;
        UnsupportedVectorMetricException.ThrowIfUnsupported(RawHttpTransport.RavenDbProductName, RavenDbVectorMetrics.Supported, metric);
        if (EmbeddingTypes.TryGetValue(embeddingType, out var type) == false)
            throw new ArgumentException($"RavenDB destination embedding type '{embeddingType}' is not one of {string.Join(", ", EmbeddingTypes.Keys)}.", nameof(embeddingType));
        (_destination, _indexName, VectorStorage) = type;
        _url = url;
        _database = database;
        _transport = new RawHttpTransport(url, database, CompressionMode.Identity, HttpVersion.Version11) { VectorSearchIdsOnly = true };
    }

    public string VectorStorage { get; }

    public IYcsbTransport Transport => _transport;
    public string EffortFamily => "ravendb";
    public string FieldName => "Vector";
    public string FilterField => "Label";
    public string? ExpectedIndex => _indexName;
    public string IdPrefix => PublishedSetImport.DocumentIdPrefix;
    public string InsertVisibility => "approximate: RavenDB acknowledges a write before its vector index contains it, and the queries do not wait for indexing, so an acknowledged insert still being indexed counts in the truth and scores as a miss";
    public DurabilityParity Durability { get; } = new() { Setting = "durability", Value = "ravendb-default" };

    public SearchEffort Effort(double value) => SearchEffort.RavenDb(value);

    public async Task LoadAsync(IAsyncEnumerable<LabelledVector> vectors, CancellationToken ct)
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        if (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(_database), ct) != null)
            throw new InvalidOperationException($"Database '{_database}' already exists; the vector run loads into a fresh database.");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(_database)), ct);

        await using (var bulk = store.BulkInsert(token: ct))
        {
            await foreach (var v in vectors.WithCancellation(ct))
                await bulk.StoreAsync(new { Embedding = v.Vector, v.Label }, IdPrefix + v.Id,
                    new Raven.Client.Json.MetadataAsDictionary { ["@collection"] = CollectionName });
        }

        await VectorIndexHelper.CreateAndWaitForIndexAsync(store, new IndexDefinition
        {
            Name = _indexName,
            Maps = { $"from v in docs.{CollectionName} select new {{ Vector = CreateVector(v.Embedding), v.Label }}" },
            Fields =
            {
                ["Vector"] = new IndexFieldOptions
                {
                    Vector = new VectorOptions
                    {
                        SourceEmbeddingType = VectorEmbeddingType.Single,
                        DestinationEmbeddingType = _destination,
                        NumberOfEdges = _build?.M,
                        NumberOfCandidatesForIndexing = _build?.EfConstruction
                    }
                }
            }
        }, "[Vector]");
    }

    public OperationBase InsertOperation(LabelledVector vector)
    {
        var json = new StringBuilder("{\"Embedding\":[");
        json.AppendJoin(',', vector.Vector.Select(f => f.ToString("R", CultureInfo.InvariantCulture)));
        json.Append("],\"Label\":\"").Append(vector.Label).Append("\",\"@metadata\":{\"@collection\":\"").Append(CollectionName).Append("\"}}");
        return new InsertOperation<string> { Id = IdPrefix + vector.Id, Payload = json.ToString() };
    }

    public async Task<(OnDiskSize Total, OnDiskSize Index)> StoredSizesAsync()
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        using var response = await store.GetRequestExecutor().HttpClient.GetAsync($"{_url.TrimEnd('/')}/databases/{Uri.EscapeDataString(_database)}/debug/storage/report");
        response.EnsureSuccessStatusCode();
        using var report = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        long total = 0;
        long? index = null;
        foreach (var environment in report.RootElement.GetProperty("Results").EnumerateArray())
        {
            var used = environment.GetProperty("Report").GetProperty("DataFile").GetProperty("UsedSpaceInBytes").GetInt64();
            total += used;
            if (environment.GetProperty("Name").GetString() == _indexName)
                index = used;
        }
        if (index is null)
            throw new InvalidOperationException($"The storage report of database '{_database}' has no environment for index '{_indexName}'.");
        return (OnDiskSize.Reported("debug/storage/report DataFile.UsedSpaceInBytes, every environment", total),
                OnDiskSize.Reported("debug/storage/report DataFile.UsedSpaceInBytes, the vector index environment", index.Value));
    }

    public async Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct)
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        var index = await store.Maintenance.SendAsync(new GetIndexOperation(_indexName), ct)
                    ?? throw new InvalidOperationException($"Index '{_indexName}' does not exist; load before reading the settings.");
        var vector = index.Fields["Vector"].Vector;
        var settings = new Dictionary<string, string>
        {
            ["index"] = _indexName,
            ["index.map"] = index.Maps.Single(),
            ["index.SearchEngineType"] = index.Configuration.TryGetValue("Indexing.Static.SearchEngineType", out var engine) ? engine : "server-default",
            ["vector.DestinationEmbeddingType"] = vector.DestinationEmbeddingType.ToString()
        };
        JsonElement? configuration = null;
        string? unreadable = null;
        try
        {
            using var response = await store.GetRequestExecutor().HttpClient.GetAsync($"{_url.TrimEnd('/')}/databases/{Uri.EscapeDataString(_database)}/admin/configuration/settings", ct);
            response.EnsureSuccessStatusCode();
            using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
            configuration = document.RootElement.Clone();
        }
        catch (Exception ex) when (ex is HttpRequestException or JsonException)
        {
            unreadable = ex.Message;
        }
        void Record(string name, int? setByBenchmark, string key)
        {
            var reason = unreadable;
            if (setByBenchmark is { } value)
                BuildSetting.Record(settings, name, value, true, key);
            else if (configuration is { } root && RavenDbConfiguration.TryEffective(root, key, out var effective, out reason))
                BuildSetting.Record(settings, name, effective, false, $"the server's {key}");
            else
                BuildSetting.Unreadable(settings, name, reason!);
        }
        Record("vector.NumberOfEdges", vector.NumberOfEdges, "Indexing.Corax.VectorSearch.DefaultNumberOfEdges");
        Record("vector.NumberOfCandidatesForIndexing", vector.NumberOfCandidatesForIndexing, "Indexing.Corax.VectorSearch.DefaultNumberOfCandidatesForIndexing");
        // Applies only to a search that passes no numberOfCandidates; every scenario search passes its effort.
        Record("vector.NumberOfCandidatesForQuerying", null, "Indexing.Corax.VectorSearch.DefaultNumberOfCandidatesForQuerying");
        return settings;
    }

    public async Task CleanupAsync()
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(_database, hardDelete: true));
    }

    public void Dispose() => _transport.Dispose();
}

/// <summary>
/// Records an index build value in force as two settings: the value under its name and, under the name
/// plus <c>.source</c>, whether the benchmark set it or the product default applies.
/// </summary>
public static class BuildSetting
{
    public const string SetByBenchmark = "set by benchmark";

    public static void Record(IDictionary<string, string> settings, string name, int value, bool setByBenchmark, string defaultSource)
    {
        settings[name] = value.ToString(CultureInfo.InvariantCulture);
        settings[name + ".source"] = setByBenchmark ? SetByBenchmark : $"default: {defaultSource}, not set by the benchmark";
    }

    public static void Unreadable(IDictionary<string, string> settings, string name, string reason)
    {
        settings[name] = "unreadable";
        settings[name + ".source"] = $"unreadable: {reason}";
    }
}

/// <summary>Reads the value RavenDB applies for one setting from a database's configuration settings response.</summary>
public static class RavenDbConfiguration
{
    /// <summary>
    /// The database value, else the server value, else the built-in default, as the server reports them.
    /// Returns false with the reason when the response does not carry the key or its value is not an integer.
    /// </summary>
    public static bool TryEffective(JsonElement root, string key, out int value, out string? reason)
    {
        value = 0;
        reason = null;
        if (root.TryGetProperty("Settings", out var settings) == false || settings.ValueKind != JsonValueKind.Array)
        {
            reason = "the configuration response carries no Settings array";
            return false;
        }
        foreach (var setting in settings.EnumerateArray())
        {
            if (setting.TryGetProperty("Metadata", out var metadata) == false || metadata.TryGetProperty("Keys", out var keys) == false
                || keys.EnumerateArray().Any(k => k.GetString() == key) == false)
                continue;
            var text = Configured(setting, "DatabaseValues", key) ?? Configured(setting, "ServerValues", key)
                       ?? (metadata.TryGetProperty("DefaultValue", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString() : null);
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                return true;
            reason = $"the server reports '{text}' for {key}, not an integer";
            return false;
        }
        reason = $"the configuration response does not list {key}";
        return false;
    }

    private static string? Configured(JsonElement setting, string scope, string key) =>
        setting.TryGetProperty(scope, out var values) && values.ValueKind == JsonValueKind.Object
        && values.TryGetProperty(key, out var entry) && entry.TryGetProperty("HasValue", out var has) && has.ValueKind == JsonValueKind.True
        && entry.TryGetProperty("Value", out var v) && v.ValueKind == JsonValueKind.String
            ? v.GetString()
            : null;
}

/// <summary>pgvector over the Apex.PgClient transport: binary COPY load, HNSW with the scenario's build, or the vendor defaults without one.</summary>
public sealed class PgVectorTarget(PgVectorTransport transport, HnswBuild? build = null) : IVectorTarget
{
    public IYcsbTransport Transport => transport;
    public string EffortFamily => PgVectorTransport.Target;
    public string FieldName => "embedding";
    public string FilterField => "label";
    public string? ExpectedIndex => PgVectorTransport.IndexName;
    public string VectorStorage => "float32, unquantized";
    public string IdPrefix => "";
    public string InsertVisibility => "exact: PostgreSQL updates the HNSW index inside the inserting transaction, so an insert acknowledged by commit is visible to every query issued after it";
    public DurabilityParity Durability { get; } = new() { Setting = PostgresYcsbTransport.DurabilitySetting, Value = PostgresYcsbTransport.DurabilityValue };

    // The COPY batch bounds client memory; it does not change what the server stores.
    private const int CopyBatch = 10_000;

    public SearchEffort Effort(double value) => SearchEffort.PgVector(value);

    public async Task LoadAsync(IAsyncEnumerable<LabelledVector> vectors, CancellationToken ct)
    {
        await transport.EnsureDatabaseExistsAsync(transport.DatabaseName);
        var existing = await transport.GetDocumentCountAsync("");
        if (existing != 0)
            throw new InvalidOperationException($"Table '{PgVectorTransport.TableName}' already holds {existing} rows; the vector run loads into a fresh database.");

        var batch = new List<DocumentToWrite<VectorRow>>(CopyBatch);
        async Task FlushAsync()
        {
            if (batch.Count == 0)
                return;
            var result = await transport.ExecuteAsync(new BulkInsertOperation<VectorRow> { Documents = batch }, ct);
            if (result.IsSuccess == false)
                throw new InvalidOperationException($"The pgvector load failed: {result.ErrorDetails}");
            batch = new List<DocumentToWrite<VectorRow>>(CopyBatch);
        }

        await foreach (var v in vectors.WithCancellation(ct))
        {
            batch.Add(new DocumentToWrite<VectorRow> { Id = v.Id, Document = new VectorRow(v.Vector, v.Label) });
            if (batch.Count == CopyBatch)
                await FlushAsync();
        }
        await FlushAsync();
        await transport.BuildIndexAsync(build, ct);
    }

    public OperationBase InsertOperation(LabelledVector vector) =>
        new InsertOperation<VectorRow> { Id = vector.Id, Payload = new VectorRow(vector.Vector, vector.Label) };

    public async Task<(OnDiskSize Total, OnDiskSize Index)> StoredSizesAsync() =>
        (OnDiskSize.Reported(transport.StorageSizeMetricName, await transport.GetStorageSizeBytesAsync()),
         OnDiskSize.Reported($"pg_relation_size of {PgVectorTransport.IndexName}", await transport.GetIndexSizeBytesAsync()));

    public async Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct)
    {
        var reported = await transport.ReadServerSettingsAsync(ct);
        var settings = new Dictionary<string, string>
        {
            ["extversion"] = reported.ExtensionVersion,
            ["indexdef"] = reported.IndexDefinition,
            ["reloptions"] = reported.IndexOptions.Count == 0 ? "none (vendor defaults)" : string.Join(',', reported.IndexOptions)
        };
        foreach (var (name, value) in reported.ServerSettings)
            settings[name] = value;
        try
        {
            var (m, efConstruction) = await transport.ReadHnswBuildParametersAsync(ct);
            BuildSetting.Record(settings, "hnsw.m", m, IsSet(reported.IndexOptions, "m"), "the pgvector default");
            BuildSetting.Record(settings, "hnsw.ef_construction", efConstruction, IsSet(reported.IndexOptions, "ef_construction"), "the pgvector default");
        }
        catch (Exception ex) when (ex is Apex.PgClient.PgException or InvalidDataException)
        {
            BuildSetting.Unreadable(settings, "hnsw.m", ex.Message);
            BuildSetting.Unreadable(settings, "hnsw.ef_construction", ex.Message);
        }
        return settings;
    }

    private static bool IsSet(IReadOnlyList<string> reloptions, string option) => reloptions.Any(o => o.StartsWith(option + "=", StringComparison.Ordinal));

    /// <summary>Replaces the HNSW index the runs measured with another index kind; the cross-check runs it last.</summary>
    public Task<string> ReplaceIndexAsync(string kind, IReadOnlyDictionary<string, int> options, IReadOnlyDictionary<string, string> session, CancellationToken ct) => transport.ReplaceIndexAsync(kind, options, session, ct);

    public Task<HashSet<string>> ReadStoredIdsAsync(CancellationToken ct) => transport.ReadStoredIdsAsync(ct);

    public Task CleanupAsync() => transport.DropTableAsync();

    public void Dispose() => transport.Dispose();
}

/// <summary>
/// Elasticsearch over its REST API on raw HTTP: a fresh index loaded through <c>_bulk</c>, refreshed,
/// with the index kind and, for a graph kind, the HNSW build the scenario names; every other index option at the vendor default. Round one
/// does not force-merge.
/// </summary>
public sealed class ElasticsearchVectorTarget(ElasticsearchVectorTransport transport) : IVectorTarget
{
    // The bulk batch bounds the request size; it does not change what the server stores.
    private const int BulkBatch = 1_000;

    private const string VendorDefault = "the Elasticsearch default";

    private string? _defaultKind;
    private string? _refreshInterval;

    public IYcsbTransport Transport => transport;
    public string EffortFamily => $"{ElasticsearchVectorTransport.Target}-{transport.Kind.Name}";
    public string FieldName => ElasticsearchVectorTransport.VectorField;
    public string FilterField => ElasticsearchVectorTransport.LabelField;
    public string? ExpectedIndex => null;
    public string VectorStorage => transport.Kind.Storage;
    public string IdPrefix => "";
    public DurabilityParity Durability { get; } = new() { Setting = ElasticsearchVectorTransport.DurabilitySetting, Value = ElasticsearchVectorTransport.DurabilityValue };

    public string InsertVisibility =>
        $"approximate: Elasticsearch makes an acknowledged write searchable only after the next refresh, and index.refresh_interval is {RequireLoaded(_refreshInterval)} as the server reports it, " +
        "so an acknowledged insert not yet refreshed counts in the truth and scores as a miss";

    public SearchEffort Effort(double value) => new(transport.Kind.Knob, value);

    public async Task LoadAsync(IAsyncEnumerable<LabelledVector> vectors, CancellationToken ct)
    {
        _defaultKind = await transport.CreateIndexAsync(ct);

        long loaded = 0;
        float[]? first = null;
        var batch = new List<DocumentToWrite<VectorRow>>(BulkBatch);
        async Task FlushAsync()
        {
            if (batch.Count == 0)
                return;
            var result = await transport.ExecuteAsync(new BulkInsertOperation<VectorRow> { Documents = batch }, ct);
            if (result.IsSuccess == false)
                throw new InvalidOperationException($"The Elasticsearch load failed: {result.ErrorDetails}");
            loaded += batch.Count;
            batch = new List<DocumentToWrite<VectorRow>>(BulkBatch);
        }

        await foreach (var v in vectors.WithCancellation(ct))
        {
            first ??= v.Vector;
            batch.Add(new DocumentToWrite<VectorRow> { Id = v.Id, Document = new VectorRow(v.Vector, v.Label) });
            if (batch.Count == BulkBatch)
                await FlushAsync();
        }
        await FlushAsync();
        await transport.RefreshAsync(ct);

        var counted = await transport.GetDocumentCountAsync("");
        if (counted != loaded)
            throw new InvalidOperationException($"Index '{transport.Index}' counts {counted} documents after the refresh; the load sent {loaded}.");
        var probe = await transport.ExecuteAsync(new VectorSearchOperation { QueryVector = first!, FieldName = FieldName, TopK = 1 }, ct);
        if (probe.IsSuccess == false || probe.NeighborIds is not { Count: 1 })
            throw new InvalidOperationException($"Index '{transport.Index}' does not answer a kNN query after the load: {probe.ErrorDetails ?? "no hit"}.");

        var settings = await transport.ReadIndexSettingsAsync(ct);
        var durability = settings.GetValueOrDefault(ElasticsearchVectorTransport.DurabilitySetting);
        if (durability != ElasticsearchVectorTransport.DurabilityValue)
            throw new InvalidOperationException($"Index '{transport.Index}' reports {ElasticsearchVectorTransport.DurabilitySetting}={durability ?? "absent"}; the run needs '{ElasticsearchVectorTransport.DurabilityValue}'.");
        _refreshInterval = settings["index.refresh_interval"];
    }

    public OperationBase InsertOperation(LabelledVector vector) =>
        new InsertOperation<VectorRow> { Id = vector.Id, Payload = new VectorRow(vector.Vector, vector.Label) };

    public async Task<(OnDiskSize Total, OnDiskSize Index)> StoredSizesAsync() =>
        (OnDiskSize.Reported(transport.StorageSizeMetricName, await transport.GetStorageSizeBytesAsync()),
         OnDiskSize.Reported($"_disk_usage fields.{ElasticsearchVectorTransport.VectorField}.knn_vectors_in_bytes", await transport.ReadVectorDiskUsageBytesAsync(CancellationToken.None)));

    public async Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct)
    {
        var settings = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["index"] = transport.Index,
            ["index_kind.sent"] = transport.Kind.Name,
            ["index_kind.vendor_default"] = RequireLoaded(_defaultKind)
        };
        foreach (var (name, value) in await transport.ReadFieldOptionsAsync(ct))
        {
            settings["mapping." + name] = value;
            if (name.StartsWith("index_options.", StringComparison.Ordinal))
                settings["mapping." + name + ".source"] = name == "index_options.type" || (transport.Build is not null && name is "index_options.m" or "index_options.ef_construction")
                    ? BuildSetting.SetByBenchmark
                    : $"default: {VendorDefault}, not set by the benchmark";
        }
        var index = await transport.ReadIndexSettingsAsync(ct);
        foreach (var name in new[] { ElasticsearchVectorTransport.DurabilitySetting, "index.refresh_interval", "index.number_of_shards", "index.number_of_replicas" })
            settings[name] = index.GetValueOrDefault(name, "absent");
        settings["segments.count"] = (await transport.ReadStatsAsync(ct)).Segments.ToString(CultureInfo.InvariantCulture);
        settings["jvm.mem.heap_max_in_bytes"] = await transport.ReadHeapAsync(ct);
        var licence = await transport.ReadLicenceAsync(ct);
        settings["license.type"] = licence.Type;
        settings["license.status"] = licence.Status;
        settings["license.expiry_date"] = licence.Expiry;
        return settings;
    }

    public Task CleanupAsync() => transport.DeleteIndexAsync(CancellationToken.None);

    public void Dispose() => transport.Dispose();

    private string RequireLoaded(string? value) =>
        value ?? throw new InvalidOperationException($"Index '{transport.Index}' has not been loaded by this target; load before reading its settings.");
}
