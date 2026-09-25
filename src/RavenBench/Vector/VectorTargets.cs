using System.Globalization;
using System.Net;
using System.Text;
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

    /// <summary>The prefix the product puts before a set's base id.</summary>
    string IdPrefix { get; }

    /// <summary>What an acknowledged insert guarantees about its visibility to a later search, stated for the under-insert truth.</summary>
    string InsertVisibility { get; }

    DurabilityParity Durability { get; }

    SearchEffort Effort(int value);

    /// <summary>Loads the vectors into an empty store and returns when the index answers queries.</summary>
    Task LoadAsync(IAsyncEnumerable<LabelledVector> vectors, CancellationToken ct);

    OperationBase InsertOperation(LabelledVector vector);

    Task<OnDiskSize> StoredSizeAsync();

    /// <summary>The index definition and server settings in force, as the product reports them.</summary>
    Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct);

    /// <summary>Removes what the run created.</summary>
    Task CleanupAsync();
}

/// <summary>
/// RavenDB over the raw HTTP transport, the published path. The vectors are stored as float32 and the
/// index holds them unquantized, with the build parameters left at the server defaults.
/// </summary>
public sealed class RavenDbVectorTarget : IVectorTarget
{
    public const string CollectionName = PublishedSetImport.CollectionName;
    private const string IndexName = "VectorBench/Float32";

    private readonly string _url;
    private readonly string _database;
    private readonly RawHttpTransport _transport;

    public RavenDbVectorTarget(string url, string database, VectorMetric metric)
    {
        UnsupportedVectorMetricException.ThrowIfUnsupported(RawHttpTransport.RavenDbProductName, RavenDbVectorMetrics.Supported, metric);
        _url = url;
        _database = database;
        _transport = new RawHttpTransport(url, database, CompressionMode.Identity, HttpVersion.Version11);
    }

    public IYcsbTransport Transport => _transport;
    public string EffortFamily => "ravendb";
    public string FieldName => "Vector";
    public string FilterField => "Label";
    public string? ExpectedIndex => IndexName;
    public string IdPrefix => PublishedSetImport.DocumentIdPrefix;
    public string InsertVisibility => "approximate: RavenDB acknowledges a write before its vector index contains it, and the queries do not wait for indexing, so an acknowledged insert still being indexed counts in the truth and scores as a miss";
    public DurabilityParity Durability { get; } = new() { Setting = "durability", Value = "ravendb-default" };

    public SearchEffort Effort(int value) => SearchEffort.RavenDb(value);

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
            Name = IndexName,
            Maps = { $"from v in docs.{CollectionName} select new {{ Vector = CreateVector(v.Embedding), v.Label }}" },
            Fields =
            {
                ["Vector"] = new IndexFieldOptions
                {
                    Vector = new VectorOptions
                    {
                        SourceEmbeddingType = VectorEmbeddingType.Single,
                        DestinationEmbeddingType = VectorEmbeddingType.Single
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

    public async Task<OnDiskSize> StoredSizeAsync()
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        var stats = await store.Maintenance.SendAsync(new GetDetailedStatisticsOperation());
        return OnDiskSize.Reported("database SizeOnDisk (documents and indexes)", stats.SizeOnDisk.SizeInBytes);
    }

    public async Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct)
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        var index = await store.Maintenance.SendAsync(new GetIndexOperation(IndexName), ct)
                    ?? throw new InvalidOperationException($"Index '{IndexName}' does not exist; load before reading the settings.");
        var vector = index.Fields["Vector"].Vector;
        // Null build parameters mean the server applies its own defaults; the run never sets them.
        return new Dictionary<string, string>
        {
            ["index"] = IndexName,
            ["index.map"] = index.Maps.Single(),
            ["index.SearchEngineType"] = index.Configuration.TryGetValue("Indexing.Static.SearchEngineType", out var engine) ? engine : "server-default",
            ["vector.DestinationEmbeddingType"] = vector.DestinationEmbeddingType.ToString(),
            ["vector.NumberOfEdges"] = vector.NumberOfEdges?.ToString(CultureInfo.InvariantCulture) ?? "server-default",
            ["vector.NumberOfCandidatesForIndexing"] = vector.NumberOfCandidatesForIndexing?.ToString(CultureInfo.InvariantCulture) ?? "server-default"
        };
    }

    public async Task CleanupAsync()
    {
        using var store = HttpHelper.Create(_url, _database, HttpVersion.Version11);
        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(_database, hardDelete: true));
    }

    public void Dispose() => _transport.Dispose();
}

/// <summary>pgvector over the Apex.PgClient transport: binary COPY load, HNSW at the vendor defaults.</summary>
public sealed class PgVectorTarget(PgVectorTransport transport) : IVectorTarget
{
    public IYcsbTransport Transport => transport;
    public string EffortFamily => PgVectorTransport.Target;
    public string FieldName => "embedding";
    public string FilterField => "label";
    public string? ExpectedIndex => PgVectorTransport.IndexName;
    public string IdPrefix => "";
    public string InsertVisibility => "exact: PostgreSQL updates the HNSW index inside the inserting transaction, so an insert acknowledged by commit is visible to every query issued after it";
    public DurabilityParity Durability { get; } = new() { Setting = PostgresYcsbTransport.DurabilitySetting, Value = PostgresYcsbTransport.DurabilityValue };

    // The COPY batch bounds client memory; it does not change what the server stores.
    private const int CopyBatch = 10_000;

    public SearchEffort Effort(int value) => SearchEffort.PgVector(value);

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
        await transport.BuildIndexAsync(ct);
    }

    public OperationBase InsertOperation(LabelledVector vector) =>
        new InsertOperation<VectorRow> { Id = vector.Id, Payload = new VectorRow(vector.Vector, vector.Label) };

    public async Task<OnDiskSize> StoredSizeAsync() =>
        OnDiskSize.Reported(transport.StorageSizeMetricName, await transport.GetStorageSizeBytesAsync());

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
        return settings;
    }

    /// <summary>Replaces the HNSW index the runs measured with another index kind; the cross-check runs it last.</summary>
    public Task<string> ReplaceIndexAsync(string kind, IReadOnlyDictionary<string, int> options, IReadOnlyDictionary<string, string> session, CancellationToken ct) => transport.ReplaceIndexAsync(kind, options, session, ct);

    public Task<HashSet<string>> ReadStoredIdsAsync(CancellationToken ct) => transport.ReadStoredIdsAsync(ct);

    public Task CleanupAsync() => transport.DropTableAsync();

    public void Dispose() => transport.Dispose();
}
