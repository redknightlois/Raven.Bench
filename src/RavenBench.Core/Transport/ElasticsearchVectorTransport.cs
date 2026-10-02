using RavenBench.Core.Vector;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using RavenBench.Core.Workload;

namespace RavenBench.Core.Transport;

/// <summary>
/// One Elasticsearch <c>dense_vector</c> index kind the benchmark sends explicitly: its name, the
/// search knob the kind honours, and the vector storage the row states.
/// </summary>
/// <param name="RequiresEnterpriseLicence">True when the server refuses writes into the kind below an enterprise or trial licence.</param>
public sealed record ElasticsearchIndexKind(string Name, string Knob, string Storage, bool RequiresEnterpriseLicence)
{
    /// <summary>The kind builds an HNSW graph, so it takes <c>m</c> and <c>ef_construction</c>.</summary>
    public bool IsHnsw => Name.EndsWith("hnsw", StringComparison.Ordinal);

    public const string CandidatesKnob = "num_candidates";
    public const string VisitKnob = "visit_percentage";

    /// <summary>The licence types under which a kind that requires an enterprise licence accepts writes.</summary>
    public static readonly IReadOnlySet<string> EnterpriseLicences = new HashSet<string>(StringComparer.Ordinal) { "trial", "enterprise" };

    public static readonly IReadOnlyList<ElasticsearchIndexKind> All =
    [
        new("hnsw", CandidatesKnob, "float32, unquantized, hnsw", false),
        new("bbq_hnsw", CandidatesKnob, "bbq 1-bit, quantized, bbq_hnsw", false),
        new("bbq_disk", VisitKnob, "bbq 1-bit, quantized, bbq_disk", true)
    ];

    /// <summary>Throws <see cref="ArgumentException"/> naming the valid kinds when <paramref name="name"/> is not one of them.</summary>
    public static ElasticsearchIndexKind Named(string name) =>
        All.FirstOrDefault(k => string.Equals(k.Name, name, StringComparison.Ordinal))
        ?? throw new ArgumentException($"Elasticsearch index kind '{name}' is not one of {string.Join(", ", All.Select(k => k.Name))}.", nameof(name));
}

/// <summary>An Elasticsearch request answered with a non-success status. The message carries the method, the path and the server's body.</summary>
public sealed class ElasticsearchRequestException(HttpMethod method, string path, HttpStatusCode status, string body)
    : HttpRequestException($"Elasticsearch {method} {path} answered {(int)status} {status}: {body}", null, status);

/// <summary>A <c>_bulk</c> response reported <c>errors: true</c>. The message names the first failed item and its reason.</summary>
public sealed class ElasticsearchBulkException(string message) : InvalidOperationException(message);

/// <summary>The licence the server reports does not allow the index kind, so the run is refused before any index is created.</summary>
public sealed class ElasticsearchLicenceException(string kind, ElasticsearchLicence licence)
    : InvalidOperationException($"Elasticsearch index kind '{kind}' needs an enterprise or trial licence; the server reports licence type '{licence.Type}', status '{licence.Status}'. " +
                                "Start the cluster with xpack.license.self_generated.type=trial, or install an enterprise licence. The run is refused before load.")
{
    public ElasticsearchLicence Licence { get; } = licence;
}

/// <summary>The licence as <c>GET /_license</c> reports it; the expiry is <c>none</c> when the licence carries none.</summary>
public sealed record ElasticsearchLicence(string Type, string Status, string Expiry)
{
    public bool Allows(ElasticsearchIndexKind kind) =>
        kind.RequiresEnterpriseLicence == false || (Status == "active" && ElasticsearchIndexKind.EnterpriseLicences.Contains(Type));
}

/// <summary>
/// Drives vector search against one Elasticsearch index over the REST API on raw HTTP. The index has
/// one shard and no replicas, a <c>dense_vector</c> field whose similarity matches the set's metric,
/// and an index kind the benchmark always sends, with the HNSW build for a graph kind; every other index option stays at the vendor default.
/// A search returns ids only.
/// </summary>
public sealed class ElasticsearchVectorTransport : IYcsbTransport, IReportsStorageSize
{
    public const string Target = "elasticsearch";
    public const string Product = "Elasticsearch";
    public const string VectorField = "embedding";
    public const string LabelField = "label";
    public const string DurabilitySetting = "index.translog.durability";
    public const string DurabilityValue = "request";

    public static readonly IReadOnlySet<VectorMetric> Supported = new HashSet<VectorMetric> { VectorMetric.Cosine, VectorMetric.L2, VectorMetric.Dot };

    private readonly HttpClient _http;
    private readonly VectorMetric _metric;
    private readonly int _dimensions;

    /// <param name="metric">The set's metric. A metric Elasticsearch cannot serve is refused here, before any request.</param>
    /// <param name="kind">The index kind name; an unknown kind is refused here by name.</param>
    /// <param name="build">The graph a kind that builds HNSW is built with; null leaves it at the vendor default.</param>
    public ElasticsearchVectorTransport(string url, string index, VectorMetric metric, int dimensions, string kind, HnswBuild? build = null)
    {
        UnsupportedVectorMetricException.ThrowIfUnsupported(Target, Supported, metric);
        if (string.IsNullOrWhiteSpace(index))
            throw new ArgumentException("An Elasticsearch index name is required.", nameof(index));
        if (index != index.ToLowerInvariant())
            throw new ArgumentException($"Elasticsearch index name '{index}' must be lowercase.", nameof(index));
        if (dimensions < 1)
            throw new ArgumentOutOfRangeException(nameof(dimensions), dimensions, "The vector dimensions must be positive.");
        Kind = ElasticsearchIndexKind.Named(kind);
        Index = index;
        _metric = metric;
        _dimensions = dimensions;
        Build = Kind.IsHnsw ? build : null;
        _http = new HttpClient(new SocketsHttpHandler { PooledConnectionLifetime = Timeout.InfiniteTimeSpan }) { BaseAddress = new Uri(url.TrimEnd('/') + "/") };
    }

    public ElasticsearchIndexKind Kind { get; }

    /// <summary>The HNSW build sent in the mapping; null when the kind builds no graph or none was given.</summary>
    public HnswBuild? Build { get; }
    public string Index { get; }
    public string ProductName => Product;
    public bool ReportsWireBytes => false;
    public string StorageSizeMetricName => "_stats primaries.store.size_in_bytes";

    public static string Similarity(VectorMetric metric) => metric switch
    {
        VectorMetric.Cosine => "cosine",
        VectorMetric.L2 => "l2_norm",
        // Ranks by the raw inner product; dot_product would require unit-length vectors.
        VectorMetric.Dot => "max_inner_product",
        _ => throw new UnsupportedVectorMetricException(Target, metric)
    };

    /// <summary>
    /// The index body: one shard, no replicas, request durability, and the vector field with <c>index_options.type</c>
    /// set when a kind is given, plus <c>m</c> and <c>ef_construction</c> when a build is given.
    /// </summary>
    internal static string IndexBody(VectorMetric metric, int dimensions, ElasticsearchIndexKind? kind, HnswBuild? build = null) => Json(w =>
    {
        w.WriteStartObject("settings");
        w.WriteNumber("number_of_shards", 1);
        w.WriteNumber("number_of_replicas", 0);
        w.WriteString(DurabilitySetting, DurabilityValue);
        w.WriteEndObject();
        w.WriteStartObject("mappings");
        w.WriteStartObject("properties");
        w.WriteStartObject(VectorField);
        w.WriteString("type", "dense_vector");
        w.WriteNumber("dims", dimensions);
        w.WriteBoolean("index", true);
        w.WriteString("similarity", Similarity(metric));
        if (kind is not null)
        {
            w.WriteStartObject("index_options");
            w.WriteString("type", kind.Name);
            if (build is not null)
            {
                w.WriteNumber("m", build.M);
                w.WriteNumber("ef_construction", build.EfConstruction);
            }
            w.WriteEndObject();
        }
        w.WriteEndObject();
        w.WriteStartObject(LabelField);
        w.WriteString("type", "keyword");
        w.WriteEndObject();
        w.WriteEndObject();
        w.WriteEndObject();
    });

    /// <summary>
    /// The kNN search body. Only ids come back; the filter sits inside <c>knn.filter</c>, so it applies during the
    /// search. The effort must be the kind's own knob; no effort leaves the knob at the vendor default.
    /// </summary>
    internal static string SearchBody(VectorSearchOperation search, ElasticsearchIndexKind kind)
    {
        if (search.UseExactSearch)
            throw new NotSupportedException($"{nameof(ElasticsearchVectorTransport)} serves approximate kNN only.");
        if (search.Quantization != VectorQuantization.None)
            throw new NotSupportedException($"{nameof(ElasticsearchVectorTransport)} quantizes by index kind, not per query; quantization {search.Quantization} is not a query option.");
        if (search.Effort is { } e && e.Knob != kind.Knob)
            throw new NotSupportedException($"Elasticsearch index kind '{kind.Name}' has no search-effort knob '{e.Knob}'; its knob is '{kind.Knob}'.");
        if (search.TopK < 1)
            throw new ArgumentOutOfRangeException(nameof(search), search.TopK, "k must be positive.");
        return Json(w =>
        {
            w.WriteNumber("size", search.TopK);
            w.WriteBoolean("_source", false);
            w.WriteStartObject("knn");
            w.WriteString("field", search.FieldName);
            w.WriteStartArray("query_vector");
            foreach (var f in search.QueryVector)
                w.WriteNumberValue(f);
            w.WriteEndArray();
            w.WriteNumber("k", search.TopK);
            if (search.Effort is { Knob: ElasticsearchIndexKind.VisitKnob } visit)
                w.WriteNumber(visit.Knob, visit.Value);
            else if (search.Effort is { } candidates)
                w.WriteNumber(candidates.Knob, candidates.WholeValue);
            if (search.Filter is { } filter)
            {
                w.WriteStartObject("filter");
                w.WriteStartObject("term");
                w.WriteString(filter.Field, filter.Value);
                w.WriteEndObject();
                w.WriteEndObject();
            }
            w.WriteEndObject();
        });
    }

    /// <summary>The hit ids of a search response, nearest first.</summary>
    internal static string[] SearchIds(string response)
    {
        using var document = JsonDocument.Parse(response);
        // filter_path drops "hits" entirely when nothing matched.
        if (document.RootElement.TryGetProperty("hits", out var hits) == false)
            return [];
        return hits.GetProperty("hits").EnumerateArray().Select(h => h.GetProperty("_id").GetString()!).ToArray();
    }

    /// <summary>The NDJSON body of one <c>_bulk</c> request that indexes every document into the transport's index.</summary>
    internal static string BulkBody(IEnumerable<DocumentToWrite<VectorRow>> documents)
    {
        var body = new StringBuilder();
        foreach (var d in documents)
        {
            body.Append(Json(w =>
            {
                w.WriteStartObject("index");
                w.WriteString("_id", d.Id);
                w.WriteEndObject();
            })).Append('\n');
            body.Append(DocumentBody(d.Document)).Append('\n');
        }
        return body.ToString();
    }

    internal static string DocumentBody(VectorRow row) => Json(w =>
    {
        w.WriteStartArray(VectorField);
        foreach (var f in row.Embedding)
            w.WriteNumberValue(f);
        w.WriteEndArray();
        if (row.Label is null)
            w.WriteNull(LabelField);
        else
            w.WriteString(LabelField, row.Label);
    });

    /// <summary>Throws <see cref="ElasticsearchBulkException"/> naming the first failed item when a <c>_bulk</c> response reports errors.</summary>
    internal static void RequireBulkSuccess(string response)
    {
        using var document = JsonDocument.Parse(response);
        if (document.RootElement.GetProperty("errors").GetBoolean() == false)
            return;
        var items = document.RootElement.GetProperty("items").EnumerateArray().Select(i => i.EnumerateObject().First().Value).ToList();
        var failed = items.Where(i => i.TryGetProperty("error", out _)).ToList();
        var first = failed.Count > 0 ? failed[0] : throw new ElasticsearchBulkException("The _bulk response reports errors but no item carries one.");
        throw new ElasticsearchBulkException($"The _bulk request failed for {failed.Count} of {items.Count} documents; the first, '{first.GetProperty("_id").GetString()}', failed with {first.GetProperty("error")}.");
    }

    /// <summary>
    /// Flattens a JSON object into dotted keys with string values. The index options of the vector field
    /// and the index settings are recorded this way, as the server reports them.
    /// </summary>
    internal static void Flatten(JsonElement element, string prefix, IDictionary<string, string> into)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            into[prefix] = element.ValueKind == JsonValueKind.String ? element.GetString()! : element.GetRawText();
            return;
        }
        foreach (var property in element.EnumerateObject())
            Flatten(property.Value, prefix.Length == 0 ? property.Name : $"{prefix}.{property.Name}", into);
    }

    public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => op switch
    {
        VectorSearchOperation search => PostgresYcsbTransport.GuardedAsync(() => SearchAsync(search, ct), ct),
        BulkInsertOperation<VectorRow> bulk => PostgresYcsbTransport.GuardedAsync(() => BulkInsertAsync(bulk, ct), ct),
        InsertOperation<VectorRow> insert => PostgresYcsbTransport.GuardedAsync(() => InsertAsync(insert, ct), ct),
        _ => throw new NotSupportedException($"{nameof(ElasticsearchVectorTransport)} cannot execute operation type {op.GetType().Name}.")
    };

    public async Task PutAsync<T>(string id, T document)
    {
        if (document is not VectorRow row)
            throw new NotSupportedException($"{nameof(ElasticsearchVectorTransport)} stores {nameof(VectorRow)}, not {typeof(T).Name}.");
        await SendAsync(HttpMethod.Put, $"{Index}/_doc/{Uri.EscapeDataString(id)}", DocumentBody(row), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>The index is created by <see cref="CreateIndexAsync"/>; this only checks that the name is the transport's.</summary>
    public Task EnsureDatabaseExistsAsync(string databaseName) =>
        string.Equals(databaseName, Index, StringComparison.Ordinal)
            ? Task.CompletedTask
            : throw new ArgumentException($"The transport was built for index '{Index}' but the run asked for '{databaseName}'.", nameof(databaseName));

    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        if (idPrefix.Length != 0)
            throw new NotSupportedException("The Elasticsearch vector index holds the set's ids unprefixed.");
        using var count = await SendJsonAsync(HttpMethod.Get, $"{Index}/_count", null, CancellationToken.None).ConfigureAwait(false);
        return count.RootElement.GetProperty("count").GetInt64();
    }

    public async Task<string> GetServerVersionAsync()
    {
        using var root = await SendJsonAsync(HttpMethod.Get, "", null, CancellationToken.None).ConfigureAwait(false);
        return root.RootElement.GetProperty("version").GetProperty("number").GetString()!;
    }

    public async Task<long> GetStorageSizeBytesAsync() => (await ReadStatsAsync(CancellationToken.None).ConfigureAwait(false)).StoreBytes;

    public async Task<ElasticsearchLicence> ReadLicenceAsync(CancellationToken ct)
    {
        using var response = await SendJsonAsync(HttpMethod.Get, "_license", null, ct).ConfigureAwait(false);
        var licence = response.RootElement.GetProperty("license");
        return new ElasticsearchLicence(
            licence.GetProperty("type").GetString()!,
            licence.GetProperty("status").GetString()!,
            licence.TryGetProperty("expiry_date", out var expiry) ? expiry.GetString()! : "none");
    }

    /// <summary>
    /// Refuses a kind the licence does not allow, refuses an index that already exists, probes the kind the
    /// server's own default resolves to for this field, then creates the index and waits until its shard is allocated.
    /// Returns the default kind.
    /// </summary>
    public async Task<string> CreateIndexAsync(CancellationToken ct)
    {
        var licence = await ReadLicenceAsync(ct).ConfigureAwait(false);
        if (licence.Allows(Kind) == false)
            throw new ElasticsearchLicenceException(Kind.Name, licence);
        if (await ExistsAsync(Index, ct).ConfigureAwait(false))
            throw new InvalidOperationException($"Index '{Index}' already exists; the vector run loads into a fresh index.");

        var probe = Index + "-default-probe";
        await SendAsync(HttpMethod.Put, probe, IndexBody(_metric, _dimensions, kind: null), ct).ConfigureAwait(false);
        string defaultKind;
        try
        {
            await SendAsync(HttpMethod.Get, $"_cluster/health/{probe}?wait_for_status=green&timeout=60s", null, ct).ConfigureAwait(false);
            defaultKind = (await ReadFieldOptionsAsync(probe, ct).ConfigureAwait(false))["index_options.type"];
        }
        finally
        {
            await SendAsync(HttpMethod.Delete, probe, null, CancellationToken.None).ConfigureAwait(false);
        }

        await SendAsync(HttpMethod.Put, Index, IndexBody(_metric, _dimensions, Kind, Build), ct).ConfigureAwait(false);
        // Answers 408 when the shard is not allocated in time, for example under a disk watermark; GET _cluster/allocation/explain names the reason.
        await SendAsync(HttpMethod.Get, $"_cluster/health/{Index}?wait_for_status=green&timeout=60s", null, ct).ConfigureAwait(false);
        return defaultKind;
    }

    public async Task RefreshAsync(CancellationToken ct) => await SendAsync(HttpMethod.Post, $"{Index}/_refresh", null, ct).ConfigureAwait(false);

    /// <summary>The primary store size and the primary segment count from <c>_stats</c>.</summary>
    public async Task<(long StoreBytes, long Segments)> ReadStatsAsync(CancellationToken ct)
    {
        using var stats = await SendJsonAsync(HttpMethod.Get, $"{Index}/_stats/store,segments", null, ct).ConfigureAwait(false);
        var primaries = stats.RootElement.GetProperty("indices").GetProperty(Index).GetProperty("primaries");
        return (primaries.GetProperty("store").GetProperty("size_in_bytes").GetInt64(), primaries.GetProperty("segments").GetProperty("count").GetInt64());
    }

    /// <summary>The vector field's mapping with defaults included, flattened, as <c>_mapping/field</c> reports it.</summary>
    public Task<Dictionary<string, string>> ReadFieldOptionsAsync(CancellationToken ct) => ReadFieldOptionsAsync(Index, ct);

    private async Task<Dictionary<string, string>> ReadFieldOptionsAsync(string index, CancellationToken ct)
    {
        using var mapping = await SendJsonAsync(HttpMethod.Get, $"{index}/_mapping/field/{VectorField}?include_defaults=true", null, ct).ConfigureAwait(false);
        var field = mapping.RootElement.GetProperty(index).GetProperty("mappings").GetProperty(VectorField).GetProperty("mapping").GetProperty(VectorField);
        var flat = new Dictionary<string, string>(StringComparer.Ordinal);
        Flatten(field, "", flat);
        return flat;
    }

    /// <summary>The index settings with defaults included, flattened; a set value wins over its default.</summary>
    public async Task<Dictionary<string, string>> ReadIndexSettingsAsync(CancellationToken ct)
    {
        using var settings = await SendJsonAsync(HttpMethod.Get, $"{Index}/_settings?include_defaults=true&flat_settings=true", null, ct).ConfigureAwait(false);
        var index = settings.RootElement.GetProperty(Index);
        var flat = new Dictionary<string, string>(StringComparer.Ordinal);
        Flatten(index.GetProperty("defaults"), "", flat);
        Flatten(index.GetProperty("settings"), "", flat);
        return flat;
    }

    /// <summary>The maximum heap of every node, as <c>_nodes/stats/jvm</c> reports it.</summary>
    public async Task<string> ReadHeapAsync(CancellationToken ct)
    {
        using var stats = await SendJsonAsync(HttpMethod.Get, "_nodes/stats/jvm", null, ct).ConfigureAwait(false);
        return string.Join(',', stats.RootElement.GetProperty("nodes").EnumerateObject()
            .Select(n => n.Value.GetProperty("jvm").GetProperty("mem").GetProperty("heap_max_in_bytes").GetInt64().ToString(CultureInfo.InvariantCulture)));
    }

    public async Task DeleteIndexAsync(CancellationToken ct)
    {
        if (await ExistsAsync(Index, ct).ConfigureAwait(false))
            await SendAsync(HttpMethod.Delete, Index, null, ct).ConfigureAwait(false);
    }

    public void Dispose() => _http.Dispose();

    private async Task<bool> ExistsAsync(string index, CancellationToken ct)
    {
        using var response = await _http.SendAsync(new HttpRequestMessage(HttpMethod.Head, index), ct).ConfigureAwait(false);
        return response.StatusCode switch
        {
            HttpStatusCode.OK => true,
            HttpStatusCode.NotFound => false,
            _ => throw new ElasticsearchRequestException(HttpMethod.Head, index, response.StatusCode, "")
        };
    }

    private async Task<TransportResult> SearchAsync(VectorSearchOperation search, CancellationToken ct)
    {
        var body = SearchBody(search, Kind);
        var response = await SendAsync(HttpMethod.Post, $"{Index}/_search?filter_path=hits.hits._id", body, ct).ConfigureAwait(false);
        var ids = SearchIds(response);
        return new TransportResult(body.Length, response.Length, resultCount: ids.Length) { NeighborIds = ids, EffortInForce = search.Effort };
    }

    private async Task<TransportResult> BulkInsertAsync(BulkInsertOperation<VectorRow> bulk, CancellationToken ct)
    {
        var body = BulkBody(bulk.Documents);
        var response = await SendAsync(HttpMethod.Post, $"{Index}/_bulk", body, ct, "application/x-ndjson").ConfigureAwait(false);
        RequireBulkSuccess(response);
        return new TransportResult(body.Length, response.Length);
    }

    private async Task<TransportResult> InsertAsync(InsertOperation<VectorRow> insert, CancellationToken ct)
    {
        var body = DocumentBody(insert.Payload);
        var response = await SendAsync(HttpMethod.Put, $"{Index}/_doc/{Uri.EscapeDataString(insert.Id)}", body, ct).ConfigureAwait(false);
        return new TransportResult(body.Length, response.Length);
    }

    private async Task<JsonDocument> SendJsonAsync(HttpMethod method, string path, string? body, CancellationToken ct) =>
        JsonDocument.Parse(await SendAsync(method, path, body, ct).ConfigureAwait(false));

    private async Task<string> SendAsync(HttpMethod method, string path, string? body, CancellationToken ct, string mediaType = "application/json")
    {
        using var request = new HttpRequestMessage(method, path);
        if (body is not null)
            request.Content = new StringContent(body, Encoding.UTF8, mediaType);
        using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
        var text = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        if (response.IsSuccessStatusCode == false)
            throw new ElasticsearchRequestException(method, path, response.StatusCode, text);
        return text;
    }

    private static string Json(Action<Utf8JsonWriter> body)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            body(writer);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
