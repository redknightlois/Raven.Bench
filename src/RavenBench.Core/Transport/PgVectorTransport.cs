using RavenBench.Core.Vector;
using System.Globalization;
using System.Text.RegularExpressions;
using Apex.PgClient;
using Apex.SqlClient;
using RavenBench.Core.Workload;

namespace RavenBench.Core.Transport;

/// <summary>One vector row: the embedding and the optional metadata label a filtered search matches.</summary>
public sealed record VectorRow(float[] Embedding, string? Label);

/// <summary>
/// The pgvector build and server settings in force, as the server reports them. Nothing here is
/// written by the harness.
/// </summary>
/// <param name="ExtensionVersion">The <c>extversion</c> of the installed extension.</param>
/// <param name="IndexDefinition">The HNSW index as <c>pg_get_indexdef</c> reports it.</param>
/// <param name="IndexOptions">The index <c>reloptions</c>; empty when the index was built at the vendor defaults.</param>
/// <param name="ServerSettings">Each recorded setting and the value <c>SHOW</c> returns for it.</param>
public sealed record PgVectorServerSettings(
    string ExtensionVersion,
    string IndexDefinition,
    IReadOnlyList<string> IndexOptions,
    IReadOnlyDictionary<string, string> ServerSettings);

/// <summary>The HNSW operator class and the distance operator pgvector uses for each metric.</summary>
public static class PgVectorMetrics
{
    public static readonly IReadOnlySet<VectorMetric> Supported = new HashSet<VectorMetric> { VectorMetric.Cosine, VectorMetric.L2, VectorMetric.Dot };

    public static string OperatorClass(VectorMetric metric) => metric switch
    {
        VectorMetric.Cosine => "vector_cosine_ops",
        VectorMetric.L2 => "vector_l2_ops",
        VectorMetric.Dot => "vector_ip_ops",
        _ => throw new UnsupportedVectorMetricException(PgVectorTransport.Target, metric)
    };

    /// <summary>The operator whose ascending order is nearest first. <c>&lt;#&gt;</c> is the negative inner product.</summary>
    public static string DistanceOperator(VectorMetric metric) => metric switch
    {
        VectorMetric.Cosine => "<=>",
        VectorMetric.L2 => "<->",
        VectorMetric.Dot => "<#>",
        _ => throw new UnsupportedVectorMetricException(PgVectorTransport.Target, metric)
    };
}

/// <summary>Thrown when the plan of an exact search uses the HNSW index, so the search is not exact.</summary>
public sealed class ExactSearchUsedIndexException(string indexName, string plan)
    : InvalidOperationException($"The exact search plan uses index '{indexName}', so it is approximate:\n{plan}")
{
    public string IndexName { get; } = indexName;
}

/// <summary>Thrown when the server has fewer free connections than the pool needs, one per concurrent worker.</summary>
public sealed class PgVectorConnectionLimitException(int needed, long free, long maxConnections)
    : InvalidOperationException($"The pool needs {needed} connections, one per concurrent worker, and the server has {free} free under max_connections={maxConnections}. Raise max_connections on the server or lower the concurrency.")
{
    public int Needed { get; } = needed;
    public long Free { get; } = free;
}

/// <summary>
/// Drives vector search against PostgreSQL with pgvector through Apex.PgClient. It shares the
/// PostgreSQL connect path, durability parity and connection set with the ycsb transport. The
/// <c>vector</c> type travels in binary through <see cref="PgVectorCodec"/>, registered for the oid
/// the server assigned to the extension. The pinned driver's prepared statements send parameters as
/// text only, so each search runs as one typed extended-protocol exchange with binary parameters;
/// each worker connection holds its fixed statement shape and the search settings it applied. The
/// run database is created through the connection string's own database when it does not exist.
/// </summary>
public sealed partial class PgVectorTransport : IYcsbTransport, IReportsStorageSize
{
    /// <summary>Scenario target name for pgvector.</summary>
    public const string Target = "pgvector";

    public const string TableName = "vectors";
    public const string IndexName = "vectors_embedding_hnsw";

    /// <summary>The settings the result records as the server reports them; one the server does not know is recorded as absent.</summary>
    public static readonly IReadOnlyList<string> RecordedSettings =
        [PostgresYcsbTransport.DurabilitySetting, "max_connections", "shared_buffers", "maintenance_work_mem", "max_parallel_maintenance_workers", "work_mem",
         SearchEffort.PgVectorKnob, "hnsw.iterative_scan", "hnsw.max_scan_tuples"];

    /// <summary>The index kinds a replacement index may use, each with the session knob that sets its search effort.</summary>
    public static readonly IReadOnlyDictionary<string, string> SearchKnobs = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["hnsw"] = SearchEffort.PgVectorKnob,
        ["ivfflat"] = "ivfflat.probes"
    };

    internal const string CreateExtensionSql = "CREATE EXTENSION IF NOT EXISTS vector";
    /// <summary>
    /// Calls into the extension so the session loads its library; <c>hnsw.ef_search</c> is unknown to a
    /// session until then.
    /// </summary>
    internal const string LoadExtensionSql = "SELECT vector_dims('[1]'::vector)";
    internal const string VectorOidSql = "SELECT 'vector'::regtype::oid::int8";
    internal const string BulkCopySql = "COPY " + TableName + " (id, embedding, label) FROM STDIN (FORMAT BINARY)";
    internal const string InsertSql = "INSERT INTO " + TableName + " (id, embedding, label) VALUES ($1, $2, $3)";
    internal const string PutSql = InsertSql + " ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding, label = EXCLUDED.label";
    internal const string CountSql = "SELECT count(*)::int8 FROM " + TableName + " WHERE starts_with(id, $1)";
    internal const string ApplyEffortSql = "SELECT set_config($1, $2, false)";
    internal const string ShowEffortSql = "SHOW " + SearchEffort.PgVectorKnob;
    /// <summary>The definition and options of the named index as the search path resolves it, never a same-named index in another schema.</summary>
    internal const string IndexDefinitionSql = "SELECT pg_get_indexdef(c.oid), coalesce(array_to_string(c.reloptions, ','), '') FROM pg_class c WHERE c.oid = to_regclass($1)";
    internal const string StorageSizeSql = "SELECT pg_total_relation_size('" + TableName + "')::int8";

    private readonly PgConnectOptions _maintenanceOptions;
    private readonly PgConnectOptions _baseOptions;
    private readonly int _maxConcurrency;
    private readonly VectorMetric _metric;
    private readonly int _dimensions;
    private readonly Lazy<Task> _ready;
    private PgType _vectorType;
    private PgConnection? _setup;
    private PgConnectionSet<SearchSlot>? _workers;
    private string _productName = string.Empty;
    private string _serverVersion = string.Empty;
    private bool _disposed;

    /// <param name="metric">The set's metric. A metric pgvector cannot serve is refused here, before any connection.</param>
    /// <param name="dimensions">The set's dimensions; the column is typed <c>vector(dimensions)</c>.</param>
    public PgVectorTransport(string connectionString, string databaseName, int maxConcurrency, VectorMetric metric, int dimensions)
    {
        UnsupportedVectorMetricException.ThrowIfUnsupported(Target, PgVectorMetrics.Supported, metric);
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("A PostgreSQL connection string is required.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("A PostgreSQL database name is required.", nameof(databaseName));
        if (maxConcurrency < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), maxConcurrency, "The concurrency ceiling must be positive.");
        if (dimensions is < 1 or > PgVectorCodec.MaxDimensions)
            throw new ArgumentOutOfRangeException(nameof(dimensions), dimensions, $"pgvector stores 1 to {PgVectorCodec.MaxDimensions} dimensions.");

        RecordedEndpoint = ConnectionStringRedaction.Redact(connectionString);
        _maintenanceOptions = PgConnectOptions.Parse(connectionString) with { CachePreparedStatements = false };
        _baseOptions = PostgresYcsbTransport.ResolveOptions(connectionString, databaseName);
        _maxConcurrency = maxConcurrency;
        _metric = metric;
        _dimensions = dimensions;
        _ready = new Lazy<Task>(InitializeAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public string ProductName => _productName.Length > 0
        ? _productName
        : throw new InvalidOperationException("The pgvector transport has not connected. Call EnsureDatabaseExistsAsync before reading ProductName.");

    public string RecordedEndpoint { get; }

    /// <summary>The database the run addresses.</summary>
    public string DatabaseName => _baseOptions.Database;

    /// <summary>The driver does not expose the socket, so a reported byte count is not a wire size.</summary>
    public bool ReportsWireBytes => false;

    public string StorageSizeMetricName => "pg_total_relation_size";

    /// <summary>The search statement for the metric; a filter adds a parameterised equality predicate on its field.</summary>
    internal static string SearchSql(VectorMetric metric, VectorFilter? filter) =>
        $"SELECT id FROM {TableName}{(filter is null ? "" : $" WHERE {filter.Field} = $3")} ORDER BY embedding {PgVectorMetrics.DistanceOperator(metric)} $1 LIMIT $2";

    public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => op switch
    {
        VectorSearchOperation search => PostgresYcsbTransport.GuardedAsync(() => SearchAsync(search, ct), ct),
        BulkInsertOperation<VectorRow> bulk => PostgresYcsbTransport.GuardedAsync(() => BulkInsertAsync(bulk, ct), ct),
        InsertOperation<VectorRow> insert => PostgresYcsbTransport.GuardedAsync(() => InsertAsync(insert, ct), ct),
        _ => throw new NotSupportedException($"{nameof(PgVectorTransport)} cannot execute operation type {op.GetType().Name}.")
    };

    public async Task PutAsync<T>(string id, T document)
    {
        if (document is not VectorRow row)
            throw new NotSupportedException($"{nameof(PgVectorTransport)} stores {nameof(VectorRow)}, not {typeof(T).Name}.");

        await _ready.Value.ConfigureAwait(false);
        await _setup!.ExecuteTypedAsync(PutSql, RowParameters(id, row), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Creates the extension and the table idempotently in the database the transport was built for.</summary>
    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        if (string.Equals(databaseName, _baseOptions.Database, StringComparison.Ordinal) == false)
            throw new ArgumentException($"The transport was built for database '{_baseOptions.Database}' but the run asked for '{databaseName}'.", nameof(databaseName));

        await _ready.Value.ConfigureAwait(false);
    }

    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        await _ready.Value.ConfigureAwait(false);
        var rows = await _setup!.QueryAsync(CountSql, SqlParameters.Create(idPrefix), CancellationToken.None).ConfigureAwait(false);
        return rows[0].Get<long>(0);
    }

    /// <summary>Reads every id the table holds, including rows whose insert committed after the client stopped counting.</summary>
    public async Task<HashSet<string>> ReadStoredIdsAsync(CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        var rows = await _setup!.QueryAsync($"SELECT id FROM {TableName}", ct).ConfigureAwait(false);
        return rows.Select(r => r.Get<string>(0)).ToHashSet(StringComparer.Ordinal);
    }

    public async Task<string> GetServerVersionAsync()
    {
        await _ready.Value.ConfigureAwait(false);
        return _serverVersion;
    }

    public async Task<long> GetStorageSizeBytesAsync()
    {
        await _ready.Value.ConfigureAwait(false);
        var rows = await _setup!.QueryAsync(StorageSizeSql, CancellationToken.None).ConfigureAwait(false);
        return rows[0].Get<long>(0);
    }

    /// <summary>
    /// Builds the HNSW index with the operator class of the set's metric and <paramref name="build"/>; no build
    /// leaves <c>m</c> and <c>ef_construction</c> at the vendor defaults. Returns when the index is queryable.
    /// </summary>
    public async Task BuildIndexAsync(HnswBuild? build = null, CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        var with = build is null ? "" : FormattableString.Invariant($" WITH (m = {build.M}, ef_construction = {build.EfConstruction})");
        await _setup!.ExecuteAsync(
            $"CREATE INDEX IF NOT EXISTS {IndexName} ON {TableName} USING hnsw (embedding {PgVectorMetrics.OperatorClass(_metric)}){with}", ct).ConfigureAwait(false);
        await _setup.ExecuteAsync($"ANALYZE {TableName}", ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Replaces the HNSW index with one of <paramref name="kind"/> built with <paramref name="options"/>,
    /// under the <paramref name="session"/> settings, and returns its definition as <c>pg_get_indexdef</c> reports it.
    /// The drop, the build and the session settings share one transaction: the settings end with it, and a
    /// failed or cancelled build keeps the old index.
    /// </summary>
    public async Task<string> ReplaceIndexAsync(string kind, IReadOnlyDictionary<string, int> options, IReadOnlyDictionary<string, string> session, CancellationToken ct = default)
    {
        if (SearchKnobs.ContainsKey(kind) == false)
            throw new ArgumentException($"Index kind '{kind}' is not one of {string.Join(", ", SearchKnobs.Keys)}.", nameof(kind));
        if (options.Keys.FirstOrDefault(key => PlainName().IsMatch(key) == false) is { } bad)
            throw new ArgumentException($"Index option '{bad}' is not a plain option name.", nameof(options));

        await _ready.Value.ConfigureAwait(false);
        var name = $"vectors_embedding_{kind}";
        var with = options.Count == 0 ? "" : $" WITH ({string.Join(", ", options.Select(o => $"{o.Key} = {o.Value.ToString(CultureInfo.InvariantCulture)}"))})";
        if (session.Keys.FirstOrDefault(key => PlainName().IsMatch(key) == false) is { } badSetting)
            throw new ArgumentException($"Session setting '{badSetting}' is not a plain setting name.", nameof(session));
        await InTransactionAsync(_setup!, commit: true, async () =>
        {
            foreach (var (setting, value) in session)
                await _setup!.QueryAsync("SELECT set_config($1, $2, true)", SqlParameters.Create(setting, value), ct).ConfigureAwait(false);
            await _setup!.ExecuteAsync($"DROP INDEX IF EXISTS {IndexName}", ct).ConfigureAwait(false);
            await _setup.ExecuteAsync($"CREATE INDEX {name} ON {TableName} USING {kind} (embedding {PgVectorMetrics.OperatorClass(_metric)}){with}", ct).ConfigureAwait(false);
            return true;
        }, ct).ConfigureAwait(false);
        await _setup!.ExecuteAsync($"ANALYZE {TableName}", ct).ConfigureAwait(false);
        return (await _setup.QueryAsync(IndexDefinitionSql, SqlParameters.Create(name), ct).ConfigureAwait(false))[0].Get<string>(0);
    }

    /// <summary>The value of one setting on the setup connection, or <c>absent</c> when the server does not know it.</summary>
    internal async Task<string> ReadSettingAsync(string name, CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        return (await _setup!.QueryAsync("SELECT coalesce(current_setting($1, true), 'absent')", SqlParameters.Create(name), ct).ConfigureAwait(false))[0].Get<string>(0);
    }

    /// <summary>
    /// The <c>m</c> and <c>ef_construction</c> the HNSW index was built with, read from its metapage through
    /// <c>pageinspect</c>. The extension is created inside a transaction that always rolls back, so the
    /// database keeps no trace of it. Throws <see cref="InvalidDataException"/> when block 0 is not an HNSW metapage.
    /// </summary>
    public async Task<(int M, int EfConstruction)> ReadHnswBuildParametersAsync(CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        return await InTransactionAsync(_setup!, commit: false, async () =>
        {
            await _setup!.ExecuteAsync("CREATE EXTENSION IF NOT EXISTS pageinspect", ct).ConfigureAwait(false);
            var row = (await _setup.QueryAsync(HnswMetaPageSql, SqlParameters.Create(IndexName), ct).ConfigureAwait(false))[0];
            if (row.Get<long>(0) != HnswMagicNumber)
                throw new InvalidDataException($"Block 0 of index '{IndexName}' carries magic 0x{row.Get<long>(0):X8}, not the HNSW metapage magic 0x{HnswMagicNumber:X8}.");
            return ((int)row.Get<long>(1), (int)row.Get<long>(2));
        }, ct).ConfigureAwait(false);
    }

    private const long HnswMagicNumber = 0xA953A953;

    // HnswMetaPageData after the 24-byte page header; PostgreSQL gives | and << equal precedence, so each shift is parenthesized: magic uint32 at 24, m uint16 at 36, efConstruction uint16 at 38, little-endian.
    private const string HnswMetaPageSql =
        "SELECT get_byte(p, 24)::int8 | (get_byte(p, 25)::int8 << 8) | (get_byte(p, 26)::int8 << 16) | (get_byte(p, 27)::int8 << 24), " +
        "(get_byte(p, 36) | (get_byte(p, 37) << 8))::int8, (get_byte(p, 38) | (get_byte(p, 39) << 8))::int8 " +
        "FROM (SELECT get_raw_page($1, 0) AS p) page";

    /// <summary>
    /// Runs the body in one transaction and commits or rolls back at its end. A failure rolls back; a failed
    /// rollback is attached to the failure that ended the body and never replaces it.
    /// </summary>
    private static async Task<T> InTransactionAsync<T>(PgConnection connection, bool commit, Func<Task<T>> body, CancellationToken ct)
    {
        await connection.ExecuteAsync("BEGIN", ct).ConfigureAwait(false);
        T result;
        try
        {
            result = await body().ConfigureAwait(false);
        }
        catch (Exception failure)
        {
            try
            {
                await connection.ExecuteAsync("ROLLBACK", CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception rollback)
            {
                throw new AggregateException(failure, rollback);
            }
            throw;
        }
        await connection.ExecuteAsync(commit ? "COMMIT" : "ROLLBACK", CancellationToken.None).ConfigureAwait(false);
        return result;
    }

    /// <summary>Drops the table and its index; a check that loaded a sample leaves nothing behind.</summary>
    public async Task DropTableAsync(CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        await _setup!.ExecuteAsync($"DROP TABLE IF EXISTS {TableName}", ct).ConfigureAwait(false);
    }

    /// <summary>Reads the extension version, the index definition and the recorded settings from the server.</summary>
    public async Task<PgVectorServerSettings> ReadServerSettingsAsync(CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        var extension = await _setup!.QueryAsync("SELECT extversion FROM pg_extension WHERE extname = 'vector'", ct).ConfigureAwait(false);
        var index = await _setup.QueryAsync(IndexDefinitionSql, SqlParameters.Create(IndexName), ct).ConfigureAwait(false);
        if (index.Count == 0)
            throw new InvalidOperationException($"Index '{IndexName}' does not exist; build it before reading the settings.");

        var settings = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var name in RecordedSettings)
            settings[name] = await ReadSettingAsync(name, ct).ConfigureAwait(false);

        var options = index[0].Get<string>(1);
        return new PgVectorServerSettings(
            extension[0].Get<string>(0),
            index[0].Get<string>(0),
            options.Length == 0 ? [] : options.Split(','),
            settings);
    }

    /// <summary>
    /// The exact k nearest ids by a sequential scan: index scans are disabled for the transaction, and
    /// the plan of the same statement is read first and must not name the HNSW index.
    /// </summary>
    public async Task<IReadOnlyList<string>> ExactSearchAsync(float[] query, int k, VectorFilter? filter, CancellationToken ct = default)
    {
        await _ready.Value.ConfigureAwait(false);
        var sql = SearchSql(_metric, filter);
        var parameters = SearchParameters(query, k, filter);
        return await _workers!.UseAsync(async slot =>
        {
            await slot.Connection.ExecuteAsync("BEGIN", ct).ConfigureAwait(false);
            try
            {
                await slot.Connection.ExecuteAsync("SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = off", ct).ConfigureAwait(false);
                var plan = await slot.Connection.QueryTypedAsync("EXPLAIN (COSTS OFF) " + sql, parameters, ct).ConfigureAwait(false);
                RequireNoIndex(plan.Select(r => r.Get<string>(0)).ToList(), IndexName);
                var rows = await slot.Connection.QueryTypedAsync(sql, parameters, ct).ConfigureAwait(false);
                await slot.Connection.ExecuteAsync("ROLLBACK", ct).ConfigureAwait(false);
                return (IReadOnlyList<string>)rows.Select(r => r.Get<string>(0)).ToList();
            }
            catch
            {
                // The rollback of a failed transaction must not replace the failure that ended it.
                try { await slot.Connection.ExecuteAsync("ROLLBACK", CancellationToken.None).ConfigureAwait(false); } catch (Exception) { }
                throw;
            }
        }, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs one body on a worker connection with the registered <c>vector</c> type. The state tests use
    /// it to read back what the server stored and the session settings a worker runs under.
    /// </summary>
    internal async Task<T> ReadWithWorkerAsync<T>(Func<PgConnection, PgType, Task<T>> body)
    {
        await _ready.Value.ConfigureAwait(false);
        return await _workers!.UseAsync(slot => body(slot.Connection, _vectorType), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Throws by index name when any plan line references the index.</summary>
    public static void RequireNoIndex(IReadOnlyList<string> planLines, string indexName)
    {
        if (planLines.Any(line => line.Contains(indexName, StringComparison.Ordinal)))
            throw new ExactSearchUsedIndexException(indexName, string.Join('\n', planLines));
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        if (_ready.IsValueCreated == false || _ready.Value.IsCompletedSuccessfully == false)
            return;

        _workers!.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _setup!.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private async Task InitializeAsync()
    {
        await PrepareServerAsync().ConfigureAwait(false);
        uint oid;
        await using (var bootstrap = await PgConnectionSet<SearchSlot>.ConnectAsync(_baseOptions).ConfigureAwait(false))
        {
            await bootstrap.ExecuteAsync(CreateExtensionSql, CancellationToken.None).ConfigureAwait(false);
            oid = checked((uint)(await bootstrap.QueryAsync(VectorOidSql, CancellationToken.None).ConfigureAwait(false))[0].Get<long>(0));
        }

        _vectorType = new PgType(oid, PgVectorCodec.TypeName);
        var options = _baseOptions with { TypeRegistry = PgVectorCodec.Register(oid) };
        _setup = await PgConnectionSet<SearchSlot>.ConnectAsync(options).ConfigureAwait(false);
        try
        {
            _productName = PostgresYcsbTransport.RequireMetadata(_setup.DatabaseMetadata.ProductName, "product name");
            _serverVersion = PostgresYcsbTransport.RequireMetadata(_setup.DatabaseMetadata.FullVersion, "version");
            await _setup.ExecuteAsync(
                $"CREATE TABLE IF NOT EXISTS {TableName} (id text PRIMARY KEY, embedding vector({_dimensions}) NOT NULL, label text)",
                CancellationToken.None).ConfigureAwait(false);
            await RequireColumnDimensionsAsync(_setup).ConfigureAwait(false);
            await _setup.ExecuteAsync(LoadExtensionSql, CancellationToken.None).ConfigureAwait(false);
            _workers = await PgConnectionSet<SearchSlot>.OpenAsync(options, _maxConcurrency, async connection =>
            {
                await connection.ExecuteAsync(LoadExtensionSql, CancellationToken.None).ConfigureAwait(false);
                return new SearchSlot(connection);
            }).ConfigureAwait(false);
        }
        catch
        {
            await _setup.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Through the connection string's own database: refuses a pool the server cannot hold, then
    /// creates the run database when it does not exist. The workers open eagerly, so the refusal comes
    /// by name before anything is created rather than mid-open.
    /// </summary>
    private async Task PrepareServerAsync()
    {
        await using var connection = await PgConnectionSet<SearchSlot>.ConnectAsync(_maintenanceOptions).ConfigureAwait(false);
        var row = (await connection.QueryAsync(
            "SELECT current_setting('max_connections')::int8, current_setting('max_connections')::int8 - current_setting('superuser_reserved_connections')::int8"
            + " - coalesce(current_setting('reserved_connections', true)::int8, 0) - (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')",
            CancellationToken.None).ConfigureAwait(false))[0];
        var free = row.Get<long>(1);
        if (free < _maxConcurrency)
            throw new PgVectorConnectionLimitException(_maxConcurrency, free, row.Get<long>(0));

        if (string.Equals(_maintenanceOptions.Database, _baseOptions.Database, StringComparison.Ordinal))
            return;
        if (PlainName().IsMatch(_baseOptions.Database) == false)
            throw new ArgumentException($"Database name '{_baseOptions.Database}' is not a plain identifier, so the run cannot create it.");
        var exists = await connection.QueryAsync("SELECT 1 FROM pg_database WHERE datname = $1", SqlParameters.Create(_baseOptions.Database), CancellationToken.None).ConfigureAwait(false);
        if (exists.Count == 0)
            await connection.ExecuteAsync($"CREATE DATABASE \"{_baseOptions.Database}\"", CancellationToken.None).ConfigureAwait(false);
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex PlainName();

    // An existing table built for another set would otherwise take a load of the wrong width.
    private async Task RequireColumnDimensionsAsync(PgConnection connection)
    {
        var rows = await connection.QueryAsync(
            $"SELECT atttypmod::int8 FROM pg_attribute WHERE attrelid = '{TableName}'::regclass AND attname = 'embedding'",
            CancellationToken.None).ConfigureAwait(false);
        var stored = rows[0].Get<long>(0);
        if (stored != _dimensions)
            throw new InvalidOperationException($"Table '{TableName}' stores vector({stored}); the set has {_dimensions} dimensions. Drop the table or use another database.");
    }

    private async Task<TransportResult> SearchAsync(VectorSearchOperation search, CancellationToken ct)
    {
        if (search.UseExactSearch)
            throw new NotSupportedException($"{nameof(PgVectorTransport)} serves exact search through {nameof(ExactSearchAsync)}, which proves the plan skips the index.");
        if (search.Quantization != VectorQuantization.None)
            throw new NotSupportedException($"{nameof(PgVectorTransport)} stores full float32 vectors; quantization {search.Quantization} is a separate row.");
        if (search.Effort is { } effort && SearchKnobs.Values.Contains(effort.Knob) == false)
            throw new NotSupportedException($"pgvector has no search-effort knob '{effort.Knob}'; its knobs are {string.Join(", ", SearchKnobs.Values.Select(k => $"'{k}'"))}.");

        await _ready.Value.ConfigureAwait(false);
        var sql = SearchSql(_metric, search.Filter);
        var parameters = SearchParameters(search.QueryVector, search.TopK, search.Filter);
        return await _workers!.UseAsync(async slot =>
        {
            var inForce = await slot.ApplyEffortAsync(search.Effort, ct).ConfigureAwait(false);
            var rows = await slot.Connection.QueryTypedAsync(sql, parameters, ct).ConfigureAwait(false);
            var ids = rows.Select(r => r.Get<string>(0)).ToArray();
            return new TransportResult(0, 0, resultCount: ids.Length) { NeighborIds = ids, EffortInForce = inForce };
        }, ct).ConfigureAwait(false);
    }

    private async Task<TransportResult> BulkInsertAsync(BulkInsertOperation<VectorRow> bulk, CancellationToken ct)
    {
        await _ready.Value.ConfigureAwait(false);
        return await _workers!.UseAsync(async slot =>
        {
            await using var importer = await slot.Connection.BeginBinaryImportAsync(BulkCopySql, ct).ConfigureAwait(false);
            foreach (var document in bulk.Documents)
            {
                await importer.StartRowAsync(ct).ConfigureAwait(false);
                await importer.WriteAsync(PgParameter.Create(PgType.Text, document.Id, PgParameterFormat.Binary), ct).ConfigureAwait(false);
                await importer.WriteAsync(VectorParameter(document.Document.Embedding), ct).ConfigureAwait(false);
                if (document.Document.Label is null)
                    await importer.WriteNullAsync(ct).ConfigureAwait(false);
                else
                    await importer.WriteAsync(PgParameter.Create(PgType.Text, document.Document.Label, PgParameterFormat.Binary), ct).ConfigureAwait(false);
            }

            await importer.CompleteAsync(ct).ConfigureAwait(false);
            return new TransportResult(0, 0);
        }, ct).ConfigureAwait(false);
    }

    private async Task<TransportResult> InsertAsync(InsertOperation<VectorRow> insert, CancellationToken ct)
    {
        await _ready.Value.ConfigureAwait(false);
        return await _workers!.UseAsync(async slot =>
        {
            var result = await slot.Connection.ExecuteTypedAsync(InsertSql, RowParameters(insert.Id, insert.Payload), ct).ConfigureAwait(false);
            return result.AffectedRows == 1
                ? new TransportResult(0, 0)
                : new TransportResult(0, 0, $"Insert of '{insert.Id}' affected {result.AffectedRows} rows.");
        }, ct).ConfigureAwait(false);
    }

    private PgParameter VectorParameter(float[] vector)
    {
        if (vector.Length != _dimensions)
            throw new ArgumentException($"The vector has {vector.Length} dimensions; the set has {_dimensions}.", nameof(vector));
        return PgParameter.Create(_vectorType, new PgVector(vector), PgParameterFormat.Binary);
    }

    private PgParameters SearchParameters(float[] query, int k, VectorFilter? filter)
    {
        if (k < 1)
            throw new ArgumentOutOfRangeException(nameof(k), k, "k must be positive.");
        var vector = VectorParameter(query);
        var limit = PgParameter.Create(PgType.Bigint, (long)k, PgParameterFormat.Binary);
        return filter is null
            ? PgParameters.Create(vector, limit)
            : PgParameters.Create(vector, limit, PgParameter.Create(PgType.Text, filter.Value, PgParameterFormat.Binary));
    }

    private PgParameters RowParameters(string id, VectorRow row) => PgParameters.Create(
        PgParameter.Create(PgType.Text, id, PgParameterFormat.Binary),
        VectorParameter(row.Embedding),
        row.Label is null
            ? new PgParameter(PgType.Text, SqlValue.Null, PgParameterFormat.Binary)
            : PgParameter.Create(PgType.Text, row.Label, PgParameterFormat.Binary));

    /// <summary>
    /// One worker connection and the search settings it last applied. A slot is used by one operation
    /// at a time, so its state needs no lock.
    /// </summary>
    private sealed class SearchSlot(PgConnection connection) : IAsyncDisposable
    {
        private readonly Dictionary<string, int> _applied = new(StringComparer.Ordinal);
        private int? _serverValue;

        public PgConnection Connection { get; } = connection;

        /// <summary>
        /// Sets the session value when the operation carries one; otherwise restores the server values
        /// and reads <c>hnsw.ef_search</c> back. Returns the effort the next query runs under.
        /// </summary>
        public async Task<SearchEffort> ApplyEffortAsync(SearchEffort? effort, CancellationToken ct)
        {
            if (effort is { } requested)
            {
                if (_applied.TryGetValue(requested.Knob, out var current) == false || current != requested.WholeValue)
                {
                    await Connection.QueryTypedAsync(ApplyEffortSql, PgParameters.Create(
                        PgParameter.Create(PgType.Text, requested.Knob, PgParameterFormat.Binary),
                        PgParameter.Create(PgType.Text, requested.WholeValue.ToString(CultureInfo.InvariantCulture), PgParameterFormat.Binary)), ct).ConfigureAwait(false);
                    _applied[requested.Knob] = requested.WholeValue;
                }
                return requested;
            }

            foreach (var knob in _applied.Keys)
                await Connection.ExecuteAsync($"RESET {knob}", ct).ConfigureAwait(false);
            _applied.Clear();
            _serverValue ??= int.Parse((await Connection.QueryAsync(ShowEffortSql, ct).ConfigureAwait(false))[0].Get<string>(0), CultureInfo.InvariantCulture);
            return SearchEffort.PgVector(_serverValue.Value);
        }

        public ValueTask DisposeAsync() => Connection.DisposeAsync();
    }
}
