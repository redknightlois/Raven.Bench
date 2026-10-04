using System.Reflection;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using RavenBench.Core.Workload;

namespace RavenBench.Core.Transport;

/// <summary>
/// Drives the ycsb document operations against PostgreSQL through Npgsql, the driver most .NET
/// applications use. It runs the same SQL on the same table as <see cref="PostgresYcsbTransport"/>,
/// so a document one transport writes, the other reads back. Connections come from Npgsql's own pool,
/// capped at the scenario's concurrency ceiling, and every measured statement is prepared. In the
/// pass-through mode a document travels as its stored JSON; in the entity mode it is mapped to and
/// from <see cref="YcsbRecord"/>, the entity the RavenDB client-entity mode uses.
/// </summary>
public sealed class NpgsqlYcsbTransport : IYcsbTransport, IReportsStorageSize, IInspectsStoredDocuments
{
    /// <summary>The client library a result from this transport records.</summary>
    public const string ClientLibraryName = "Npgsql";

    /// <summary>The version of the loaded Npgsql assembly, without the source revision suffix.</summary>
    public static string ClientLibraryVersion { get; } = LoadedNpgsqlVersion();

    private readonly NpgsqlDataSource _dataSource;
    private readonly string _database;
    private readonly bool _mapEntities;
    private readonly Lazy<Task> _ready;
    private string _productName = string.Empty;
    private string _serverVersion = string.Empty;

    /// <param name="connectionString">The PostgreSQL connection string in the form the Apex transport accepts; it is never recorded verbatim.</param>
    /// <param name="databaseName">The database the run addresses; it overrides any database the connection string names.</param>
    /// <param name="maxConcurrency">The largest concurrency the scenario reaches; it is the pool maximum.</param>
    /// <param name="mapEntities">True maps documents to and from <see cref="YcsbRecord"/>; false passes the stored JSON through.</param>
    public NpgsqlYcsbTransport(string connectionString, string databaseName, int maxConcurrency, bool mapEntities)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("A PostgreSQL connection string is required.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("A PostgreSQL database name is required.", nameof(databaseName));
        if (maxConcurrency < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), maxConcurrency, "The concurrency ceiling must be positive.");

        RecordedEndpoint = ConnectionStringRedaction.Redact(connectionString);
        _database = databaseName;
        _mapEntities = mapEntities;
        _dataSource = NpgsqlDataSource.Create(BuildConnectionString(connectionString, databaseName, maxConcurrency));
        _ready = new Lazy<Task>(InitializeAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>The product the server names in its version banner, never a literal.</summary>
    public string ProductName => _productName.Length > 0
        ? _productName
        : throw new InvalidOperationException("The Npgsql transport has not connected. Call EnsureDatabaseExistsAsync before reading ProductName.");

    /// <summary>The connection string with its password replaced by a fixed token.</summary>
    public string RecordedEndpoint { get; }

    /// <summary>The driver does not expose the socket, so a reported byte count is not a wire size.</summary>
    public bool ReportsWireBytes => false;

    /// <inheritdoc />
    public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => op switch
    {
        ReadOperation read => TransportResult.GuardedAsync(() => ReadAsync(read.Id, ct), ct),
        InsertOperation<string> insert => TransportResult.GuardedAsync(() => InsertAsync(insert.Id, ToStoredJson(insert.Payload), ct), ct),
        InsertOperation<YcsbRecord> insert => TransportResult.GuardedAsync(() => InsertAsync(insert.Id, ToStoredJson(insert.Payload), ct), ct),
        UpdateFieldOperation update => TransportResult.GuardedAsync(() => UpdateFieldAsync(update, ct), ct),
        BulkInsertOperation<string> bulk => TransportResult.GuardedAsync(() => BulkInsertAsync(bulk.Documents.Select(d => (d.Id, ToStoredJson(d.Document))), ct), ct),
        BulkInsertOperation<YcsbRecord> bulk => TransportResult.GuardedAsync(() => BulkInsertAsync(bulk.Documents.Select(d => (d.Id, ToStoredJson(d.Document))), ct), ct),
        _ => throw new NotSupportedException($"{nameof(NpgsqlYcsbTransport)} cannot execute operation type {op.GetType().Name}.")
    };

    /// <summary>Upserts one document outside the measured path under synchronous_commit=on.</summary>
    public async Task PutAsync<T>(string id, T document)
    {
        var json = document switch
        {
            string s => ToStoredJson(s),
            YcsbRecord r => ToStoredJson(r),
            _ => throw new NotSupportedException($"{nameof(NpgsqlYcsbTransport)} stores a JSON string or a {nameof(YcsbRecord)}, not {typeof(T).Name}.")
        };

        await ExecuteNonQueryAsync(PostgresYcsbTransport.PutSql, CancellationToken.None, Text(id), Text(json)).ConfigureAwait(false);
    }

    /// <summary>Creates the fixed table idempotently. The name must be the database the transport was built for.</summary>
    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("A PostgreSQL database name is required.", nameof(databaseName));
        if (string.Equals(databaseName, _database, StringComparison.Ordinal) == false)
            throw new ArgumentException($"The transport was built for database '{_database}' but the run asked for '{databaseName}'.", nameof(databaseName));

        await _ready.Value.ConfigureAwait(false);
    }

    /// <summary>Counts the documents whose id starts with the prefix.</summary>
    public async Task<long> GetDocumentCountAsync(string idPrefix) =>
        (long)(await ScalarAsync(PostgresYcsbTransport.CountSql, Text(idPrefix)).ConfigureAwait(false));

    /// <inheritdoc />
    public string StorageSizeMetricName => PostgresYcsbTransport.StorageSizeMetric;

    /// <inheritdoc />
    public async Task<long> GetStorageSizeBytesAsync() =>
        (long)(await ScalarAsync(PostgresYcsbTransport.StorageSizeSql).ConfigureAwait(false));

    /// <summary>The server_version the server reports, never a literal.</summary>
    public async Task<string> GetServerVersionAsync()
    {
        await _ready.Value.ConfigureAwait(false);
        return _serverVersion;
    }

    /// <summary>The synchronous_commit value a pooled connection runs under, read back from the server.</summary>
    internal async Task<string> ReadAppliedDurabilityAsync() =>
        (string)(await ScalarAsync(PostgresYcsbTransport.ReadDurabilitySql).ConfigureAwait(false));

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>?> ReadStoredFieldsAsync(string id, CancellationToken ct)
    {
        var json = await ReadStoredJsonAsync(id, ct).ConfigureAwait(false);
        return json is null ? null : PostgresYcsbTransport.ParseStoredFields(json);
    }

    /// <inheritdoc />
    public Task DeleteStoredDocumentAsync(string id, CancellationToken ct) =>
        ExecuteNonQueryAsync(PostgresYcsbTransport.DeleteSql, ct, Text(id));

    /// <summary>The stored JSON of one document as the server returns it, or null when the id is absent.</summary>
    internal async Task<string?> ReadStoredJsonAsync(string id, CancellationToken ct)
    {
        await _ready.Value.ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = await PrepareAsync(connection, PostgresYcsbTransport.ReadSql, ct, Text(id)).ConfigureAwait(false);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await reader.ReadAsync(ct).ConfigureAwait(false) ? reader.GetString(0) : null;
    }

    /// <summary>Closes every pooled connection.</summary>
    public void Dispose() => _dataSource.Dispose();

    /// <summary>
    /// Translates the connection string through the Apex parser, so both transports accept the same
    /// strings and address the same server, database and session options. synchronous_commit and
    /// every option the string carries travel in the startup packet: they become the session
    /// defaults, so the pool's reset on connection return keeps them.
    /// </summary>
    internal static string BuildConnectionString(string connectionString, string databaseName, int maxPoolSize)
    {
        var options = PostgresYcsbTransport.ResolveOptions(connectionString, databaseName);
        var sessionOptions = options.Properties
            .Select(p => $"-c {p.Key}={p.Value}")
            .Prepend($"-c {PostgresYcsbTransport.DurabilitySetting}={PostgresYcsbTransport.DurabilityValue}");

        return new NpgsqlConnectionStringBuilder
        {
            Host = options.Host,
            Port = options.Port,
            Username = options.Username,
            Password = options.Password,
            Database = options.Database,
            SslMode = Enum.Parse<SslMode>(options.SslMode.ToString(), ignoreCase: true),
            Timeout = (int)Math.Ceiling(options.ConnectTimeout.TotalSeconds),
            MinPoolSize = 0,
            MaxPoolSize = maxPoolSize,
            Options = string.Join(' ', sessionOptions)
        }.ConnectionString;
    }

    private async Task InitializeAsync()
    {
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        _serverVersion = PostgresYcsbTransport.RequireMetadata(connection.ServerVersion, "version");

        await using (var durability = new NpgsqlCommand(PostgresYcsbTransport.ReadDurabilitySql, connection))
        {
            var applied = (string?)await durability.ExecuteScalarAsync().ConfigureAwait(false);
            if (applied != PostgresYcsbTransport.DurabilityValue)
                throw new InvalidOperationException(
                    $"The Npgsql connection runs with {PostgresYcsbTransport.DurabilitySetting}='{applied}', not '{PostgresYcsbTransport.DurabilityValue}'.");
        }

        await using (var banner = new NpgsqlCommand("SELECT version()", connection))
        {
            var text = (string)(await banner.ExecuteScalarAsync().ConfigureAwait(false))!;
            _productName = PostgresYcsbTransport.RequireMetadata(text.Split(' ', 2)[0], "product name");
        }

        await using var create = new NpgsqlCommand(PostgresYcsbTransport.CreateTableSql, connection);
        await create.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private async Task<TransportResult> ReadAsync(string id, CancellationToken ct)
    {
        var json = await ReadStoredJsonAsync(id, ct).ConfigureAwait(false);
        if (json is null)
            return TransportResult.DocumentNotFound(id);

        if (_mapEntities)
            _ = JsonSerializer.Deserialize<YcsbRecord>(json) ?? throw new JsonException($"Document '{id}' deserialized to null.");

        return new TransportResult(0, 0);
    }

    private async Task<TransportResult> InsertAsync(string id, string json, CancellationToken ct)
    {
        var affected = await ExecuteNonQueryAsync(PostgresYcsbTransport.InsertSql, ct, Text(id), Text(json)).ConfigureAwait(false);
        return affected == 1
            ? new TransportResult(0, 0)
            : new TransportResult(0, 0, $"Insert of '{id}' affected {affected} rows.");
    }

    private async Task<TransportResult> UpdateFieldAsync(UpdateFieldOperation update, CancellationToken ct)
    {
        var affected = await ExecuteNonQueryAsync(PostgresYcsbTransport.UpdateSql, ct, Text(update.Id), Text(update.FieldName), Text(update.Value)).ConfigureAwait(false);
        return affected == 0
            ? TransportResult.DocumentNotFound(update.Id, "field update")
            : new TransportResult(0, 0);
    }

    private async Task<TransportResult> BulkInsertAsync(IEnumerable<(string Id, string Json)> documents, CancellationToken ct)
    {
        await _ready.Value.ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var importer = await connection.BeginBinaryImportAsync(PostgresYcsbTransport.BulkCopySql, ct).ConfigureAwait(false);

        foreach (var (id, json) in documents)
        {
            await importer.StartRowAsync(ct).ConfigureAwait(false);
            await importer.WriteAsync(id, NpgsqlDbType.Text, ct).ConfigureAwait(false);
            await importer.WriteAsync(json, NpgsqlDbType.Jsonb, ct).ConfigureAwait(false);
        }

        await importer.CompleteAsync(ct).ConfigureAwait(false);
        return new TransportResult(0, 0);
    }

    /// <summary>The JSON text the table stores: a pass-through string, or the entity's serialized form.</summary>
    private string ToStoredJson(string json) => _mapEntities
        ? ToStoredJson(JsonSerializer.Deserialize<YcsbRecord>(json) ?? throw new JsonException("A ycsb document deserialized to null."))
        : json;

    private string ToStoredJson(YcsbRecord record) => _mapEntities
        ? JsonSerializer.Serialize(record)
        : throw new NotSupportedException($"The pass-through {nameof(NpgsqlYcsbTransport)} writes the stored JSON, not a {nameof(YcsbRecord)}.");

    private async Task<int> ExecuteNonQueryAsync(string sql, CancellationToken ct, params NpgsqlParameter[] parameters)
    {
        await _ready.Value.ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = await PrepareAsync(connection, sql, ct, parameters).ConfigureAwait(false);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<object> ScalarAsync(string sql, params NpgsqlParameter[] parameters)
    {
        await _ready.Value.ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync().ConfigureAwait(false);
        await using var command = await PrepareAsync(connection, sql, CancellationToken.None, parameters).ConfigureAwait(false);
        return await command.ExecuteScalarAsync().ConfigureAwait(false)
            ?? throw new InvalidOperationException($"'{sql}' returned no value.");
    }

    /// <summary>
    /// Prepares the statement on the pooled physical connection. Npgsql keeps a prepared statement
    /// on its connection across pool returns, so a repeat prepare of the same SQL is a local lookup.
    /// </summary>
    private static async Task<NpgsqlCommand> PrepareAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        await command.PrepareAsync(ct).ConfigureAwait(false);
        return command;
    }

    // Unnamed, so Npgsql binds them to the shared SQL's positional $n placeholders.
    private static NpgsqlParameter Text(string value) => new() { NpgsqlDbType = NpgsqlDbType.Text, Value = value };

    private static string LoadedNpgsqlVersion()
    {
        var assembly = typeof(NpgsqlDataSource).Assembly;
        var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? throw new InvalidOperationException("The loaded Npgsql assembly carries no informational version.");
        return informational.Split('+', 2)[0];
    }
}
