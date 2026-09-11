using System.Threading.Channels;
using Apex.PgClient;
using Apex.SqlClient;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;

namespace RavenBench.Core.Transport;

/// <summary>
/// Drives the ycsb document operations against PostgreSQL through Apex.PgClient. It implements
/// <see cref="IYcsbTransport"/> alone, because PostgreSQL exposes no SNMP, no RavenDB license type
/// and no RavenDB calibration endpoint, and the driver does not hand the transport the socket, so
/// byte counts are not wire-accurate. Measured operations draw from a plain connection set sized
/// to the scenario's largest concurrency; each connection prepares its three fixed statements once
/// and reuses them. Apex's pipelining pool and its automatic prepared-statement cache are not used.
/// </summary>
public sealed class PostgresYcsbTransport : IYcsbTransport
{
    /// <summary>Scenario target name for PostgreSQL.</summary>
    public const string Target = "postgresql";

    /// <summary>The durability setting a PostgreSQL run applies and records.</summary>
    public const string DurabilitySetting = "synchronous_commit";

    /// <summary>The value of <see cref="DurabilitySetting"/> every write path runs under.</summary>
    public const string DurabilityValue = "on";

    /// <summary>Reads the stored jsonb as text, addressed by id.</summary>
    internal const string ReadSql = "SELECT doc::text FROM ycsb WHERE id = $1";

    /// <summary>One row per insert; the payload reaches the server as jsonb through the cast.</summary>
    internal const string InsertSql = "INSERT INTO ycsb (id, doc) VALUES ($1, $2::jsonb)";

    /// <summary>
    /// A one-field <c>jsonb_set</c>. It is never <c>SET doc = $2</c>, which would replace the whole
    /// document and drift the other nine fields.
    /// </summary>
    internal const string UpdateSql =
        "UPDATE ycsb SET doc = jsonb_set(doc, ARRAY[$2], to_jsonb($3::text)) WHERE id = $1";

    /// <summary>The load path: binary COPY of the batch, not one single-document insert per row.</summary>
    internal const string BulkCopySql = "COPY ycsb (id, doc) FROM STDIN (FORMAT BINARY)";

    /// <summary>Idempotent schema setup for the fixed table.</summary>
    internal const string CreateTableSql = "CREATE TABLE IF NOT EXISTS ycsb (id text primary key, doc jsonb)";

    /// <summary>Counts the seeded documents whose id starts with the prefix.</summary>
    internal const string CountSql = "SELECT count(*)::int8 FROM ycsb WHERE starts_with(id, $1)";

    /// <summary>Upsert used by <see cref="PutAsync"/> outside the measured path.</summary>
    internal const string PutSql =
        "INSERT INTO ycsb (id, doc) VALUES ($1, $2::jsonb) ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc";

    /// <summary>The durability parity setting applied on every connection the transport opens.</summary>
    internal const string SetDurabilitySql = "SET " + DurabilitySetting + " = " + DurabilityValue;

    /// <summary>Reads back the applied durability; the state tests use it as evidence.</summary>
    internal const string ReadDurabilitySql = "SHOW " + DurabilitySetting;

    private readonly PgConnectOptions _options;
    private readonly int _maxConcurrency;
    private readonly Lazy<Task> _ready;
    private PgConnection? _setup;
    private Channel<MeasuredConnection>? _measured;
    private string _productName = string.Empty;
    private string _serverVersion = string.Empty;
    private bool _disposed;

    /// <param name="connectionString">
    /// The full connection string; the transport never records it verbatim. It supplies the host,
    /// port, credentials and options.
    /// </param>
    /// <param name="databaseName">
    /// The database the run addresses. It is the authority for the database; the connection string
    /// supplies the host, port, credentials and options.
    /// </param>
    /// <param name="maxConcurrency">
    /// The largest concurrency the resolved scenario reaches. The measured connection set is sized
    /// to exactly this many connections, so the set follows the scenario rather than a constant.
    /// </param>
    public PostgresYcsbTransport(string connectionString, string databaseName, int maxConcurrency)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("A PostgreSQL connection string is required.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("A PostgreSQL database name is required.", nameof(databaseName));
        if (maxConcurrency < 1)
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency), maxConcurrency, "The concurrency ceiling must be positive.");

        RecordedEndpoint = ConnectionStringRedaction.Redact(connectionString);
        _options = ResolveOptions(connectionString, databaseName);
        _maxConcurrency = maxConcurrency;
        _ready = new Lazy<Task>(InitializeAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    /// <summary>The product the server reports at connect, never a literal.</summary>
    public string ProductName => _productName.Length > 0
        ? _productName
        : throw new InvalidOperationException("The PostgreSQL transport has not connected. Call EnsureDatabaseExistsAsync before reading ProductName.");

    /// <summary>
    /// The endpoint safe to record in a result: the connection string with its password replaced by
    /// a fixed token. The transport still receives the full string.
    /// </summary>
    public string RecordedEndpoint { get; }

    /// <summary>The driver does not expose the socket, so a reported byte count is not a wire size.</summary>
    public bool ReportsWireBytes => false;

    /// <summary>The concurrency ceiling the measured connection set was sized to.</summary>
    internal int MeasuredConnectionCount => _maxConcurrency;

    /// <inheritdoc />
    public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => op switch
    {
        ReadOperation read => GuardedAsync(() => ReadAsync(read, ct), ct),
        InsertOperation<string> insert => GuardedAsync(() => InsertAsync(insert, ct), ct),
        UpdateFieldOperation update => GuardedAsync(() => UpdateFieldAsync(update, ct), ct),
        BulkInsertOperation<string> bulk => GuardedAsync(() => BulkInsertAsync(bulk, ct), ct),
        _ => throw new NotSupportedException($"{nameof(PostgresYcsbTransport)} cannot execute operation type {op.GetType().Name}.")
    };

    /// <summary>
    /// Writes one document outside the measured path under the same synchronous_commit=on setting
    /// as every other write. The write upserts, so a caller can preload an existing id.
    /// </summary>
    public async Task PutAsync<T>(string id, T document)
    {
        if (document is not string payload)
            throw new NotSupportedException($"{nameof(PostgresYcsbTransport)} stores the ycsb document as a JSON string, not {typeof(T).Name}.");

        await _ready.Value.ConfigureAwait(false);
        await _setup!.ExecuteAsync(PutSql, SqlParameters.Create(id, payload), CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the fixed table idempotently: a second call succeeds. The name must be the database
    /// the transport was built for; the connection string already selects it.
    /// </summary>
    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("A PostgreSQL database name is required.", nameof(databaseName));
        if (string.Equals(databaseName, _options.Database, StringComparison.Ordinal) == false)
            throw new ArgumentException(
                $"The transport was built for database '{_options.Database}' but the run asked for '{databaseName}'.",
                nameof(databaseName));

        await _ready.Value.ConfigureAwait(false);
        await _setup!.ExecuteAsync(CreateTableSql, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Counts the documents whose <c>id</c> starts with the prefix. The ycsb sequence reads the
    /// keyspace back through here before it issues an operation.
    /// </summary>
    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        await _ready.Value.ConfigureAwait(false);
        var rows = await _setup!
            .QueryAsync(CountSql, SqlParameters.Create(idPrefix), CancellationToken.None)
            .ConfigureAwait(false);
        return rows[0].Get<long>(0);
    }

    /// <summary>The version string the server reports at connect, never a literal.</summary>
    public async Task<string> GetServerVersionAsync()
    {
        await _ready.Value.ConfigureAwait(false);
        return _serverVersion;
    }

    /// <summary>Closes the setup connection and every connection in the measured set.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        if (_ready.IsValueCreated == false || _ready.Value.IsCompletedSuccessfully == false)
            return;

        DisposeConnectionsAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Runs one body against a connection from the measured set. The state tests use it to read the
    /// documented jsonb accessor and the applied durability from a connection the measured path uses.
    /// </summary>
    internal async Task<T> ReadWithMeasuredConnectionAsync<T>(Func<PgConnection, Task<T>> body)
    {
        ArgumentNullException.ThrowIfNull(body);

        var slot = await RentAsync(CancellationToken.None).ConfigureAwait(false);
        var healthy = true;
        try
        {
            return await body(slot.Connection).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            healthy = false;
            throw;
        }
        finally
        {
            await ReturnAsync(slot, healthy).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Builds the connect options from the caller's string, then sets the run's database on them.
    /// The run's <c>--database</c> is the authority, so a connection string that omits one works
    /// even though the driver defaults an absent name to <c>db</c>. The pinned driver's automatic
    /// prepared-statement cache is forced off here: this transport prepares explicitly.
    /// </summary>
    internal static PgConnectOptions ResolveOptions(string connectionString, string databaseName) =>
        PgConnectOptions.Parse(connectionString) with
        {
            CachePreparedStatements = false,
            Database = databaseName
        };

    private async Task InitializeAsync()
    {
        var opened = new List<MeasuredConnection>(_maxConcurrency);
        try
        {
            _setup = await PgClient.ConnectAsync(_options, CancellationToken.None).ConfigureAwait(false);
            await ApplyDurabilityAsync(_setup).ConfigureAwait(false);

            _productName = RequireMetadata(_setup.DatabaseMetadata.ProductName, "product name");
            _serverVersion = RequireMetadata(_setup.DatabaseMetadata.FullVersion, "version");

            // The prepared statements reference the fixed table, so it must exist before they are
            // prepared. EnsureDatabaseExistsAsync re-runs the same idempotent create.
            await _setup.ExecuteAsync(CreateTableSql, CancellationToken.None).ConfigureAwait(false);

            var channel = Channel.CreateBounded<MeasuredConnection>(_maxConcurrency);
            for (int i = 0; i < _maxConcurrency; i++)
            {
                var slot = await OpenMeasuredConnectionAsync().ConfigureAwait(false);
                opened.Add(slot);
                channel.Writer.TryWrite(slot);
            }

            _measured = channel;
        }
        catch
        {
            foreach (var slot in opened)
                await slot.DisposeAsync().ConfigureAwait(false);
            if (_setup is not null)
                await _setup.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Opens one plain connection and prepares the three fixed statements on it. The statements are
    /// prepared once per connection and reused, because the preview driver's automatic cache
    /// mis-reads the server's ParameterDescription against PostgreSQL 17.
    /// </summary>
    private async Task<MeasuredConnection> OpenMeasuredConnectionAsync()
    {
        var connection = await PgClient.ConnectAsync(_options, CancellationToken.None).ConfigureAwait(false);
        try
        {
            await ApplyDurabilityAsync(connection).ConfigureAwait(false);
            var read = await connection.PrepareAsync(ReadSql, CancellationToken.None).ConfigureAwait(false);
            var insert = await connection.PrepareAsync(InsertSql, CancellationToken.None).ConfigureAwait(false);
            var update = await connection.PrepareAsync(UpdateSql, CancellationToken.None).ConfigureAwait(false);
            return new MeasuredConnection(connection, read, insert, update);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task<TransportResult> ReadAsync(ReadOperation read, CancellationToken ct)
    {
        var slot = await RentAsync(ct).ConfigureAwait(false);
        var healthy = true;
        try
        {
            var rows = await slot.Read.QueryAsync(SqlParameters.Create(read.Id), ct).ConfigureAwait(false);
            return rows.Count == 0
                ? new TransportResult(0, 0, $"Document '{read.Id}' was not found.")
                : new TransportResult(0, 0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            healthy = false;
            throw;
        }
        finally
        {
            await ReturnAsync(slot, healthy).ConfigureAwait(false);
        }
    }

    private async Task<TransportResult> InsertAsync(InsertOperation<string> insert, CancellationToken ct)
    {
        var slot = await RentAsync(ct).ConfigureAwait(false);
        var healthy = true;
        try
        {
            var result = await slot.Insert
                .ExecuteAsync(SqlParameters.Create(insert.Id, insert.Payload), ct)
                .ConfigureAwait(false);

            return result.AffectedRows == 1
                ? new TransportResult(0, 0)
                : new TransportResult(0, 0, $"Insert of '{insert.Id}' affected {result.AffectedRows} rows.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            healthy = false;
            throw;
        }
        finally
        {
            await ReturnAsync(slot, healthy).ConfigureAwait(false);
        }
    }

    private async Task<TransportResult> UpdateFieldAsync(UpdateFieldOperation update, CancellationToken ct)
    {
        var slot = await RentAsync(ct).ConfigureAwait(false);
        var healthy = true;
        try
        {
            var result = await slot.Update
                .ExecuteAsync(SqlParameters.Create(update.Id, update.FieldName, update.Value), ct)
                .ConfigureAwait(false);

            // The server reports a missing match as a successful no-op; a run that updates a key it
            // never loaded is an error, not throughput.
            return result.AffectedRows == 0
                ? new TransportResult(0, 0, $"Document '{update.Id}' was not found for field update.")
                : new TransportResult(0, 0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            healthy = false;
            throw;
        }
        finally
        {
            await ReturnAsync(slot, healthy).ConfigureAwait(false);
        }
    }

    private async Task<TransportResult> BulkInsertAsync(BulkInsertOperation<string> bulk, CancellationToken ct)
    {
        var slot = await RentAsync(ct).ConfigureAwait(false);
        var healthy = true;
        try
        {
            await using var importer = await slot.Connection
                .BeginBinaryImportAsync(BulkCopySql, ct)
                .ConfigureAwait(false);

            foreach (var document in bulk.Documents)
            {
                await importer.StartRowAsync(ct).ConfigureAwait(false);
                await importer.WriteAsync(PgParameter.Create(PgType.Text, document.Id), ct).ConfigureAwait(false);
                await importer.WriteAsync(PgParameter.Create(PgType.Jsonb, document.Document), ct).ConfigureAwait(false);
            }

            await importer.CompleteAsync(ct).ConfigureAwait(false);
            return new TransportResult(0, 0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            healthy = false;
            throw;
        }
        finally
        {
            await ReturnAsync(slot, healthy).ConfigureAwait(false);
        }
    }

    private async Task<MeasuredConnection> RentAsync(CancellationToken ct)
    {
        await _ready.Value.ConfigureAwait(false);
        return await _measured!.Reader.ReadAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns a healthy connection to the set. A connection an operation failed on is disposed and
    /// replaced, because the driver closes it on a protocol-level error and the next operation on
    /// it would otherwise fail for the wrong reason; if the server is gone, the set is closed so a
    /// later rent fails fast instead of blocking on an empty set.
    /// </summary>
    private async Task ReturnAsync(MeasuredConnection slot, bool healthy)
    {
        if (healthy)
        {
            _measured!.Writer.TryWrite(slot);
            return;
        }

        // Disposing a connection the driver already closed can itself throw; that must not replace
        // the operation's own error with the teardown failure.
        try { await slot.DisposeAsync().ConfigureAwait(false); } catch (Exception) { }
        try
        {
            _measured!.Writer.TryWrite(await OpenMeasuredConnectionAsync().ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            _measured!.Writer.TryComplete(ex);
        }
    }

    private static Task ApplyDurabilityAsync(PgConnection connection) =>
        connection.ExecuteAsync(SetDurabilitySql, CancellationToken.None).AsTask();

    private static string RequireMetadata(string value, string what) =>
        string.IsNullOrWhiteSpace(value) || string.Equals(value, "unknown", StringComparison.OrdinalIgnoreCase)
            ? throw new InvalidOperationException($"The PostgreSQL server reported no {what} at connect.")
            : value;

    private static async Task<TransportResult> GuardedAsync(Func<Task<TransportResult>> body, CancellationToken ct)
    {
        try
        {
            return await body().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return TransportResult.CancelledResult;
        }
        catch (Exception ex)
        {
            return TransportResult.FromException(ex, ct);
        }
    }

    private async ValueTask DisposeConnectionsAsync()
    {
        if (_measured is not null)
        {
            while (_measured.Reader.TryRead(out var slot))
                await slot.DisposeAsync().ConfigureAwait(false);
        }

        if (_setup is not null)
            await _setup.DisposeAsync().ConfigureAwait(false);
    }

    private sealed record MeasuredConnection(
        PgConnection Connection,
        ISqlPreparedStatement Read,
        ISqlPreparedStatement Insert,
        ISqlPreparedStatement Update) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await Read.DisposeAsync().ConfigureAwait(false);
            await Insert.DisposeAsync().ConfigureAwait(false);
            await Update.DisposeAsync().ConfigureAwait(false);
            await Connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
