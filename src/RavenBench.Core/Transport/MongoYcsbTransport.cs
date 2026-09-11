using System.Text.RegularExpressions;
using MongoDB.Bson;
using MongoDB.Driver;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;

namespace RavenBench.Core.Transport;

/// <summary>
/// Drives the ycsb document operations against MongoDB Community and Microsoft DocumentDB through
/// the official MongoDB .NET driver. One transport serves both targets: they speak the same wire
/// protocol, so only the reported product name differs. It implements <see cref="IYcsbTransport"/>
/// alone, because neither product exposes SNMP, a license type or a RavenDB calibration endpoint,
/// and the driver hides the socket, so byte counts are not wire-accurate.
/// </summary>
public sealed class MongoYcsbTransport : IYcsbTransport
{
    /// <summary>Scenario target name for MongoDB Community.</summary>
    public const string MongoDbTarget = "mongodb";

    /// <summary>Scenario target name for Microsoft DocumentDB.</summary>
    public const string DocumentDbTarget = "documentdb";

    /// <summary>Product name recorded by a <see cref="MongoDbTarget"/> run.</summary>
    public const string MongoDbProductName = "MongoDB Community";

    /// <summary>Product name recorded by a <see cref="DocumentDbTarget"/> run.</summary>
    public const string DocumentDbProductName = "DocumentDB";

    /// <summary>The collection that holds the seeded ycsb documents.</summary>
    internal const string CollectionName = "ycsb";

    // The driver's own insecure-TLS option, which the shell/Java tlsAllowInvalidCertificates
    // spelling does not reach.
    private const string DriverInsecureTlsOption = "tlsInsecure";

    /// <summary>
    /// The journaled concern every write path applies: acknowledged on the primary and flushed to
    /// the on-disk journal before it returns. Every write path waits for the journal.
    /// </summary>
    internal static readonly WriteConcern DurableWriteConcern = WriteConcern.W1.With(journal: true);

    private readonly IMongoClient _client;
    private readonly IMongoDatabase _database;
    private readonly IMongoCollection<BsonDocument> _collection;

    /// <param name="connectionString">The full connection string; the transport never records it verbatim.</param>
    /// <param name="databaseName">The database the run addresses; the collection lives in it.</param>
    /// <param name="target">
    /// The scenario target, <c>mongodb</c> or <c>documentdb</c>. The server reports no product name,
    /// so the selected target names the product in the result. Any other value is rejected.
    /// </param>
    public MongoYcsbTransport(string connectionString, string databaseName, string target)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("A Mongo connection string is required.", nameof(connectionString));
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("A Mongo database name is required.", nameof(databaseName));

        ProductName = target switch
        {
            _ when string.Equals(target, MongoDbTarget, StringComparison.OrdinalIgnoreCase) => MongoDbProductName,
            _ when string.Equals(target, DocumentDbTarget, StringComparison.OrdinalIgnoreCase) => DocumentDbProductName,
            _ => throw new ArgumentException(
                $"Unknown Mongo target '{target}'. Known targets are '{MongoDbTarget}' and '{DocumentDbTarget}'.", nameof(target))
        };

        RecordedEndpoint = RedactConnectionString(connectionString);

        var settings = MongoClientSettings.FromConnectionString(NormalizeConnectionString(connectionString));
        // Every write path inherits the concern from the client, so no path can drift from j:true.
        settings.WriteConcern = DurableWriteConcern;

        _client = new MongoClient(settings);
        _database = _client.GetDatabase(databaseName);
        _collection = _database.GetCollection<BsonDocument>(CollectionName);
    }

    /// <summary>The product the target actually ran, supplied because the server reports no product name.</summary>
    public string ProductName { get; }

    /// <summary>
    /// The endpoint safe to record in a result: the connection string with its password replaced by
    /// a fixed token. The transport still receives the full string.
    /// </summary>
    public string RecordedEndpoint { get; }

    /// <summary>The driver hides the socket, so a reported byte count is not a wire size.</summary>
    public bool ReportsWireBytes => false;

    /// <summary>The write concern every write path applies; exposed so a test can pin j:true.</summary>
    internal WriteConcern AppliedWriteConcern => _client.Settings.WriteConcern;

    /// <summary>The collection the tests read stored documents back from.</summary>
    internal IMongoCollection<BsonDocument> Documents => _collection;

    /// <inheritdoc />
    public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) => op switch
    {
        ReadOperation read => Guarded(() => ReadAsync(read, ct), ct),
        InsertOperation<string> insert => Guarded(() => InsertAsync(insert, ct), ct),
        UpdateFieldOperation update => Guarded(() => UpdateFieldAsync(update, ct), ct),
        BulkInsertOperation<string> bulk => Guarded(() => BulkInsertAsync(bulk, ct), ct),
        _ => throw new NotSupportedException($"{nameof(MongoYcsbTransport)} cannot execute operation type {op.GetType().Name}.")
    };

    /// <summary>
    /// Writes one document outside the measured path under the same journaled concern as every
    /// other write. The write upserts, so a caller can preload an id that already exists.
    /// </summary>
    public async Task PutAsync<T>(string id, T document)
    {
        if (document is not string payload)
            throw new NotSupportedException($"{nameof(MongoYcsbTransport)} stores the ycsb document as a JSON string, not {typeof(T).Name}.");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await _collection.ReplaceOneAsync(ReadFilter(id), ToStoredDocument(id, payload), new ReplaceOptions { IsUpsert = true }, cts.Token).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates the collection the run needs, idempotently: a second call on an existing collection
    /// succeeds. A server that reports the create as NamespaceExists is already in the target state.
    /// </summary>
    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        try
        {
            await _client.GetDatabase(databaseName).CreateCollectionAsync(CollectionName).ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.CodeName == "NamespaceExists" || ex.Code == 48)
        {
            // Expected: the collection already exists.
        }
    }

    /// <summary>
    /// Counts the documents whose <c>_id</c> starts with the prefix. The ycsb sequence reads the
    /// keyspace back through here before it issues an operation.
    /// </summary>
    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        var filter = Builders<BsonDocument>.Filter.Regex("_id", new BsonRegularExpression("^" + Regex.Escape(idPrefix)));
        return await _collection.CountDocumentsAsync(filter).ConfigureAwait(false);
    }

    /// <summary>
    /// The <c>version</c> string the server reports for <c>buildInfo</c>, so a result names the
    /// server that produced it rather than a literal.
    /// </summary>
    public async Task<string> GetServerVersionAsync()
    {
        var info = await _database.RunCommandAsync<BsonDocument>(new BsonDocument("buildInfo", 1)).ConfigureAwait(false);
        if (info.TryGetValue("version", out var version) && version.IsString)
            return version.AsString;

        throw new InvalidOperationException("The Mongo server's buildInfo response carries no version string.");
    }

    public void Dispose()
    {
        // The pinned driver keeps its cluster in a process-wide registry and exposes no dispose on
        // the client, so there is no per-run resource to release here.
    }

    /// <summary>
    /// The read filter: a document is addressed by the id the ycsb workloads carry, stored under
    /// <c>_id</c>.
    /// </summary>
    internal static FilterDefinition<BsonDocument> ReadFilter(string id) =>
        Builders<BsonDocument>.Filter.Eq("_id", id);

    /// <summary>
    /// The document an insert or a load stores: the id under <c>_id</c> and the ten generated
    /// fields at the top level. The stored form is BSON, so <c>_id</c> and key order are the only
    /// differences from the JSON payload.
    /// </summary>
    internal static BsonDocument ToStoredDocument(string id, string payloadJson)
    {
        var document = new BsonDocument("_id", id);
        document.AddRange(BsonDocument.Parse(payloadJson).Elements);
        return document;
    }

    /// <summary>
    /// A <c>$set</c> that touches exactly the named field; it is never a whole-document replace,
    /// which would fail the byte-identical check on the other nine fields.
    /// </summary>
    internal static UpdateDefinition<BsonDocument> FieldUpdate(string fieldName, string value) =>
        Builders<BsonDocument>.Update.Set(fieldName, value);

    /// <summary>
    /// Translates the insecure-certificate option the caller's connection string may use
    /// (<c>tlsAllowInvalidCertificates</c>, a shell/Java spelling the .NET driver ignores) to the
    /// driver's own <c>tlsInsecure</c>. Every other connection string passes through unchanged, so
    /// certificate validation stays on for a target whose string does not ask for this.
    /// </summary>
    internal static string NormalizeConnectionString(string connectionString)
    {
        if (HasOption(connectionString, DriverInsecureTlsOption) || HasTrueOption(connectionString, "tlsAllowInvalidCertificates") == false)
            return connectionString;

        var separator = connectionString.Contains('?') ? '&' : '?';
        return connectionString + separator + DriverInsecureTlsOption + "=true";
    }

    /// <summary>
    /// Replaces the password in a connection string's user-info with a fixed token, keeping the
    /// user, host, port and options. The authority bounds the search, so a credential-free string
    /// is returned unchanged even when its query carries an <c>@</c>.
    /// </summary>
    internal static string RedactConnectionString(string connectionString)
    {
        const string schemeSeparator = "://";
        var schemeEnd = connectionString.IndexOf(schemeSeparator, StringComparison.Ordinal);
        if (schemeEnd < 0)
            return connectionString;

        var userInfoStart = schemeEnd + schemeSeparator.Length;
        var authorityEnd = connectionString.IndexOfAny(['/', '?', '#'], userInfoStart);
        if (authorityEnd < 0)
            authorityEnd = connectionString.Length;
        if (authorityEnd <= userInfoStart)
            return connectionString;

        var at = connectionString.LastIndexOf('@', authorityEnd - 1);
        if (at < userInfoStart)
            return connectionString;

        var colon = connectionString.IndexOf(':', userInfoStart, at - userInfoStart);
        if (colon < 0)
            return connectionString;

        return string.Concat(connectionString.AsSpan(0, colon + 1), "***", connectionString.AsSpan(at));
    }

    private async Task<TransportResult> ReadAsync(ReadOperation read, CancellationToken ct)
    {
        var document = await _collection.Find(ReadFilter(read.Id)).FirstOrDefaultAsync(ct).ConfigureAwait(false);
        return document == null
            ? new TransportResult(0, 0, $"Document '{read.Id}' was not found.")
            : new TransportResult(0, 0);
    }

    private async Task<TransportResult> InsertAsync(InsertOperation<string> insert, CancellationToken ct)
    {
        await _collection.InsertOneAsync(ToStoredDocument(insert.Id, insert.Payload), cancellationToken: ct).ConfigureAwait(false);
        return new TransportResult(0, 0);
    }

    private async Task<TransportResult> UpdateFieldAsync(UpdateFieldOperation update, CancellationToken ct)
    {
        var result = await _collection
            .UpdateOneAsync(ReadFilter(update.Id), FieldUpdate(update.FieldName, update.Value), cancellationToken: ct)
            .ConfigureAwait(false);

        // The server reports a missing match as a successful no-op; a run that updates a key it
        // never loaded is an error, not throughput.
        return result.MatchedCount == 0
            ? new TransportResult(0, 0, $"Document '{update.Id}' was not found for field update.")
            : new TransportResult(0, 0);
    }

    private async Task<TransportResult> BulkInsertAsync(BulkInsertOperation<string> bulk, CancellationToken ct)
    {
        var batch = new List<BsonDocument>(bulk.Documents.Count);
        foreach (var document in bulk.Documents)
            batch.Add(ToStoredDocument(document.Id, document.Document));

        // Unordered keeps a bulk load from serializing behind a single duplicate key; the load run
        // is the bulk path and its throughput is the load row.
        await _collection.InsertManyAsync(batch, new InsertManyOptions { IsOrdered = false }, ct).ConfigureAwait(false);
        return new TransportResult(0, 0);
    }

    private static async Task<TransportResult> Guarded(Func<Task<TransportResult>> body, CancellationToken ct)
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

    private static bool HasOption(string connectionString, string name)
    {
        var index = connectionString.IndexOf(name, StringComparison.OrdinalIgnoreCase);
        while (index >= 0)
        {
            if (IsOptionBoundary(connectionString, index))
                return true;
            index = connectionString.IndexOf(name, index + 1, StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static bool HasTrueOption(string connectionString, string name)
    {
        var index = connectionString.IndexOf(name, StringComparison.OrdinalIgnoreCase);
        while (index >= 0)
        {
            if (IsOptionBoundary(connectionString, index))
            {
                var value = connectionString.AsSpan(index + name.Length);
                if (value.StartsWith("=true", StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            index = connectionString.IndexOf(name, index + 1, StringComparison.OrdinalIgnoreCase);
        }

        return false;
    }

    private static bool IsOptionBoundary(string connectionString, int index) =>
        index == 0 || connectionString[index - 1] is '?' or '&';
}
