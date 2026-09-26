using System.Text.RegularExpressions;
using MongoDB.Bson;
using MongoDB.Driver;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;
using RavenBench.Core;

namespace RavenBench.Core.Transport;

/// <summary>
/// Drives the ycsb document operations against MongoDB Community and Microsoft DocumentDB through
/// the official MongoDB .NET driver. One transport serves both targets: they speak the same wire
/// protocol, so only the reported product name differs. It implements <see cref="IYcsbTransport"/>
/// alone, because neither product exposes SNMP, a license type or a RavenDB calibration endpoint,
/// and the driver hides the socket, so byte counts are not wire-accurate.
/// </summary>
public sealed class MongoYcsbTransport : IYcsbTransport, IReportsStorageSize, IInspectsStoredDocuments
{
    /// <summary>Scenario target name for MongoDB Community.</summary>
    public const string MongoDbTarget = "mongodb";

    /// <summary>
    /// Scenario target name for MongoDB Community with the supporting aggregate indexes. It differs
    /// from <see cref="MongoDbTarget"/> only in that it creates those indexes.
    /// </summary>
    public const string MongoDbIndexedTarget = "mongodb-indexed";

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
    private readonly IMongoCollection<BsonDocument> _aggregates;

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
            _ when string.Equals(target, MongoDbIndexedTarget, StringComparison.OrdinalIgnoreCase) => MongoDbProductName,
            _ when string.Equals(target, DocumentDbTarget, StringComparison.OrdinalIgnoreCase) => DocumentDbProductName,
            _ => throw new ArgumentException(
                $"Unknown Mongo target '{target}'. Known targets are '{MongoDbTarget}', '{MongoDbIndexedTarget}' and '{DocumentDbTarget}'.", nameof(target))
        };

        Target = target.ToLowerInvariant();
        RecordedEndpoint = RedactConnectionString(connectionString);

        var settings = MongoClientSettings.FromConnectionString(NormalizeConnectionString(connectionString));
        // Every write path inherits the concern from the client, so no path can drift from j:true.
        settings.WriteConcern = DurableWriteConcern;

        _client = new MongoClient(settings);
        _database = _client.GetDatabase(databaseName);
        _collection = _database.GetCollection<BsonDocument>(CollectionName);
        _aggregates = _database.GetCollection<BsonDocument>(AggregateDocument.MongoCollection);
    }

    /// <summary>The scenario target this transport was built for.</summary>
    public string Target { get; }

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
        BulkInsertOperation<AggregateDocument> bulk => Guarded(() => BulkInsertAggregatesAsync(bulk, ct), ct),
        GroupedAggregateOperation aggregate => Guarded(() => AggregateAsync(aggregate, ct), ct),
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

    /// <inheritdoc />
    public string StorageSizeMetricName => StorageSizeMetric;

    /// <summary>
    /// The <c>dbStats</c> key the on-disk size is read from. It is the storage the collections and
    /// their indexes occupy; the plain storage size omits the indexes.
    /// </summary>
    internal const string StorageSizeMetric = "dbStats.totalSize";

    /// <inheritdoc />
    public async Task<long> GetStorageSizeBytesAsync()
    {
        var stats = await _database.RunCommandAsync<BsonDocument>(new BsonDocument("dbStats", 1)).ConfigureAwait(false);
        if (stats.TryGetValue("totalSize", out var size) && size.IsNumeric)
            return (long)size.ToDouble();

        throw new InvalidOperationException($"The Mongo server's dbStats response carries no totalSize for database '{_database.DatabaseNamespace.DatabaseName}'.");
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

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>?> ReadStoredFieldsAsync(string id, CancellationToken ct)
    {
        var stored = await _collection.Find(ReadFilter(id)).FirstOrDefaultAsync(ct).ConfigureAwait(false);
        if (stored is null)
            return null;

        var fields = new Dictionary<string, string>(PayloadGenerator.FieldCount);
        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            var name = PayloadGenerator.FieldName(i);
            if (stored.TryGetValue(name, out var value) && value.IsString)
                fields[name] = value.AsString;
        }

        return fields;
    }

    /// <inheritdoc />
    public Task DeleteStoredDocumentAsync(string id, CancellationToken ct) =>
        _collection.DeleteOneAsync(ReadFilter(id), ct);

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
    internal static string RedactConnectionString(string connectionString) =>
        ConnectionStringRedaction.Redact(connectionString);

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

    /// <summary>The stored form of an aggregate document: the emitted fields unchanged, the amount as a 64-bit integer.</summary>
    internal static BsonDocument ToStoredAggregate(AggregateDocument d) => new()
    {
        { "_id", d.Id },
        { AggregateDocument.CategoryField, d.Category },
        { AggregateDocument.RegionField, d.Region },
        { AggregateDocument.AmountField, new BsonInt64(d.Amount) },
        { AggregateDocument.TimestampField, d.Timestamp },
        { AggregateDocument.PayloadField, d.Payload }
    };

    /// <summary>
    /// The pipeline of a grouped aggregate: <c>$match</c> on the filter, <c>$group</c> by the key,
    /// then <c>$sort</c> by value descending and key ascending in binary order, and <c>$limit</c>.
    /// Binary string order is the shared ordinal tie break, so the server cut is the shared cut.
    /// </summary>
    private static BsonDocument[] AggregatePipeline(GroupedAggregateOperation op)
    {
        op.Validate();
        var stages = new List<BsonDocument>(5);
        switch (op.Filter)
        {
            case null:
                break;
            case EqualityFilter eq:
                stages.Add(new BsonDocument("$match", new BsonDocument(eq.Field, eq.Value)));
                break;
            case RangeFilter range:
                stages.Add(new BsonDocument("$match", new BsonDocument(range.Field, new BsonDocument { { "$gte", range.Lower }, { "$lt", range.Upper } })));
                break;
            default:
                throw new NotSupportedException($"{nameof(MongoYcsbTransport)} cannot filter by {op.Filter.GetType().Name}.");
        }
        BsonValue value = op.Kind == AggregateKind.Count ? new BsonInt64(1) : "$" + op.SumField;
        stages.Add(new BsonDocument("$group", new BsonDocument { { "_id", "$" + op.GroupBy }, { "value", new BsonDocument("$sum", value) } }));
        stages.Add(new BsonDocument("$sort", new BsonDocument { { "value", -1 }, { "_id", 1 } }));
        stages.Add(new BsonDocument("$limit", op.TopN));
        return stages.ToArray();
    }

    /// <summary>
    /// Creates the repository-defined supporting indexes when the target is <see cref="MongoDbIndexedTarget"/>,
    /// and nothing otherwise. This is the only behavior that depends on the target. A repeated
    /// call with the same definitions succeeds.
    /// </summary>
    public async Task EnsureAggregateIndexesAsync(CancellationToken ct)
    {
        await EnsureCollectionAsync(AggregateDocument.MongoCollection).ConfigureAwait(false);
        if (Target != MongoDbIndexedTarget)
            return;
        var models = AggregateShapes.MongoIndexes().Select(spec => new CreateIndexModel<BsonDocument>(
            BsonDocument.Parse(spec.GetProperty("key").GetRawText()),
            new CreateIndexOptions { Name = spec.GetProperty("name").GetString() }));
        await _aggregates.Indexes.CreateManyAsync(models, ct).ConfigureAwait(false);
    }

    /// <summary>The names of every index on the aggregate collection.</summary>
    public async Task<IReadOnlyList<string>> ListAggregateIndexNamesAsync(CancellationToken ct)
    {
        using var cursor = await _aggregates.Indexes.ListAsync(ct).ConfigureAwait(false);
        return (await cursor.ToListAsync(ct).ConfigureAwait(false)).Select(i => i["name"].AsString).ToList();
    }

    /// <summary>Drops the aggregate collection and its indexes; dropping a missing collection succeeds.</summary>
    public Task DropAggregateCollectionAsync(CancellationToken ct) => _database.DropCollectionAsync(AggregateDocument.MongoCollection, ct);

    private async Task EnsureCollectionAsync(string name)
    {
        try
        {
            await _database.CreateCollectionAsync(name).ConfigureAwait(false);
        }
        catch (MongoCommandException ex) when (ex.CodeName == "NamespaceExists" || ex.Code == 48)
        {
            // Expected: the collection already exists.
        }
    }

    private async Task<TransportResult> BulkInsertAggregatesAsync(BulkInsertOperation<AggregateDocument> bulk, CancellationToken ct)
    {
        await _aggregates.InsertManyAsync(bulk.Documents.Select(d => ToStoredAggregate(d.Document)), new InsertManyOptions { IsOrdered = false }, ct).ConfigureAwait(false);
        return new TransportResult(0, 0);
    }

    // A pipeline computes at read time, so the product never marks the answer stale.
    private async Task<TransportResult> AggregateAsync(GroupedAggregateOperation op, CancellationToken ct)
    {
        using var cursor = await _aggregates.AggregateAsync<BsonDocument>(AggregatePipeline(op), cancellationToken: ct).ConfigureAwait(false);
        var rows = await cursor.ToListAsync(ct).ConfigureAwait(false);
        var groups = rows.Select(r => new AggregateGroup(r["_id"].AsString, r["value"].AsInt64)).ToList();
        return new TransportResult(0, 0, resultCount: groups.Count, isStale: false) { Groups = groups };
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
