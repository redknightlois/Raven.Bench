using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apex.SqlClient;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using Raven.Embedded;
using Raven.TestDriver;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// The four-product parity check: for a 1,000-document sample, the ten field names and values read
/// back from PostgreSQL, MongoDB and DocumentDB equal the ones read back from RavenDB. The only
/// permitted differences are the id field/column and key order.
/// </summary>
public class YcsbProductParityTests : RavenTestDriver
{
    private const int Seed = 42;
    private const int DocumentSize = 1024;
    private const int DocumentCount = 1000;
    private const int BulkBatchSize = 100;
    private const int PostgreSqlConcurrency = 8;

    static YcsbProductParityTests()
    {
        ConfigureServer(new TestServerOptions
        {
            Licensing = new ServerOptions.LicensingOptions
            {
                ThrowOnInvalidOrMissingLicense = false
            }
        });
    }

    [RequiresMongoDocumentDbAndPostgreSqlFact]
    public async Task The_Ten_Fields_Read_Back_The_Same_From_PostgreSql_MongoDB_DocumentDB_And_RavenDB()
    {
        var mongoDatabase = "ycsb_parity_mongo_" + Guid.NewGuid().ToString("N");
        var documentDbDatabase = "ycsb_parity_docdb_" + Guid.NewGuid().ToString("N");

        await using var pgSchema = await PgTestSchema.CreateAsync();
        using var store = GetDocumentStore();
        using var raven = new RawHttpTransport(store.Urls[0], store.Database, CompressionMode.Identity, HttpVersion.Version11);
        using var mongo = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, mongoDatabase, MongoYcsbTransport.MongoDbTarget);
        using var documentDb = new MongoYcsbTransport(MongoTestEndpoints.DocumentDbConnectionString, documentDbDatabase, MongoYcsbTransport.DocumentDbTarget);
        using var postgres = new PostgresYcsbTransport(pgSchema.ConnectionString, PostgreSqlTestEndpoints.Database, PostgreSqlConcurrency);

        await raven.EnsureDatabaseExistsAsync(store.Database);
        await mongo.EnsureDatabaseExistsAsync(mongoDatabase);
        await documentDb.EnsureDatabaseExistsAsync(documentDbDatabase);
        await postgres.EnsureDatabaseExistsAsync(PostgreSqlTestEndpoints.Database);

        try
        {
            var batches = Enumerable.Range(1, DocumentCount)
                .Select(i =>
                {
                    var id = BenchIds.IdFor(i);
                    return new DocumentToWrite<string> { Id = id, Document = PayloadGenerator.Generate(Seed, id, DocumentSize) };
                })
                .Chunk(BulkBatchSize);

            foreach (var batch in batches)
            {
                var documents = batch.ToList();
                var bulk = new BulkInsertOperation<string> { Documents = documents };

                (await raven.ExecuteAsync(bulk, CancellationToken.None)).IsSuccess.Should().BeTrue();
                (await mongo.ExecuteAsync(bulk, CancellationToken.None)).IsSuccess.Should().BeTrue();
                (await documentDb.ExecuteAsync(bulk, CancellationToken.None)).IsSuccess.Should().BeTrue();
                (await postgres.ExecuteAsync(bulk, CancellationToken.None)).IsSuccess.Should().BeTrue();
            }

            (await mongo.GetDocumentCountAsync("bench/")).Should().Be(DocumentCount);
            (await documentDb.GetDocumentCountAsync("bench/")).Should().Be(DocumentCount);
            (await postgres.GetDocumentCountAsync("bench/")).Should().Be(DocumentCount);

            using var http = new HttpClient();
            for (int i = 1; i <= DocumentCount; i++)
            {
                var id = BenchIds.IdFor(i);
                var expected = BsonDocument.Parse(PayloadGenerator.Generate(Seed, id, DocumentSize));
                var fromMongo = await mongo.Documents.Find(MongoYcsbTransport.ReadFilter(id)).FirstOrDefaultAsync();
                var fromDocumentDb = await documentDb.Documents.Find(MongoYcsbTransport.ReadFilter(id)).FirstOrDefaultAsync();
                var fromPostgres = await ReadPostgresFieldsAsync(postgres, id);
                var fromRaven = await ReadRavenFieldsAsync(http, store.Urls[0], store.Database, id);

                fromMongo.Should().NotBeNull("MongoDB must store {0}", id);
                fromDocumentDb.Should().NotBeNull("DocumentDB must store {0}", id);
                fromPostgres.Should().NotBeNull("PostgreSQL must store {0}", id);

                for (int field = 0; field < PayloadGenerator.FieldCount; field++)
                {
                    var name = PayloadGenerator.FieldName(field);
                    var value = expected[name].AsString;
                    fromMongo![name].AsString.Should().Be(value, "MongoDB field {0} of {1}", name, id);
                    fromDocumentDb![name].AsString.Should().Be(value, "DocumentDB field {0} of {1}", name, id);
                    fromPostgres![name].Should().Be(value, "PostgreSQL field {0} of {1}", name, id);
                    fromRaven[name].Should().Be(value, "RavenDB field {0} of {1}", name, id);
                }
            }
        }
        finally
        {
            await mongo.Documents.Database.Client.DropDatabaseAsync(mongoDatabase);
            await documentDb.Documents.Database.Client.DropDatabaseAsync(documentDbDatabase);
        }
    }

    private static Task<Dictionary<string, string>?> ReadPostgresFieldsAsync(PostgresYcsbTransport transport, string id) =>
        transport.ReadWithMeasuredConnectionAsync(async connection =>
        {
            var rows = await connection.QueryAsync(PostgresYcsbTransport.ReadSql, SqlParameters.Create(id), CancellationToken.None);
            if (rows.Count == 0)
                return (Dictionary<string, string>?)null;

            using var stored = JsonDocument.Parse(rows[0].Get<string>(0));
            var fields = new Dictionary<string, string>(PayloadGenerator.FieldCount);
            for (int i = 0; i < PayloadGenerator.FieldCount; i++)
            {
                var name = PayloadGenerator.FieldName(i);
                fields[name] = stored.RootElement.GetProperty(name).GetString()!;
            }

            return fields;
        });

    private static async Task<Dictionary<string, string>> ReadRavenFieldsAsync(HttpClient http, string url, string database, string id)
    {
        var body = await http.GetStringAsync($"{url}/databases/{database}/docs?id={Uri.EscapeDataString(id)}");
        using var response = JsonDocument.Parse(body);
        var stored = response.RootElement.GetProperty("Results")[0];

        var fields = new Dictionary<string, string>(PayloadGenerator.FieldCount);
        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            var name = PayloadGenerator.FieldName(i);
            fields[name] = stored.GetProperty(name).GetString()!;
        }

        return fields;
    }
}
