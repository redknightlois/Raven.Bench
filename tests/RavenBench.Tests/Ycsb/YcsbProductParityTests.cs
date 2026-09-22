using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Driver;
using Raven.Embedded;
using Raven.TestDriver;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// The four-product parity check: for a 1,000-document sample, the ten field names and values read
/// back from PostgreSQL, MongoDB and DocumentDB equal the ones read back from RavenDB. The only
/// permitted differences are the id field/column and key order.
/// </summary>
public class YcsbProductParityTests : EmbeddedRavenTestBase
{
    private const int Seed = 42;
    private const int DocumentSize = 1024;
    private const int DocumentCount = 1000;
    private const int BulkBatchSize = 100;
    private const int PostgreSqlConcurrency = 8;

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

            for (int i = 1; i <= DocumentCount; i++)
            {
                var id = BenchIds.IdFor(i);
                var expected = YcsbParityCheck.ExpectedFields(Seed, id, DocumentSize);

                // Each product is read back through its own transport, and the shared comparison
                // rule decides agreement.
                foreach (var (product, transport) in new (string, IInspectsStoredDocuments)[]
                         {
                             ("RavenDB", raven), ("MongoDB", mongo), ("DocumentDB", documentDb), ("PostgreSQL", postgres)
                         })
                {
                    var stored = await transport.ReadStoredFieldsAsync(id, CancellationToken.None);
                    YcsbParityCheck.DescribeDifference(id, expected, stored)
                        .Should().BeNull("{0} must store the seeded fields of {1}", product, id);
                }
            }
        }
        finally
        {
            await mongo.Documents.Database.Client.DropDatabaseAsync(mongoDatabase);
            await documentDb.Documents.Database.Client.DropDatabaseAsync(documentDbDatabase);
        }
    }

}
