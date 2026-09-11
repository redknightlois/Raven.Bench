using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using MongoDB.Bson;
using MongoDB.Driver;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// State-based contract tests for the Mongo transport against the live MongoDB Community and
/// DocumentDB containers. Both endpoints run the same assertions through the same transport; the
/// environment gate skips one endpoint at a time, and the test body then uses the real server.
/// Each test uses its own database and drops it when it finishes.
/// </summary>
public class MongoYcsbTransportIntegrationTests
{
    private const int Seed = 42;
    private const int DocumentSize = 1024;

    [RequiresMongoFact]
    public Task Mongo_Insert_Read_Round_Trips_And_Missing_Ids_Fail() =>
        RoundTrip(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget);

    [RequiresDocumentDbFact]
    public Task DocumentDb_Insert_Read_Round_Trips_And_Missing_Ids_Fail() =>
        RoundTrip(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget);

    [RequiresMongoFact]
    public Task Mongo_One_Field_Update_Leaves_The_Other_Nine_Byte_Identical() =>
        SingleFieldUpdate(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget);

    [RequiresDocumentDbFact]
    public Task DocumentDb_One_Field_Update_Leaves_The_Other_Nine_Byte_Identical() =>
        SingleFieldUpdate(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget);

    [RequiresMongoFact]
    public Task Mongo_Bulk_Load_Fills_The_Counted_Keyspace() =>
        BulkLoad(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget);

    [RequiresDocumentDbFact]
    public Task DocumentDb_Bulk_Load_Fills_The_Counted_Keyspace() =>
        BulkLoad(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget);

    [RequiresMongoFact]
    public Task Mongo_Ensure_Is_Idempotent_And_A_Fresh_Collection_Is_Empty() =>
        EnsureIdempotent(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget);

    [RequiresDocumentDbFact]
    public Task DocumentDb_Ensure_Is_Idempotent_And_A_Fresh_Collection_Is_Empty() =>
        EnsureIdempotent(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget);

    [RequiresMongoFact]
    public Task Mongo_Reports_The_Product_The_Server_Version_The_Concern_And_The_Put_Path() =>
        Metadata(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget);

    [RequiresDocumentDbFact]
    public Task DocumentDb_Reports_The_Product_The_Server_Version_The_Concern_And_The_Put_Path() =>
        Metadata(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget);

    private static async Task RoundTrip(string connectionString, string target)
    {
        await WithTransport(connectionString, target, async transport =>
        {
            var id = BenchIds.IdFor(3);
            var payload = PayloadGenerator.Generate(Seed, id, DocumentSize);

            var insert = await transport.ExecuteAsync(new InsertOperation<string> { Id = id, Payload = payload }, CancellationToken.None);
            insert.IsSuccess.Should().BeTrue(insert.ErrorDetails);

            var read = await transport.ExecuteAsync(new ReadOperation { Id = id }, CancellationToken.None);
            read.IsSuccess.Should().BeTrue(read.ErrorDetails);

            var stored = await FindAsync(transport, id);
            stored.Should().NotBeNull();
            stored!["_id"].AsString.Should().Be(id);
            var expected = BsonDocument.Parse(payload);
            for (int i = 0; i < PayloadGenerator.FieldCount; i++)
            {
                var field = PayloadGenerator.FieldName(i);
                stored[field].AsString.Should().Be(expected[field].AsString);
            }

            var missingId = BenchIds.IdFor(999999);
            var missingRead = await transport.ExecuteAsync(new ReadOperation { Id = missingId }, CancellationToken.None);
            missingRead.IsSuccess.Should().BeFalse("a read of a missing id is a definite not-found");
            missingRead.ErrorDetails.Should().Contain(missingId);

            var missingUpdate = await transport.ExecuteAsync(new UpdateFieldOperation
            {
                Id = missingId,
                FieldName = PayloadGenerator.FieldName(0),
                Value = PayloadGenerator.GenerateFieldValue(PayloadGenerator.FieldWidth(DocumentSize), new Random(7))
            }, CancellationToken.None);
            missingUpdate.IsSuccess.Should().BeFalse("an update of a missing id is a definite failure");
            missingUpdate.ErrorDetails.Should().Contain(missingId);
        });
    }

    private static async Task SingleFieldUpdate(string connectionString, string target)
    {
        await WithTransport(connectionString, target, async transport =>
        {
            var id = BenchIds.IdFor(5);
            var payload = PayloadGenerator.Generate(Seed, id, DocumentSize);
            (await transport.ExecuteAsync(new InsertOperation<string> { Id = id, Payload = payload }, CancellationToken.None))
                .IsSuccess.Should().BeTrue();

            const int updatedField = 4;
            var replacement = PayloadGenerator.GenerateFieldValue(
                PayloadGenerator.FieldWidth(DocumentSize), new Random(7));
            var update = await transport.ExecuteAsync(new UpdateFieldOperation
            {
                Id = id,
                FieldName = PayloadGenerator.FieldName(updatedField),
                Value = replacement
            }, CancellationToken.None);
            update.IsSuccess.Should().BeTrue(update.ErrorDetails);

            var stored = await FindAsync(transport, id);
            stored.Should().NotBeNull();
            var expected = BsonDocument.Parse(payload);
            for (int i = 0; i < PayloadGenerator.FieldCount; i++)
            {
                var field = PayloadGenerator.FieldName(i);
                stored![field].AsString.Should().Be(
                    i == updatedField ? replacement : expected[field].AsString,
                    "only {0} may differ from the seeded document", PayloadGenerator.FieldName(updatedField));
            }

            // The replacement keeps the field width, so a run of updates does not drift the document size.
            stored![PayloadGenerator.FieldName(updatedField)].AsString.Length.Should().Be(expected[PayloadGenerator.FieldName(updatedField)].AsString.Length);
        });
    }

    private static async Task BulkLoad(string connectionString, string target)
    {
        await WithTransport(connectionString, target, async transport =>
        {
            const int count = 100;
            var documents = Enumerable.Range(1, count)
                .Select(i => new DocumentToWrite<string>
                {
                    Id = BenchIds.IdFor(i),
                    Document = PayloadGenerator.Generate(Seed, BenchIds.IdFor(i), DocumentSize)
                })
                .ToList();

            var result = await transport.ExecuteAsync(new BulkInsertOperation<string> { Documents = documents }, CancellationToken.None);
            result.IsSuccess.Should().BeTrue(result.ErrorDetails);

            (await transport.GetDocumentCountAsync("bench/")).Should().Be(count);
            (await transport.GetDocumentCountAsync("other/")).Should().Be(0);
        });
    }

    private static async Task EnsureIdempotent(string connectionString, string target)
    {
        await WithTransport(connectionString, target, async transport =>
        {
            // The wrapper already called Ensure once; a second call must succeed and leave the
            // collection empty, so a read of a missing id is a not-found and not a collection error.
            await transport.EnsureDatabaseExistsAsync(transport.Documents.Database.DatabaseNamespace.DatabaseName);

            (await transport.GetDocumentCountAsync("bench/")).Should().Be(0);
            var read = await transport.ExecuteAsync(new ReadOperation { Id = BenchIds.IdFor(1) }, CancellationToken.None);
            read.IsSuccess.Should().BeFalse();
        });
    }

    private static async Task Metadata(string connectionString, string target)
    {
        await WithTransport(connectionString, target, async transport =>
        {
            var expectedProduct = target == MongoYcsbTransport.MongoDbTarget
                ? MongoYcsbTransport.MongoDbProductName
                : MongoYcsbTransport.DocumentDbProductName;

            transport.ProductName.Should().Be(expectedProduct);
            transport.AppliedWriteConcern.Journal.Should().BeTrue();

            var reported = await transport.GetServerVersionAsync();
            reported.Should().NotBeNullOrWhiteSpace();

            // The recorded version is the server's own buildInfo version, not a literal.
            var info = await transport.Documents.Database
                .RunCommandAsync(new BsonDocumentCommand<BsonDocument>(new BsonDocument("buildInfo", 1)));
            info["version"].AsString.Should().Be(reported);

            // PutAsync is the outside-the-measured-path write; it stores one document too.
            var id = BenchIds.IdFor(9);
            await transport.PutAsync(id, PayloadGenerator.Generate(Seed, id, DocumentSize));
            var read = await transport.ExecuteAsync(new ReadOperation { Id = id }, CancellationToken.None);
            read.IsSuccess.Should().BeTrue(read.ErrorDetails);
            (await transport.GetDocumentCountAsync("bench/")).Should().Be(1);
        });
    }

    private static async Task<BsonDocument?> FindAsync(MongoYcsbTransport transport, string id) =>
        await transport.Documents.Find(MongoYcsbTransport.ReadFilter(id)).FirstOrDefaultAsync();

    private static async Task WithTransport(string connectionString, string target, Func<MongoYcsbTransport, Task> body)
    {
        var database = "ycsb_it_" + Guid.NewGuid().ToString("N");
        using var transport = new MongoYcsbTransport(connectionString, database, target);
        try
        {
            await transport.EnsureDatabaseExistsAsync(database);
            await body(transport);
        }
        finally
        {
            await transport.Documents.Database.Client.DropDatabaseAsync(database);
        }
    }
}
