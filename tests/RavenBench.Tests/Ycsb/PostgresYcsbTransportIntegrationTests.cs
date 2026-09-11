using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apex.SqlClient;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// State-based contract tests for the PostgreSQL transport against the live container. Each test
/// works in its own schema so parallel classes do not share the fixed <c>ycsb</c> table, and drops
/// the schema when it finishes.
/// </summary>
public class PostgresYcsbTransportIntegrationTests
{
    private const int Seed = 42;
    private const int DocumentSize = 1024;
    private const int MaxConcurrency = 4;

    [RequiresPostgreSqlFact]
    public Task Insert_Read_Round_Trips_And_Missing_Ids_Fail() => RoundTrip();

    [RequiresPostgreSqlFact]
    public Task One_Field_Update_Leaves_The_Other_Nine_Byte_Identical() => SingleFieldUpdate();

    [RequiresPostgreSqlFact]
    public Task Bulk_Load_Fills_The_Counted_Keyspace() => BulkLoad();

    [RequiresPostgreSqlFact]
    public Task A_Failed_Bulk_Load_Does_Not_Poison_The_Connection_Set() => FailedBulkLoadRecovers();

    [RequiresPostgreSqlFact]
    public Task Ensure_Is_Idempotent_And_A_Fresh_Table_Is_Empty() => EnsureIdempotent();

    [RequiresPostgreSqlFact]
    public Task A_Measured_Connection_Runs_With_Synchronous_Commit_On() => Durability();

    [RequiresPostgreSqlFact]
    public Task Reports_The_Product_The_Server_Version_And_The_Put_Path() => Metadata();

    private static async Task RoundTrip()
    {
        await WithTransport(async transport =>
        {
            var id = BenchIds.IdFor(3);
            var payload = PayloadGenerator.Generate(Seed, id, DocumentSize);

            var insert = await transport.ExecuteAsync(new InsertOperation<string> { Id = id, Payload = payload }, CancellationToken.None);
            insert.IsSuccess.Should().BeTrue(insert.ErrorDetails);

            var read = await transport.ExecuteAsync(new ReadOperation { Id = id }, CancellationToken.None);
            read.IsSuccess.Should().BeTrue(read.ErrorDetails);

            var stored = await FindDocumentAsync(transport, id);
            stored.Should().NotBeNull();
            var expected = JsonDocument.Parse(payload);
            foreach (var field in expected.RootElement.EnumerateObject())
                stored!.Value.GetProperty(field.Name).GetString().Should().Be(field.Value.GetString(), "field {0} of {1}", field.Name, id);

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

    private static async Task SingleFieldUpdate()
    {
        await WithTransport(async transport =>
        {
            var id = BenchIds.IdFor(5);
            var payload = PayloadGenerator.Generate(Seed, id, DocumentSize);
            (await transport.ExecuteAsync(new InsertOperation<string> { Id = id, Payload = payload }, CancellationToken.None))
                .IsSuccess.Should().BeTrue();

            const int updatedField = 4;
            var replacement = PayloadGenerator.GenerateFieldValue(PayloadGenerator.FieldWidth(DocumentSize), new Random(7));
            var update = await transport.ExecuteAsync(new UpdateFieldOperation
            {
                Id = id,
                FieldName = PayloadGenerator.FieldName(updatedField),
                Value = replacement
            }, CancellationToken.None);
            update.IsSuccess.Should().BeTrue(update.ErrorDetails);

            var stored = await FindDocumentAsync(transport, id);
            stored.Should().NotBeNull();
            var expected = JsonDocument.Parse(payload);
            for (int i = 0; i < PayloadGenerator.FieldCount; i++)
            {
                var field = PayloadGenerator.FieldName(i);
                stored!.Value.GetProperty(field).GetString().Should().Be(
                    i == updatedField ? replacement : expected.RootElement.GetProperty(field).GetString(),
                    "only {0} may differ from the seeded document", field);
            }

            // The replacement keeps the field width, so a run of updates does not drift the document size.
            stored!.Value.GetProperty(PayloadGenerator.FieldName(updatedField)).GetString()!.Length
                .Should().Be(expected.RootElement.GetProperty(PayloadGenerator.FieldName(updatedField)).GetString()!.Length);
        });
    }

    private static async Task BulkLoad()
    {
        await WithTransport(async transport =>
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

            // The bulk path stores the same fields the single insert stores.
            var stored = await FindDocumentAsync(transport, BenchIds.IdFor(7));
            stored.Should().NotBeNull();
            stored!.Value.GetProperty(PayloadGenerator.FieldName(0)).GetString()
                .Should().Be(JsonDocument.Parse(documents[6].Document).RootElement.GetProperty(PayloadGenerator.FieldName(0)).GetString());
        });
    }

    private static async Task FailedBulkLoadRecovers()
    {
        // One measured connection, so the connection the failed COPY ran on is the only one the
        // next operation can draw. Without replacement the read fails with the dead connection.
        await using var schema = await PgTestSchema.CreateAsync();
        using var transport = new PostgresYcsbTransport(schema.ConnectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 1);
        await transport.EnsureDatabaseExistsAsync(PostgreSqlTestEndpoints.Database);

        var id = BenchIds.IdFor(1);
        var document = new DocumentToWrite<string>
        {
            Id = id,
            Document = PayloadGenerator.Generate(Seed, id, DocumentSize)
        };
        var first = await transport.ExecuteAsync(new BulkInsertOperation<string> { Documents = new List<DocumentToWrite<string>> { document } }, CancellationToken.None);
        first.IsSuccess.Should().BeTrue(first.ErrorDetails);

        var duplicate = await transport.ExecuteAsync(new BulkInsertOperation<string> { Documents = new List<DocumentToWrite<string>> { document } }, CancellationToken.None);
        duplicate.IsSuccess.Should().BeFalse("the table already holds the id");
        duplicate.ErrorDetails.Should().Contain("duplicate", "the server's cause must not be hidden by a cascade of connection errors");

        var read = await transport.ExecuteAsync(new ReadOperation { Id = id }, CancellationToken.None);
        read.IsSuccess.Should().BeTrue("the failed COPY must not leave the only measured connection dead");
    }

    private static async Task EnsureIdempotent()
    {
        await WithTransport(async transport =>
        {
            // The wrapper already called Ensure once; a second call must succeed and leave the
            // table empty, so a read of a missing id is a not-found and not a missing-table error.
            await transport.EnsureDatabaseExistsAsync(PostgreSqlTestEndpoints.Database);

            (await transport.GetDocumentCountAsync("bench/")).Should().Be(0);
            var read = await transport.ExecuteAsync(new ReadOperation { Id = BenchIds.IdFor(1) }, CancellationToken.None);
            read.IsSuccess.Should().BeFalse();
        });
    }

    private static async Task Durability()
    {
        await WithTransport(async transport =>
        {
            var applied = await transport.ReadWithMeasuredConnectionAsync(async connection =>
            {
                var rows = await connection.QueryAsync(PostgresYcsbTransport.ReadDurabilitySql, CancellationToken.None);
                return rows[0].Get<string>(0);
            });

            applied.Should().Be("on");
        });
    }

    private static async Task Metadata()
    {
        await WithTransport(async transport =>
        {
            transport.ProductName.Should().NotBeNullOrWhiteSpace().And.NotBe("unknown");
            transport.ReportsWireBytes.Should().BeFalse();

            var reportedVersion = await transport.GetServerVersionAsync();
            reportedVersion.Should().NotBeNullOrWhiteSpace();

            var server = await transport.ReadWithMeasuredConnectionAsync(async connection =>
            {
                var versionRows = await connection.QueryAsync("SHOW server_version", CancellationToken.None);
                var bannerRows = await connection.QueryAsync("SELECT version()", CancellationToken.None);
                return (Version: versionRows[0].Get<string>(0), Banner: bannerRows[0].Get<string>(0));
            });

            reportedVersion.Should().Be(server.Version, "the recorded version is the server's own, not a literal");
            server.Banner.Should().StartWith(transport.ProductName, "the product name is the server's own, not a literal");

            var id = BenchIds.IdFor(9);
            await transport.PutAsync(id, PayloadGenerator.Generate(Seed, id, DocumentSize));
            (await transport.ExecuteAsync(new ReadOperation { Id = id }, CancellationToken.None)).IsSuccess.Should().BeTrue();
            (await transport.GetDocumentCountAsync("bench/")).Should().Be(1);

            // PutAsync upserts outside the measured path, so a second call is not a duplicate-key failure.
            await transport.PutAsync(id, PayloadGenerator.Generate(Seed, id, DocumentSize));
            (await transport.GetDocumentCountAsync("bench/")).Should().Be(1);
        });
    }

    private static Task<JsonElement?> FindDocumentAsync(PostgresYcsbTransport transport, string id) =>
        transport.ReadWithMeasuredConnectionAsync(async connection =>
        {
            var rows = await connection.QueryAsync(PostgresYcsbTransport.ReadSql, SqlParameters.Create(id), CancellationToken.None);
            if (rows.Count == 0)
                return (JsonElement?)null;

            using var document = JsonDocument.Parse(rows[0].Get<string>(0));
            return document.RootElement.Clone();
        });

    private static async Task WithTransport(Func<PostgresYcsbTransport, Task> body)
    {
        await using var schema = await PgTestSchema.CreateAsync();
        using var transport = new PostgresYcsbTransport(schema.ConnectionString, PostgreSqlTestEndpoints.Database, MaxConcurrency);
        await transport.EnsureDatabaseExistsAsync(PostgreSqlTestEndpoints.Database);
        await body(transport);
    }
}
