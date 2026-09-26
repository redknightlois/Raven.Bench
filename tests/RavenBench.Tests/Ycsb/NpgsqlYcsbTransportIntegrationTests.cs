using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Npgsql;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;
using RavenBench.Tests.Infrastructure;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// State-based contract tests for the Npgsql transport against the live container. Each test works
/// in its own schema and runs the Apex transport beside it on the same table, so the two modes are
/// checked against each other's stored state.
/// </summary>
[Collection(LiveServers.Name)]
public class NpgsqlYcsbTransportIntegrationTests
{
    private const int Seed = 42;
    private const int DocumentSize = 1024;
    private const int MaxConcurrency = 4;

    [RequiresPostgreSqlFact]
    public Task Insert_Read_Round_Trips_And_Missing_Ids_Fail() => WithTransports(async (apex, client, entity) =>
    {
        foreach (var (transport, ordinal) in new[] { (client, 1), (entity, 2) })
        {
            var id = BenchIds.IdFor(ordinal);
            var insert = await transport.ExecuteAsync(PayloadGenerator.InsertOperationFor(PayloadKind.Json, Seed, id, DocumentSize), CancellationToken.None);
            insert.IsSuccess.Should().BeTrue(insert.ErrorDetails);
            (await transport.ExecuteAsync(new ReadOperation { Id = id }, CancellationToken.None)).IsSuccess.Should().BeTrue();

            var readBack = await apex.ReadStoredFieldsAsync(id, CancellationToken.None);
            YcsbParityCheck.DescribeDifference(id, YcsbParityCheck.ExpectedFields(Seed, id, DocumentSize), readBack).Should().BeNull();

            var missingId = BenchIds.IdFor(999999);
            var missing = await transport.ExecuteAsync(new ReadOperation { Id = missingId }, CancellationToken.None);
            missing.IsSuccess.Should().BeFalse();
            missing.ErrorDetails.Should().Contain(missingId);

            var missingUpdate = await transport.ExecuteAsync(new UpdateFieldOperation { Id = missingId, FieldName = PayloadGenerator.FieldName(0), Value = "x" }, CancellationToken.None);
            missingUpdate.IsSuccess.Should().BeFalse();

            var duplicate = await transport.ExecuteAsync(PayloadGenerator.InsertOperationFor(PayloadKind.Json, Seed, id, DocumentSize), CancellationToken.None);
            duplicate.IsSuccess.Should().BeFalse("a duplicate id is a primary-key violation, not a silent success");
        }
    });

    [RequiresPostgreSqlFact]
    public Task The_Entity_Mode_Writes_A_Mapped_Record_That_Apex_Reads_Back() => WithTransports(async (apex, _, entity) =>
    {
        var id = BenchIds.IdFor(5);
        var record = PayloadGenerator.GenerateRecord(Seed, id, DocumentSize);
        (await entity.ExecuteAsync(new InsertOperation<YcsbRecord> { Id = id, Payload = record }, CancellationToken.None)).IsSuccess.Should().BeTrue();

        var stored = await apex.ReadStoredFieldsAsync(id, CancellationToken.None);
        for (int i = 0; i < YcsbRecord.FieldCount; i++)
            stored![PayloadGenerator.FieldName(i)].Should().Be(record.GetField(i));

        var json = await entity.ReadStoredJsonAsync(id, CancellationToken.None);
        var mapped = JsonSerializer.Deserialize<YcsbRecord>(json!)!;
        mapped.GetField(3).Should().Be(record.GetField(3));
    });

    [RequiresPostgreSqlFact]
    public Task The_Client_Mode_Refuses_An_Entity_And_Reads_The_Stored_Json_Unchanged() => WithTransports(async (apex, client, _) =>
    {
        var id = BenchIds.IdFor(6);
        await apex.PutAsync(id, PayloadGenerator.Generate(Seed, id, DocumentSize));

        var apexText = await apex.ReadWithMeasuredConnectionAsync(async connection =>
        {
            var rows = await connection.QueryAsync(PostgresYcsbTransport.ReadSql, Apex.SqlClient.SqlParameters.Create(id), CancellationToken.None);
            return rows[0].Get<string>(0);
        });
        (await client.ReadStoredJsonAsync(id, CancellationToken.None)).Should().Be(apexText);

        var refused = await client.ExecuteAsync(new InsertOperation<YcsbRecord> { Id = BenchIds.IdFor(7), Payload = new YcsbRecord() }, CancellationToken.None);
        refused.IsSuccess.Should().BeFalse();
        refused.ErrorDetails.Should().Contain(nameof(YcsbRecord));
    });

    [RequiresPostgreSqlFact]
    public Task One_Field_Update_Leaves_The_Other_Nine_Unchanged() => WithTransports(async (apex, client, _) =>
    {
        var id = BenchIds.IdFor(8);
        await apex.PutAsync(id, PayloadGenerator.Generate(Seed, id, DocumentSize));
        var update = new UpdateFieldOperation { Id = id, FieldName = PayloadGenerator.FieldName(4), Value = new string('z', PayloadGenerator.FieldWidth(DocumentSize)) };
        (await client.ExecuteAsync(update, CancellationToken.None)).IsSuccess.Should().BeTrue();

        var expected = YcsbParityCheck.ExpectedFields(Seed, id, DocumentSize).ToDictionary(p => p.Key, p => p.Value);
        expected[update.FieldName] = update.Value;
        YcsbParityCheck.DescribeDifference(id, expected, await apex.ReadStoredFieldsAsync(id, CancellationToken.None)).Should().BeNull();
    });

    [RequiresPostgreSqlFact]
    public Task A_Binary_Copy_Bulk_Load_Is_Counted_Through_Both_Modes() => WithTransports(async (apex, client, entity) =>
    {
        const int count = 250;
        var strings = Enumerable.Range(1, count).Select(i => BenchIds.IdFor(i))
            .Select(id => new DocumentToWrite<string> { Id = id, Document = PayloadGenerator.Generate(Seed, id, DocumentSize) }).ToList();
        (await client.ExecuteAsync(new BulkInsertOperation<string> { Documents = strings }, CancellationToken.None)).IsSuccess.Should().BeTrue();

        var records = Enumerable.Range(count + 1, count).Select(i => BenchIds.IdFor(i))
            .Select(id => new DocumentToWrite<YcsbRecord> { Id = id, Document = PayloadGenerator.GenerateRecord(Seed, id, DocumentSize) }).ToList();
        (await entity.ExecuteAsync(new BulkInsertOperation<YcsbRecord> { Documents = records }, CancellationToken.None)).IsSuccess.Should().BeTrue();

        (await client.GetDocumentCountAsync("bench/")).Should().Be(2 * count);
        (await apex.GetDocumentCountAsync("bench/")).Should().Be(2 * count);

        var failed = await client.ExecuteAsync(new BulkInsertOperation<string> { Documents = strings.Take(1).ToList() }, CancellationToken.None);
        failed.IsSuccess.Should().BeFalse("a COPY of an existing id is a primary-key violation");
        (await client.GetDocumentCountAsync("bench/")).Should().Be(2 * count, "the pool recovers the connection a failed COPY ran on");
    });

    [RequiresPostgreSqlFact]
    public Task Pooled_Connections_Run_With_Synchronous_Commit_On_Under_Concurrent_Use() => WithTransports(async (_, client, _) =>
    {
        // More concurrent callers than the pool holds: every one waits for a pooled connection and
        // every connection, including a reused one, reports the parity setting.
        var applied = await Task.WhenAll(Enumerable.Range(0, MaxConcurrency * 4).Select(_ => client.ReadAppliedDurabilityAsync()));
        applied.Should().OnlyContain(v => v == PostgresYcsbTransport.DurabilityValue);
        new NpgsqlConnectionStringBuilder(NpgsqlYcsbTransport.BuildConnectionString("postgresql://bench:bench@localhost:5432/bench", "bench", 17)).MaxPoolSize.Should().Be(17);
    });

    [RequiresPostgreSqlFact]
    public Task Concurrent_Inserts_And_Updates_Through_Both_Clients_Leave_Consistent_State() => WithTransports(async (apex, client, entity) =>
    {
        const int count = 64;
        var ids = Enumerable.Range(1, count).Select(i => BenchIds.IdFor(i)).ToArray();
        var inserts = ids.Select((id, i) => (i % 2 == 0 ? client : entity)
            .ExecuteAsync(PayloadGenerator.InsertOperationFor(PayloadKind.Json, Seed, id, DocumentSize), CancellationToken.None));
        (await Task.WhenAll(inserts)).Should().OnlyContain(r => r.IsSuccess);

        var width = PayloadGenerator.FieldWidth(DocumentSize);
        var updates = ids.Select((id, i) => client.ExecuteAsync(
            new UpdateFieldOperation { Id = id, FieldName = PayloadGenerator.FieldName(i % 10), Value = new string('q', width) }, CancellationToken.None));
        (await Task.WhenAll(updates)).Should().OnlyContain(r => r.IsSuccess);

        (await apex.GetDocumentCountAsync("bench/")).Should().Be(count);
        for (int i = 0; i < count; i++)
            (await apex.ReadStoredFieldsAsync(ids[i], CancellationToken.None))![PayloadGenerator.FieldName(i % 10)].Should().Be(new string('q', width));
    });

    [RequiresPostgreSqlFact]
    public Task Reports_The_Server_Product_Version_And_The_Loaded_Npgsql_Version() => WithTransports(async (apex, client, _) =>
    {
        client.ProductName.Should().Be(apex.ProductName);
        (await client.GetServerVersionAsync()).Should().Be(await apex.GetServerVersionAsync());
        client.ReportsWireBytes.Should().BeFalse();
        (await client.GetStorageSizeBytesAsync()).Should().BePositive();

        var informational = typeof(NpgsqlConnection).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()!.InformationalVersion;
        informational.Should().StartWith(NpgsqlYcsbTransport.ClientLibraryVersion);
        NpgsqlYcsbTransport.ClientLibraryVersion.Should().NotContain("+");
    });

    [RequiresPostgreSqlFact]
    public Task Parity_Agrees_Through_Apex_And_Both_Npgsql_Modes() => WithTransports(async (apex, client, entity) =>
    {
        var database = PostgreSqlTestEndpoints.Database;
        var products = new[]
        {
            new YcsbParityProduct(PostgresYcsbTransport.Target, apex.RecordedEndpoint, database, apex),
            new YcsbParityProduct(ParityCommand.PostgreSqlModeName(TransportKind.Client), client.RecordedEndpoint, database, client),
            new YcsbParityProduct(ParityCommand.PostgreSqlModeName(TransportKind.ClientEntity), entity.RecordedEndpoint, database, entity)
        };

        var report = await new YcsbParityCheck(Seed, DocumentSize, sampleSize: 50).RunAsync(products, CancellationToken.None);

        report.Products.Should().Equal("postgresql", "postgresql/client", "postgresql/client-entity");
        foreach (var product in report.Products)
            report.Pairs.Where(p => p.Product == product).Select(p => p.Operation).Should().Equal(YcsbParityOperations.All);
        report.Pairs.Should().OnlyContain(p => p.Agreed, "every operation agrees through every mode");
        (await apex.GetDocumentCountAsync("bench/")).Should().Be(0, "the check removes its sample");
    });

    [RequiresPostgreSqlFact]
    public async Task A_Ycsb_Run_Through_Each_Npgsql_Mode_Records_The_Mode_And_The_Npgsql_Version()
    {
        foreach (var (mode, kind) in new[] { ("client", TransportKind.Client), ("client-entity", TransportKind.ClientEntity) })
        {
            await using var schema = await PgTestSchema.CreateAsync();
            var scenario = new YcsbScenario
            {
                Seed = 1,
                Target = PostgresYcsbTransport.Target,
                DocumentCount = 10,
                DocumentSize = "256B",
                Concurrency = "2..2",
                Distribution = "uniform",
                Warmup = "0s",
                Duration = "200ms"
            };
            var settings = new YcsbSettings
            {
                Url = schema.ConnectionString,
                Database = PostgreSqlTestEndpoints.Database,
                Scenario = "unused.json",
                BulkBatchSize = 5,
                Transport = mode
            };

            var results = await new YcsbRunner(scenario, settings).RunAsync();

            results.Should().HaveCount(5);
            foreach (var (_, summary) in results)
            {
                summary.Options.Transport.Should().Be(kind);
                summary.Ycsb!.ClientLibrary.Should().Be("Npgsql");
                summary.Ycsb.ClientLibraryVersion.Should().Be(NpgsqlYcsbTransport.ClientLibraryVersion);
                summary.Ycsb.Durability.Setting.Should().Be("synchronous_commit");
                summary.Ycsb.Durability.Value.Should().Be("on");
                summary.Options.Url.Should().NotContain("bench:bench");
                summary.Steps.Should().OnlyContain(s => s.ErrorRate == 0, $"every {mode} operation succeeds");
            }
        }
    }

    private static async Task WithTransports(Func<PostgresYcsbTransport, NpgsqlYcsbTransport, NpgsqlYcsbTransport, Task> body)
    {
        await using var schema = await PgTestSchema.CreateAsync();
        var database = PostgreSqlTestEndpoints.Database;
        using var apex = new PostgresYcsbTransport(schema.ConnectionString, database, MaxConcurrency);
        using var client = new NpgsqlYcsbTransport(schema.ConnectionString, database, MaxConcurrency, mapEntities: false);
        using var entity = new NpgsqlYcsbTransport(schema.ConnectionString, database, MaxConcurrency, mapEntities: true);
        await apex.EnsureDatabaseExistsAsync(database);
        await client.EnsureDatabaseExistsAsync(database);
        await entity.EnsureDatabaseExistsAsync(database);
        await body(apex, client, entity);
    }
}
