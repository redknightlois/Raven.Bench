using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core.Transport;
using RavenBench.Core.Ycsb;
using RavenBench.Reporter;
using RavenBench.Reporting;
using RavenBench.Tests.Infrastructure;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins target dispatch at the runner: a known Mongo target builds the Mongo transport and never
/// touches the RavenDB-only setup, an unknown target names the offending value, and the result
/// records the endpoint and the durability of the target that ran.
/// </summary>
public class YcsbRunnerTargetTests
{
    [Fact]
    public async Task An_Unknown_Target_Throws_Naming_The_Value()
    {
        var scenario = new YcsbScenario
        {
            Seed = 1,
            Target = "cockroachdb",
            DocumentCount = 10,
            DocumentSize = "256B",
            Concurrency = "2..2",
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "200ms"
        };
        var settings = new YcsbSettings { Url = "http://localhost:8081", Database = "unused", Scenario = "unused.json" };

        var act = () => new YcsbRunner(scenario, settings).RunAsync();

        await act.Should().ThrowAsync<YcsbScenarioException>().WithMessage("*cockroachdb*");
    }

    [RequiresMongoFact]
    public Task A_Mongo_Target_Completes_And_Records_Its_Durability() =>
        RunDispatchedSequence(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget, MongoYcsbTransport.MongoDbProductName);

    [RequiresDocumentDbFact]
    public Task A_DocumentDb_Target_Completes_And_Records_A_Redacted_Endpoint() =>
        RunDispatchedSequence(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget, MongoYcsbTransport.DocumentDbProductName);

    [RequiresPostgreSqlFact]
    public Task A_PostgreSql_Target_Completes_And_Records_Its_Durability() => RunPostgreSqlSequence();

    [Theory]
    [InlineData("2..2", 2)]
    [InlineData("8..64x2", 64)]
    public void The_Concurrency_Ceiling_Follows_The_Resolved_Step_Plan(string concurrency, int expected)
    {
        var scenario = new YcsbScenario
        {
            Seed = 1,
            Target = PostgresYcsbTransport.Target,
            DocumentCount = 10,
            DocumentSize = "256B",
            Concurrency = concurrency,
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "200ms"
        };

        YcsbRunner.ResolveConcurrencyCeiling(scenario, "unused", "unused").Should().Be(expected);
    }

    [Fact]
    public void A_Rate_Scenario_Keeps_At_Least_The_Closed_Loop_Concurrency()
    {
        var scenario = new YcsbScenario
        {
            Seed = 1,
            Target = PostgresYcsbTransport.Target,
            DocumentCount = 10,
            DocumentSize = "256B",
            Concurrency = "4..16x2",
            Rate = 500,
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "200ms"
        };

        YcsbRunner.ResolveConcurrencyCeiling(scenario, "unused", "unused").Should().BeGreaterThanOrEqualTo(16);
    }

    private static async Task RunDispatchedSequence(string connectionString, string target, string expectedProductName)
    {
        var database = "ycsb_dispatch_" + Guid.NewGuid().ToString("N");
        var scenario = new YcsbScenario
        {
            Seed = 1,
            Target = target,
            DocumentCount = 10,
            DocumentSize = "256B",
            Concurrency = "2..2",
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "200ms"
        };

        // The RavenDB-only options would abort an HTTP negotiation against a mongo:// endpoint, so a
        // Mongo run that reached them would fail before issuing an operation.
        var settings = new YcsbSettings
        {
            Url = connectionString,
            Database = database,
            Scenario = "unused.json",
            BulkBatchSize = 5,
            Transport = "client",
            Compression = "gzip",
            HttpVersion = "3.0",
            StrictHttpVersion = true
        };

        try
        {
            var results = await new YcsbRunner(scenario, settings).RunAsync();

            results.Select(r => r.Kind).Should().Equal(
                YcsbRunKind.Load, YcsbRunKind.WorkloadC, YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream);

            foreach (var (_, summary) in results)
            {
                summary.Ycsb!.ProductName.Should().Be(expectedProductName);
                summary.Ycsb.ServerVersion.Should().NotBeNullOrWhiteSpace();
                summary.Ycsb.Durability.Setting.Should().Be("writeConcern");
                summary.Ycsb.Durability.Value.Should().Be("j=true");
                summary.Options.Url.Should().NotContain("bench:bench", "no result field may carry the connection-string password");
            }

            var path = Path.Combine(Path.GetTempPath(), $"ycsb-dispatch-{Guid.NewGuid():N}.json");
            try
            {
                JsonResultsWriter.Write(path, results[0].Summary);
                File.ReadAllText(path).Should().NotContain("bench:bench");

                var loaded = await SummaryLoader.LoadAsync(path);
                loaded.SchemaVersion.Should().Be(SummaryLoader.ExpectedSchemaVersion);
                loaded.Ycsb!.ProductName.Should().Be(expectedProductName);
                loaded.Ycsb.Durability.Value.Should().Be("j=true");
            }
            finally
            {
                File.Delete(path);
            }
        }
        finally
        {
            using var cleanup = new MongoYcsbTransport(connectionString, database, target);
            await cleanup.Documents.Database.Client.DropDatabaseAsync(database);
        }
    }

    private static async Task RunPostgreSqlSequence()
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

        // The RavenDB-only options would abort against a PostgreSQL endpoint, so a PostgreSQL run
        // that reached HTTP negotiation would fail before issuing an operation.
        var settings = new YcsbSettings
        {
            Url = schema.ConnectionString,
            Database = PostgreSqlTestEndpoints.Database,
            Scenario = "unused.json",
            BulkBatchSize = 5,
            Transport = "client",
            Compression = "gzip",
            HttpVersion = "3.0",
            StrictHttpVersion = true
        };

        var results = await new YcsbRunner(scenario, settings).RunAsync();

        results.Select(r => r.Kind).Should().Equal(
            YcsbRunKind.Load, YcsbRunKind.WorkloadC, YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream);

        foreach (var (_, summary) in results)
        {
            summary.Ycsb!.ProductName.Should().Be("PostgreSQL");
            summary.Ycsb.ServerVersion.Should().NotBeNullOrWhiteSpace();
            summary.Ycsb.Durability.Setting.Should().Be("synchronous_commit");
            summary.Ycsb.Durability.Value.Should().Be("on");
            summary.Options.Url.Should().NotContain("bench:bench", "no result field may carry the connection-string password");
            summary.Steps.Should().OnlyContain(step => step.NetworkBytesMeasured == false, "the driver does not expose the socket");
        }

        var path = Path.Combine(Path.GetTempPath(), $"ycsb-pg-dispatch-{Guid.NewGuid():N}.json");
        try
        {
            JsonResultsWriter.Write(path, results[0].Summary);
            File.ReadAllText(path).Should().NotContain("bench:bench");

            var loaded = await SummaryLoader.LoadAsync(path);
            loaded.SchemaVersion.Should().Be(SummaryLoader.ExpectedSchemaVersion);
            loaded.Ycsb!.ProductName.Should().Be("PostgreSQL");
            loaded.Ycsb.Durability.Setting.Should().Be("synchronous_commit");
            loaded.Ycsb.Durability.Value.Should().Be("on");
        }
        finally
        {
            File.Delete(path);
        }
    }
}
