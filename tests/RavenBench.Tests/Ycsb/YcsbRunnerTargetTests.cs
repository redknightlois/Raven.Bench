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
            Target = "postgresql",
            DocumentCount = 10,
            DocumentSize = "256B",
            Concurrency = "2..2",
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "200ms"
        };
        var settings = new YcsbSettings { Url = "http://localhost:8081", Database = "unused", Scenario = "unused.json" };

        var act = () => new YcsbRunner(scenario, settings).RunAsync();

        await act.Should().ThrowAsync<YcsbScenarioException>().WithMessage("*postgresql*");
    }

    [RequiresMongoFact]
    public Task A_Mongo_Target_Completes_And_Records_Its_Durability() =>
        RunDispatchedSequence(MongoTestEndpoints.MongoConnectionString, MongoYcsbTransport.MongoDbTarget, MongoYcsbTransport.MongoDbProductName);

    [RequiresDocumentDbFact]
    public Task A_DocumentDb_Target_Completes_And_Records_A_Redacted_Endpoint() =>
        RunDispatchedSequence(MongoTestEndpoints.DocumentDbConnectionString, MongoYcsbTransport.DocumentDbTarget, MongoYcsbTransport.DocumentDbProductName);

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
}
