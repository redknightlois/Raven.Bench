using System;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Embedded;
using Raven.TestDriver;
using RavenBench.Cli;
using RavenBench.Core.Ycsb;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Drives the full ycsb sequence against a real (embedded) RavenDB server, the way the repository
/// already tests other full-run paths: state-based, no transport mock.
/// </summary>
public class YcsbRunnerIntegrationTests : RavenTestDriver
{
    static YcsbRunnerIntegrationTests()
    {
        ConfigureServer(new TestServerOptions
        {
            Licensing = new ServerOptions.LicensingOptions
            {
                ThrowOnInvalidOrMissingLicense = false
            }
        });
    }

    [Fact]
    public async Task Full_Sequence_Produces_One_Result_Per_Run_With_The_Resolved_Scenario_Recorded()
    {
        using var store = GetDocumentStore();

        var scenario = new YcsbScenario
        {
            Seed = 42,
            Target = "ravendb",
            DocumentCount = 20,
            DocumentSize = "256B",
            Concurrency = "2..2",
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "300ms"
        };

        var settings = new YcsbSettings
        {
            Url = store.Urls[0],
            Database = store.Database,
            Scenario = "unused-in-this-test.json",
            BulkBatchSize = 5
        };

        var runner = new YcsbRunner(scenario, settings);
        var results = await runner.RunAsync();

        results.Select(r => r.Kind).Should().Equal(
            YcsbRunKind.Load, YcsbRunKind.WorkloadC, YcsbRunKind.WorkloadA, YcsbRunKind.WorkloadB, YcsbRunKind.InsertStream);

        results.Select(r => r.Summary.Ycsb!.Run).Should().Equal("load", "C", "A", "B", "insert-stream");

        foreach (var (_, summary) in results)
        {
            summary.Steps.Should().NotBeEmpty();
            summary.Ycsb!.ProductName.Should().Be("RavenDB");
            summary.Ycsb.ServerVersion.Should().NotBeNullOrEmpty();
            summary.Ycsb.Durability.Setting.Should().Be("durability");
            summary.Ycsb.ResolvedScenario.Should().Be(scenario);
        }
    }

    [Fact]
    public async Task Load_Run_Fills_The_Keyspace_The_Later_Runs_Depend_On()
    {
        using var store = GetDocumentStore();

        var scenario = new YcsbScenario
        {
            Seed = 1,
            Target = "ravendb",
            DocumentCount = 15,
            DocumentSize = "256B",
            Concurrency = "2..2",
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "300ms"
        };

        var settings = new YcsbSettings
        {
            Url = store.Urls[0],
            Database = store.Database,
            Scenario = "unused-in-this-test.json",
            BulkBatchSize = 5
        };

        var results = await new YcsbRunner(scenario, settings).RunAsync();

        // If the load run had not reached DocumentCount, the later runs would never have started
        // (YcsbRunner throws before C when the keyspace falls short).
        results.Should().HaveCount(5);
    }
}
