using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Embedded;
using Raven.TestDriver;
using MongoDB.Driver;
using RavenBench.Analysis;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Tests.Infrastructure;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Ycsb;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Drives the full ycsb sequence against a real (embedded) RavenDB server, the way the repository
/// already tests other full-run paths: state-based, no transport mock.
/// </summary>
[Collection(LiveServers.Name)]
public class YcsbRunnerIntegrationTests : EmbeddedRavenTestBase
{
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
            summary.MachineFingerprint.Should().NotBeNull();
            summary.MachineFingerprint!.DatabaseInDocker.Should().BeFalse("the RavenDB target is external");
            summary.MachineFingerprint.DatabaseImage.Should().BeNull();
            summary.Ycsb.ImageReference.Should().BeNull();
            summary.Ycsb.ImageDigest.Should().BeNull();
        }
    }

    [Fact]
    public async Task Full_Sequence_Leaves_Distinct_Histogram_And_Csv_Artifacts()
    {
        using var store = GetDocumentStore();

        var scenario = new YcsbScenario
        {
            Seed = 3,
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

        results.Select(r => r.Summary.Ycsb!.Run).Should().OnlyHaveUniqueItems();

        var artifacts = results.SelectMany(r => r.Summary.HistogramArtifacts!).ToList();
        artifacts.Should().NotBeEmpty();

        var paths = artifacts.SelectMany(a => new[] { a.HlogPath, a.CsvPath }).ToList();
        paths.Should().NotContainNulls("every ycsb step writes both artifacts");
        paths.Should().OnlyHaveUniqueItems("the run identity keeps the five runs' artifacts apart");

        foreach (var path in paths)
        {
            File.Exists(path).Should().BeTrue($"the result names '{path}'");
            new FileInfo(path!).Length.Should().BeGreaterThan(0);
        }
    }

    [Theory]
    [InlineData("ravendb-6")]
    [InlineData("ravendb-7")]
    public async Task A_Containerized_RavenDb_Target_Dispatches_To_The_RavenDb_Transport(string target)
    {
        using var store = GetDocumentStore();

        var scenario = new YcsbScenario
        {
            Seed = 9,
            Target = target,
            DocumentCount = 10,
            DocumentSize = "256B",
            Concurrency = "2..2",
            Distribution = "uniform",
            Warmup = "0s",
            Duration = "200ms"
        };

        var settings = new YcsbSettings
        {
            Url = store.Urls[0],
            Database = store.Database,
            Scenario = "unused-in-this-test.json",
            BulkBatchSize = 5
        };

        var results = await new YcsbRunner(scenario, settings).RunAsync();

        results.Should().HaveCount(5);
        foreach (var (_, summary) in results)
        {
            summary.Ycsb!.ProductName.Should().Be("RavenDB");
            summary.Ycsb.ServerVersion.Should().NotBeNullOrWhiteSpace();
            summary.Ycsb.Durability.Setting.Should().Be("durability");
            summary.Ycsb.ResolvedScenario.Target.Should().Be(target, "the target value reaches the resolved scenario");
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

    // The step-measurement contract: what every ycsb step and the load result report.

    private static YcsbScenario Scenario(string target, int documentCount) => new()
    {
        Seed = 42,
        Target = target,
        DocumentCount = documentCount,
        DocumentSize = "256B",
        Concurrency = "2..2",
        Distribution = "uniform",
        Warmup = "0s",
        // The load run is a bounded fill: it ends when the keyspace is full, well inside this cap,
        // which is what makes the measured wall time distinguishable from the configured duration.
        Duration = "10s"
    };

    private static YcsbSettings Settings(string url, string database) => new()
    {
        Url = url,
        Database = database,
        Scenario = "unused-in-this-test.json",
        BulkBatchSize = 5
    };

    [Fact]
    public async Task A_Ravendb_Run_Reports_Load_Host_Cpu_The_Server_Columns_It_Collected_And_A_Derived_Verdict()
    {
        using var store = GetDocumentStore();

        var results = await new YcsbRunner(Scenario("ravendb", 20), Settings(store.Urls[0], store.Database)).RunAsync();

        foreach (var (kind, summary) in results)
        {
            summary.Steps.Should().OnlyContain(s => s.ClientCpu > 0, $"the {kind} run did work over a closed window");
            summary.Steps.Should().OnlyContain(s => s.ClientCpu <= 1.0, "the figure is a 0..1 fraction");

            var columns = summary.Ycsb!.ServerColumns;
            columns.Product.Should().Be("RavenDB");
            columns.Statement.Should().Contain("RavenDB");

            // The statement is derived from the steps: a column is named only when a step carries it.
            columns.Columns.Contains(nameof(StepResult.ServerCpu))
                .Should().Be(summary.Steps.Any(s => s.ServerCpu.HasValue));
            columns.Columns.Contains(nameof(StepResult.ServerMemoryMB))
                .Should().Be(summary.Steps.Any(s => s.ServerMemoryMB.HasValue));

            summary.Verdict.Should().NotBe("ycsb", "the field carries an attribution");
            summary.Knee.Should().NotBeNull();
            summary.Verdict.Should().Be(ResultAnalyzer.BuildVerdict(summary.Knee, summary.Options),
                "the ycsb path reaches the one attribution its siblings use");
        }

        // The tracker is wired into the ycsb path, so a run long enough for the collector's poll
        // carries at least the memory column somewhere in the sequence.
        results.SelectMany(r => r.Summary.Steps).Should().Contain(s => s.ServerMemoryMB.HasValue,
            "a RavenDB ycsb target drives the server metrics tracker");

        // The same attribution on a saturated step gives the client-limited verdict.
        var knee = results[0].Summary.Knee!;
        var saturated = new StepResult { Concurrency = knee.Concurrency, Throughput = knee.Throughput, ClientCpu = ClientSaturation.Threshold };
        ResultAnalyzer.BuildVerdict(saturated, results[0].Summary.Options).Should().Be("client-limited (CPU)");
        results.Should().NotContain(r => r.Summary.Verdict == "client-limited (CPU)",
            "an embedded two-worker run does not saturate the load host");
    }

    [Fact]
    public async Task The_Load_Fills_The_Keyspace_Whatever_The_Scenario_Duration()
    {
        using var store = GetDocumentStore();

        var scenario = Scenario("ravendb", 40) with { Duration = "1ms" };
        var results = await new YcsbRunner(scenario, Settings(store.Urls[0], store.Database), transportOverride: null, include: id => id.Kind == YcsbRunKind.Load).RunAsync();

        // The runner refuses a short keyspace, so a returned load result is a full one.
        results.Should().ContainSingle(r => r.Kind == YcsbRunKind.Load);
    }

    [Fact]
    public async Task The_Load_Result_Reports_Throughput_Wall_Time_Cpu_And_The_Size_Ravendb_Names()
    {
        using var store = GetDocumentStore();

        var scenario = Scenario("ravendb", 40);
        var results = await new YcsbRunner(scenario, Settings(store.Urls[0], store.Database)).RunAsync();
        var (_, load) = results.Single(r => r.Kind == YcsbRunKind.Load);

        var step = load.Steps.Should().ContainSingle().Subject;
        step.Throughput.Should().BeGreaterThan(0, "documents per second, not batches per second");
        step.ClientCpu.Should().BeGreaterThan(0);
        step.MeasuredDuration.Should().NotBeNull();
        step.MeasuredDuration!.Value.Should().BeGreaterThan(TimeSpan.Zero);
        step.MeasuredDuration.Value.Should().BeLessThan(load.Options.Duration,
            "a bounded fill that ends early reports the window it measured, never the configured cap");

        var size = load.Ycsb!.LoadedSize.Should().NotBeNull().And.Subject as OnDiskSize;
        size!.Unavailable.Should().BeNull("RavenDB exposes an on-disk size");
        size.Metric.Should().Be("SizeOnDisk.SizeInBytes", "the result names the statistic the figure came from");
        size.Bytes.Should().BeGreaterThan(0);

        // The load figures are readable against one clock: throughput times the window is the
        // document count the fill produced.
        (step.Throughput * step.MeasuredDuration.Value.TotalSeconds)
            .Should().BeApproximately(scenario.DocumentCount, scenario.DocumentCount * 0.1);

        results.Where(r => r.Kind != YcsbRunKind.Load)
            .Should().OnlyContain(r => r.Summary.Ycsb!.LoadedSize == null,
                "the C, A and B runs do not change the loaded set");
    }

    [RequiresMongoFact]
    public async Task A_Mongo_Ycsb_Run_States_It_Has_No_Server_Column_And_Reports_Its_Own_Size()
    {
        var database = "ycsb-measure-" + Guid.NewGuid().ToString("N")[..8];

        try
        {
            var results = await new YcsbRunner(
                Scenario(MongoYcsbTransport.MongoDbTarget, 20),
                Settings(MongoTestEndpoints.MongoConnectionString, database)).RunAsync();

            foreach (var (_, summary) in results)
            {
                var columns = summary.Ycsb!.ServerColumns;
                columns.Columns.Should().BeEmpty("this harness reads no MongoDB server metric");
                columns.Statement.Should().Contain("no server column");
                summary.Steps.Should().OnlyContain(s => s.ServerCpu == null && s.ServerMemoryMB == null);
                summary.Steps.Should().OnlyContain(s => s.ClientCpu > 0);
            }

            var load = results.Single(r => r.Kind == YcsbRunKind.Load).Summary;
            load.Ycsb!.LoadedSize!.Metric.Should().Be(MongoYcsbTransport.StorageSizeMetric);
            load.Ycsb.LoadedSize.Bytes.Should().BeGreaterThan(0);
            load.Ycsb.LoadedSize.Unavailable.Should().BeNull();
        }
        finally
        {
            using var cleanup = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbTarget);
            await cleanup.Documents.Database.Client.DropDatabaseAsync(database);
        }
    }

    [RequiresPostgreSqlFact]
    public async Task A_Postgres_Transport_Reports_The_Size_Through_Its_Own_Named_Function()
    {
        using var transport = new PostgresYcsbTransport(
            PostgreSqlTestEndpoints.ConnectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 1);

        await transport.EnsureDatabaseExistsAsync(PostgreSqlTestEndpoints.Database);

        var reporter = transport.Should().BeAssignableTo<IReportsStorageSize>().Subject;
        reporter.StorageSizeMetricName.Should().Be(PostgresYcsbTransport.StorageSizeMetric);
        (await reporter.GetStorageSizeBytesAsync()).Should().BeGreaterThan(0);
    }

    [Fact]
    public void A_Product_Whose_Transport_Exposes_No_Size_Says_So_By_Name()
    {
        var stated = OnDiskSize.NotExposed("SomeProduct");

        stated.Bytes.Should().BeNull("a fabricated zero is worse than the named fact");
        stated.Metric.Should().BeNull();
        stated.Unavailable.Should().Contain("SomeProduct").And.Contain("no on-disk size");
    }

    // The set one invocation produces, and what each result of it says about itself.

    private static YcsbScenario SetScenario(string target, int documentCount) => Scenario(target, documentCount) with
    {
        Seed = 11,
        Duration = "300ms",
        Rates = new[] { 50.0 },
        Distributions = new[] { "uniform", "zipfian" },
        Repetitions = 2
    };

    private static int ExpectedResultCount(YcsbScenario scenario) =>
        1 + scenario.ResolvedRepetitions * (scenario.ResolvedDistributions.Count + 3 + scenario.ResolvedRates.Count);

    [Fact]
    public async Task One_Invocation_Produces_The_Set_The_Scenario_Names()
    {
        using var store = GetDocumentStore();

        var scenario = SetScenario("ravendb", 20);
        var results = await new YcsbRunner(scenario, Settings(store.Urls[0], store.Database)).RunAsync();

        results.Should().HaveCount(ExpectedResultCount(scenario));
        results.Count(r => r.Kind == YcsbRunKind.Load).Should().Be(1, "the keyspace is loaded once per invocation");

        // The ramp gives the ceiling, every named rate gives a fixed-rate row, and workload C runs
        // under every named distribution.
        results.Should().Contain(r => r.Summary.Ycsb!.Shape == "closed");
        results.Where(r => r.Summary.Ycsb!.Shape == "rate").Select(r => r.Summary.Ycsb!.Rate)
            .Should().OnlyContain(rate => rate == scenario.ResolvedRates[0]);
        results.Where(r => r.Kind == YcsbRunKind.WorkloadC && r.Summary.Ycsb!.Shape == "closed")
            .Select(r => r.Summary.Ycsb!.Distribution).Distinct()
            .Should().BeEquivalentTo(scenario.ResolvedDistributions);

        // A scenario that names one repetition fewer produces a smaller set, so the count follows
        // the file.
        var smaller = scenario with { Repetitions = 1 };
        var fewer = await new YcsbRunner(smaller, Settings(store.Urls[0], store.Database)).RunAsync();
        fewer.Should().HaveCount(ExpectedResultCount(smaller));
        fewer.Count.Should().BeLessThan(results.Count);
    }

    [Fact]
    public async Task Every_Result_Records_What_Produced_It_And_No_Two_Share_That_Identity()
    {
        using var store = GetDocumentStore();

        var scenario = SetScenario("ravendb", 20);
        var results = await new YcsbRunner(scenario, Settings(store.Urls[0], store.Database)).RunAsync();

        foreach (var (identity, summary) in results)
        {
            var info = summary.Ycsb!;
            info.Shape.Should().BeOneOf("closed", "rate");
            info.Repetition.Should().BeGreaterThan(0);
            info.Rate.HasValue.Should().Be(info.Shape == "rate", "a rate row records its rate and a closed row has none");

            // The recorded distribution is the one the run used, not the scenario's first value.
            CliParsing.ParseDistribution(info.Distribution).Should().Be(summary.Options.Distribution);

            info.ResolvedScenario.Seed.Should().Be(scenario.Seed, "the resolved scenario keeps the file's seed");
            summary.Options.Seed.Should().Be(SeedMixer.Derive(scenario.Seed, identity.ResultName),
                "the seed a run used is readable from its own result");
        }

        results.Select(r => (r.Summary.Ycsb!.Run, r.Summary.Ycsb!.Shape, r.Summary.Ycsb!.Distribution, r.Summary.Ycsb!.Rate, r.Summary.Ycsb!.Repetition))
            .Should().OnlyHaveUniqueItems();

        var zipfianC = results.First(r => r.Kind == YcsbRunKind.WorkloadC && r.Summary.Ycsb!.Distribution == "zipfian");
        zipfianC.Summary.Options.Distribution.Should().Be(KeyDistributionKind.Zipfian);
    }

    [Fact]
    public async Task Exactly_One_Repetition_Of_Each_Row_Is_The_Median_And_Every_Repetition_Is_Kept()
    {
        using var store = GetDocumentStore();

        var scenario = SetScenario("ravendb", 20) with { Repetitions = 3 };
        var results = await new YcsbRunner(scenario, Settings(store.Urls[0], store.Database)).RunAsync();

        foreach (var row in results.GroupBy(r => r.Identity.RowKey))
        {
            row.Should().HaveCount(row.First().Kind == YcsbRunKind.Load ? 1 : scenario.ResolvedRepetitions,
                "every repetition is kept as its own result");

            var marked = row.Where(r => r.Summary.Ycsb!.IsRowMedian).ToList();
            var publishable = row.Where(r => r.Summary.Steps.All(s => s.InvalidReason == null)).ToList();

            if (publishable.Count == 0)
            {
                marked.Should().BeEmpty("a row whose every repetition is client-bound carries no median");
                continue;
            }

            marked.Should().ContainSingle("the median is identifiable from one file alone");
            marked[0].Summary.Ycsb!.MedianStatistic.Should().Be(YcsbMedianSelector.StatisticName);
            YcsbMedianSelector.Statistic(marked[0].Summary.Steps).Should().Be(
                publishable.Select(r => YcsbMedianSelector.Statistic(r.Summary.Steps))
                    .OrderBy(v => v)
                    .ElementAt((publishable.Count - 1) / 2));
        }
    }

    [Fact]
    public async Task Every_Artifact_Path_Of_The_Larger_Set_Exists_And_Is_Its_Own()
    {
        using var store = GetDocumentStore();

        var results = await new YcsbRunner(SetScenario("ravendb", 20), Settings(store.Urls[0], store.Database)).RunAsync();

        var paths = results.SelectMany(r => r.Summary.HistogramArtifacts!)
            .SelectMany(a => new[] { a.HlogPath, a.CsvPath })
            .ToList();

        paths.Should().NotContainNulls();
        paths.Should().OnlyHaveUniqueItems("the whole identity is in the histogram prefix");
        foreach (var path in paths)
        {
            File.Exists(path).Should().BeTrue($"the result names '{path}'");
            new FileInfo(path!).Length.Should().BeGreaterThan(0);
        }

        results.Select(r => r.Identity.ResultName).Should().OnlyHaveUniqueItems("the result file names are distinguishable");
    }

    [RequiresMongoFact]
    public async Task Two_Insert_Stream_Runs_Of_One_Invocation_Address_Disjoint_Ids()
    {
        var database = "ycsb-inserts-" + Guid.NewGuid().ToString("N")[..8];

        try
        {
            var scenario = SetScenario(MongoYcsbTransport.MongoDbTarget, 20) with { Rates = null, Distributions = null };
            var results = await new YcsbRunner(scenario, Settings(MongoTestEndpoints.MongoConnectionString, database)).RunAsync();

            var inserts = results.Where(r => r.Kind == YcsbRunKind.InsertStream).ToList();
            inserts.Should().HaveCount(scenario.ResolvedRepetitions);

            // MongoDB rejects a duplicate _id, so a second run that reused the first run's starting
            // key would report errors instead of a measurement.
            inserts.Should().OnlyContain(r => r.Summary.Steps.All(s => s.ErrorRate == 0),
                "each insert-stream run starts above every id the previous one issued");
        }
        finally
        {
            using var cleanup = new MongoYcsbTransport(MongoTestEndpoints.MongoConnectionString, database, MongoYcsbTransport.MongoDbTarget);
            await cleanup.Documents.Database.Client.DropDatabaseAsync(database);
        }
    }
}
