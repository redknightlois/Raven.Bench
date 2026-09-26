using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Aggregate;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Aggregate;

/// <summary>The aggregate runs: freshness, the paced writer, stale counts, the scenario and its overrides, and the RavenDB query request.</summary>
public class AggregateRunTests
{
    private static long Ms(double ms) => (long)(ms * Stopwatch.Frequency / 1000.0);

    private static IReadOnlyList<AggregateGroup> Answer(long tracked) => [new("c01", tracked), new("c02", 5)];

    [Fact]
    public void Freshness_Is_The_Time_From_Acknowledgement_To_The_First_Answer_That_Reflects_The_Write()
    {
        var tracker = new FreshnessTracker("c01", baseline: 10);
        tracker.Answered(Answer(10), Ms(5));    // computed before the write: reflects nothing
        tracker.Acknowledged(Ms(10));
        tracker.Answered(Answer(10), Ms(20));   // after the acknowledgement, still the old value
        tracker.Answered(Answer(11), Ms(35));   // first reflection of write 1
        tracker.Answered(Answer(11), Ms(50));

        var info = tracker.Complete();

        info.Observed.Should().Be(1);
        info.Unobserved.Should().Be(0);
        info.Distribution.P50.Should().BeApproximately(25, 0.01);
    }

    [Fact]
    public void An_Answer_Received_Before_The_Acknowledgement_Is_Not_A_Reflection()
    {
        var tracker = new FreshnessTracker("c01", baseline: 0);
        tracker.Answered(Answer(1), Ms(8));
        tracker.Acknowledged(Ms(10));
        tracker.Answered(Answer(1), Ms(12));

        var info = tracker.Complete();

        info.Observed.Should().Be(1);
        info.Distribution.P50.Should().BeApproximately(2, 0.01);
    }

    [Fact]
    public void A_Write_No_Answer_Reflected_Is_Unobserved_And_Out_Of_The_Percentiles()
    {
        var tracker = new FreshnessTracker("c01", baseline: 100);
        tracker.Acknowledged(Ms(0));
        tracker.Acknowledged(Ms(10));
        tracker.Acknowledged(Ms(20));
        tracker.Answered(Answer(101), Ms(4));
        tracker.Answered(Answer(102), Ms(30));
        tracker.Answered([new AggregateGroup("c02", 500)], Ms(40)); // no tracked group: reflects nothing

        var info = tracker.Complete();

        info.Writes.Should().Be(3);
        info.Observed.Should().Be(2);
        info.Unobserved.Should().Be(1);
        info.Distribution.Count.Should().Be(2);
        info.Distribution.P50.Should().BeApproximately(4, 0.01);
        info.Distribution.Max.Should().BeApproximately(20, 0.01);
    }

    [Fact]
    public void An_Answer_Reflecting_A_Later_Write_Reflects_Every_Earlier_Write()
    {
        var tracker = new FreshnessTracker("c01", baseline: 0);
        tracker.Acknowledged(Ms(0));
        tracker.Acknowledged(Ms(1));
        tracker.Answered(Answer(2), Ms(6));

        var info = tracker.Complete();

        info.Observed.Should().Be(2);
        info.Distribution.P50.Should().BeApproximately(5, 0.01);
        info.Distribution.Max.Should().BeApproximately(6, 0.01);
    }

    [Fact]
    public async Task A_Writer_That_Cannot_Hold_The_Rate_Reports_The_Rate_It_Held()
    {
        using var stop = new CancellationTokenSource(TimeSpan.FromMilliseconds(600));
        var acks = new ConcurrentQueue<long>();

        var held = await PacedWriter.RunAsync(1_000, async (_, ct) =>
        {
            await Task.Delay(20, CancellationToken.None);
            return true;
        }, acks.Enqueue, stop.Token);

        held.RequestedPerSecond.Should().Be(1_000);
        held.HeldPerSecond.Should().BeLessThan(held.RequestedPerSecond / 2);
        held.HeldRequested.Should().BeFalse();
        held.Acknowledged.Should().Be(acks.Count);
        held.HeldPerSecond.Should().BeApproximately(held.Acknowledged / held.WriterSeconds, 1e-9);
    }

    [Fact]
    public async Task A_Writer_Stopped_Before_It_Starts_Still_Acknowledges_Its_First_Write()
    {
        var tokens = new List<CancellationToken>();
        var held = await PacedWriter.RunAsync(1_000, (_, ct) => { tokens.Add(ct); return Task.FromResult(true); }, _ => { }, new CancellationToken(canceled: true));

        held.Acknowledged.Should().Be(1);
        tokens.Should().ContainSingle().Which.CanBeCanceled.Should().BeFalse();
    }

    [Fact]
    public async Task A_Writer_With_No_Document_Left_Stops_And_Says_Why()
    {
        var held = await PacedWriter.RunAsync(1_000, (n, _) => Task.FromResult(n < 3), _ => { }, CancellationToken.None);

        held.Acknowledged.Should().Be(3);
        held.StopReason.Should().Contain("no document");
    }

    [Fact]
    public async Task Stale_Counts_Per_Step_Come_From_Each_Response_Flag()
    {
        using var transport = new StaleEveryThirdTransport();
        var opts = new RunOptions
        {
            Url = "http://fake", Database = "fake", Seed = 1, Warmup = TimeSpan.FromMilliseconds(100), Duration = TimeSpan.FromMilliseconds(500),
            Shape = LoadShape.Closed, Step = new StepPlan(2, 2, 2.0)
        };
        var workload = new AggregateQueryWorkload(() => AggregateShapes.Create(AggregateShapes.CountByCategory, 5));
        var executor = new BenchmarkExecutor(opts, transport, workload, new ProcessCpuTracker(), serverTracker: null, "stale", null);

        var ramp = await BenchmarkRunner.RunRampAsync(opts, transport, executor, workload, null, new Random(1));
        var answers = AggregateStepAnswers.From(0, ramp.Steps[^1]);

        answers.Answers.Should().BeGreaterThan(3);
        answers.StaleAnswers.Should().BeGreaterThan(0).And.BeLessThan(answers.Answers);

        var fresh = AggregateStepAnswers.From(0, new StepResult { QueryOperations = 12 });
        fresh.StaleAnswers.Should().Be(0, "a step whose responses were never stale records a zero, not an absent count");
    }

    private sealed class StaleEveryThirdTransport : IYcsbTransport
    {
        private long _n;
        public string ProductName => "fake";
        public bool ReportsWireBytes => false;

        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) =>
            Task.FromResult(new TransportResult(0, 0, indexName: "idx", resultCount: 1, isStale: Interlocked.Increment(ref _n) % 3 == 0) { Groups = [new AggregateGroup("c1", 1)] });

        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<string> GetServerVersionAsync() => Task.FromResult("0");
        public void Dispose() { }
    }

    private static JsonObject DefaultScenario() =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(RepositoryRootLocator.Find(), "benchmarks", "aggregate", "scenario.json")))!.AsObject();

    [Fact]
    public void The_Default_Scenario_Holds_Every_Parameter_And_Validates()
    {
        var scenario = AggregateScenario.FromJson(JsonDocument.Parse(DefaultScenario().ToJsonString()).RootElement);
        scenario.DocumentCount.Should().Be(10_000_000);
        scenario.CategoryCardinality.Should().Be(100);
        scenario.RegionCardinality.Should().Be(10_000);
        scenario.RegionTopN.Should().Be(100);
        scenario.WriteRate.Should().Be(2_000);
        Path.IsPathRooted(Environment.ExpandEnvironmentVariables(scenario.DataDirectory)).Should().BeTrue();
    }

    [Theory]
    [InlineData("writeRate")]
    [InlineData("duration")]
    [InlineData("dataDirectory")]
    [InlineData("documentCount")]
    [InlineData("categoryCardinality")]
    [InlineData("distributionExponent")]
    public void A_Missing_Parameter_Throws_Naming_It(string key)
    {
        var json = DefaultScenario();
        json.Remove(key);

        var act = () => AggregateScenario.FromJson(JsonDocument.Parse(json.ToJsonString()).RootElement);

        act.Should().Throw<AggregateScenarioException>().Which.Parameter.Should().Be(key);
    }

    [Theory]
    [InlineData("regionCardinality", "0")]
    [InlineData("distribution", "\"pareto\"")]
    [InlineData("writeRate", "0")]
    [InlineData("nosuchKey", "1")]
    public void An_Invalid_Or_Unknown_Parameter_Throws_Naming_It(string key, string value)
    {
        var json = DefaultScenario();
        json[key] = JsonNode.Parse(value);

        var act = () => AggregateScenario.FromJson(JsonDocument.Parse(json.ToJsonString()).RootElement);

        act.Should().Throw<AggregateScenarioException>().Which.Parameter.Should().Be(key);
    }

    [Fact]
    public void A_Data_Directory_Inside_The_Repository_Is_Refused()
    {
        var root = RepositoryRootLocator.Find();
        var scenario = AggregateScenario.FromJson(JsonDocument.Parse(DefaultScenario().ToJsonString()).RootElement) with { DataDirectory = Path.Combine(root, "data") };

        var act = () => scenario.ResolveDataDirectory(root);

        act.Should().Throw<AggregateScenarioException>().Which.Parameter.Should().Be("dataDirectory");
    }

    [Fact]
    public void An_Override_Is_Recorded_And_Resolved()
    {
        var file = AggregateScenario.FromJson(JsonDocument.Parse(DefaultScenario().ToJsonString()).RootElement);
        var settings = new AggregateSettings { DocumentCount = 500, WriteRate = 50 };

        var (resolved, overrides) = AggregateScenarioResolver.Resolve(file, settings, ["aggregate", "--documents", "500", "--write-rate=50"]);

        resolved.DocumentCount.Should().Be(500);
        resolved.WriteRate.Should().Be(50);
        overrides.Should().Equal(new Dictionary<string, string> { ["--documents"] = "500", ["--write-rate"] = "50" });
    }

    [Fact]
    public async Task The_RavenDB_Aggregate_Query_Never_Asks_The_Server_To_Wait_For_A_Non_Stale_Result()
    {
        var port = FreeTcpPort();
        using var listener = new HttpListener();
        listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        listener.Start();
        var seen = new ConcurrentQueue<string>();
        var server = Task.Run(async () =>
        {
            var context = await listener.GetContextAsync();
            using (var reader = new StreamReader(context.Request.InputStream))
                seen.Enqueue(context.Request.RawUrl + "\n" + await reader.ReadToEndAsync());
            var body = Encoding.UTF8.GetBytes("{\"IndexName\":\"Aggregate/Count/By/category\",\"IsStale\":true,\"TotalResults\":1,\"Results\":[{\"category\":\"c1\",\"value\":3}]}");
            context.Response.ContentType = "application/json";
            await context.Response.OutputStream.WriteAsync(body);
            context.Response.Close();
        });

        using var transport = new RawHttpTransport($"http://127.0.0.1:{port}", "db", CompressionMode.Identity, HttpVersion.Version11);
        var result = await transport.ExecuteAsync(AggregateShapes.Create(AggregateShapes.CountByCategory, 5), CancellationToken.None);
        await server;

        result.IsStale.Should().BeTrue("the stale flag comes from the server response");
        seen.Should().ContainSingle().Which.Should().NotContainEquivalentOf("waitForNonStale");
    }

    private static int FreeTcpPort()
    {
        using var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        return ((IPEndPoint)probe.LocalEndpoint).Port;
    }
}

/// <summary>The whole aggregate command against the live servers over a small set; each test uses and deletes its own database.</summary>
[Collection(LiveServers.Name)]
public class AggregateRunnerLiveTests
{
    private static AggregateScenario Small(string dataDirectory) => new()
    {
        Seed = 3, DocumentCount = 600, DocumentSize = 256, CategoryCardinality = 10, RegionCardinality = 50,
        Distribution = GroupDistribution.Uniform, DistributionExponent = null, CountTopN = 5, RegionTopN = 20, FilterSelectivity = 0.1,
        Concurrency = 2, WriteRate = 50, UnderWriteQueryRate = 40, Warmup = "200ms", Duration = "1s", NonStaleTimeout = "2m", DataDirectory = dataDirectory
    };

    private static async Task<List<AggregateRunResult>> RunAsync(string target, string url)
    {
        var data = Directory.CreateTempSubdirectory("aggregate-data-").FullName;
        try
        {
            var settings = new AggregateSettings { Target = target, Url = url, Database = $"aggregate_test_{Guid.NewGuid():N}" };
            return await new AggregateRunner(Small(data), new Dictionary<string, string>(), settings).RunAsync();
        }
        finally
        {
            Directory.Delete(data, recursive: true);
        }
    }

    private static void AssertRuns(List<AggregateRunResult> results, string target, bool staleMarked)
    {
        results.Select(r => r.Run).Should().Equal(AggregateRunner.Runs);
        results.Should().OnlyContain(r => r.Summary.Aggregate!.Target == target && r.Summary.Aggregate.DataSet.Count == 600);

        var build = results[0].Summary.Aggregate!.Build!;
        build.ServerCpuPercent.Unavailable.Should().NotBeNullOrEmpty("no node_exporter is configured, so the figure names why it is missing");
        build.ServerCpuPercent.Value.Should().BeNull();

        foreach (var query in results.Skip(1).Take(3).Select(r => r.Summary))
        {
            var info = query.Aggregate!.Query!;
            info.FixedRate.Should().Be(Math.Max(1, (int)Math.Floor(info.ClosedLoopRate)));
            query.Steps[^1].TargetThroughput.Should().Be(info.FixedRate);
            // Workers drain every scheduled request before a step ends, so a slow server delays answers but does not remove them.
            info.StepAnswers.Should().OnlyContain(a => a.Answers > 0);
            // Only RavenDB serves a shape from a named index; the name does not depend on the filter value.
            var expectedIndex = staleMarked ? AggregateShapes.Create(info.Shape, info.TopN, "any").IndexName : null;
            info.IndexName.Should().Be(expectedIndex);
            JsonSerializer.SerializeToNode(info)!["IndexName"]?.GetValue<string>().Should().Be(expectedIndex);
        }

        var underWrite = results[^1].Summary.Aggregate!.UnderWrite!;
        // The writer always sends and awaits its first write, so one acknowledgement does not depend on the host's speed.
        underWrite.Writer.Acknowledged.Should().BeGreaterThan(0);
        var freshness = underWrite.Freshness;
        freshness.Writes.Should().Be(underWrite.Writer.Acknowledged);
        (freshness.Observed + freshness.Unobserved).Should().Be(freshness.Writes, "every acknowledged write is observed or counted as unobserved");
        // How many writes an index catches up with inside the run depends on the host, so the check is on what the answers produced, not on a count.
        freshness.Distribution.Count.Should().Be(freshness.Observed, "every freshness value comes from an answer");
        if (freshness.Observed > 0)
            freshness.Distribution.P50.Should().BeGreaterThanOrEqualTo(0);
        else
            freshness.Distribution.P50.Should().BeNull();
        if (staleMarked == false)
            results.Skip(1).SelectMany(r => (r.Summary.Aggregate!.Query?.StepAnswers ?? r.Summary.Aggregate.UnderWrite!.StepAnswers)).Should().OnlyContain(a => a.StaleAnswers == 0);
    }

    [RequiresMongoFact]
    public async Task Mongodb_And_Mongodb_Indexed_Complete_The_Five_Runs()
    {
        AssertRuns(await RunAsync(MongoYcsbTransport.MongoDbTarget, MongoTestEndpoints.MongoConnectionString), MongoYcsbTransport.MongoDbTarget, staleMarked: false);
        var indexed = await RunAsync(MongoYcsbTransport.MongoDbIndexedTarget, MongoTestEndpoints.MongoConnectionString);
        AssertRuns(indexed, MongoYcsbTransport.MongoDbIndexedTarget, staleMarked: false);
        indexed[0].Summary.Aggregate!.Build!.Indexes.Should().HaveCount(AggregateShapes.MongoIndexes().Count + 1);
    }

    [RequiresRavenDbFact(8081)]
    public async Task Ravendb_Completes_The_Five_Runs()
    {
        AssertRuns(await RunAsync(AggregateRunner.RavendbTarget, "http://localhost:8081"), AggregateRunner.RavendbTarget, staleMarked: true);
    }
}
