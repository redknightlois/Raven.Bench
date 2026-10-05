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

        var held = await PacedWriter.RunAsync(1_000, 1, async (_, ct) =>
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
        var held = await PacedWriter.RunAsync(1_000, 1, (_, ct) => { tokens.Add(ct); return Task.FromResult(true); }, _ => { }, new CancellationToken(canceled: true));

        held.Acknowledged.Should().Be(1);
        tokens.Should().ContainSingle().Which.CanBeCanceled.Should().BeFalse();
    }

    [Fact]
    public async Task A_Writer_With_No_Document_Left_Stops_And_Says_Why()
    {
        int sent = 0;
        var held = await PacedWriter.RunAsync(1_000, 1, (_, _) => Task.FromResult(Interlocked.Increment(ref sent) <= 3), _ => { }, CancellationToken.None);

        held.Acknowledged.Should().Be(3);
        held.StopReason.Should().Contain("no document");
    }

    [Fact]
    public async Task Writers_Hold_The_Rate_Up_To_Their_Count_Over_The_Latency()
    {
        const int writers = 8;
        int inFlight = 0, peak = 0, calls = 0;
        var allBusy = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();
        async Task<bool> Write(int _, CancellationToken __)
        {
            Interlocked.Increment(ref calls);
            var now = Interlocked.Increment(ref inFlight);
            InterlockedMax(ref peak, now);
            if (now == writers)
                allBusy.TrySetResult();
            await release.Task;
            Interlocked.Decrement(ref inFlight);
            return true;
        }

        // Every slot is already due, so only the writer count bounds the writes in flight.
        var run = PacedWriter.RunAsync(double.MaxValue, writers, Write, _ => { }, stop.Token);
        await allBusy.Task;
        stop.Cancel();
        release.SetResult();
        var capped = await run;

        peak.Should().Be(writers);
        calls.Should().Be(writers, "a stopped writer sends nothing after its write in flight");
        capped.Acknowledged.Should().Be(writers);
        capped.Writers.Should().Be(writers);
        capped.HeldRequested.Should().BeFalse();
        capped.Shortfall.Should().Contain($"{writers} writer(s)");

        // The held rate is bounded by the writer count over the mean write latency.
        const double latencyMs = 20;
        var ceiling = writers * 1000 / latencyMs;
        var atCeiling = HeldWriteRate.Of(ceiling * 2, writers, acknowledged: (long)ceiling, seconds: 1, "the run ended", Enumerable.Repeat(latencyMs, (int)ceiling).ToArray());
        atCeiling.HeldRequested.Should().BeFalse();
        atCeiling.Shortfall.Should().Contain($"hold at most {ceiling:F0} updates/s");
        HeldWriteRate.Of(ceiling / 4, writers, acknowledged: (long)(ceiling / 4), seconds: 1, "the run ended", [latencyMs]).Shortfall.Should().BeNull();
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int seen;
        while ((seen = Volatile.Read(ref target)) < value && Interlocked.CompareExchange(ref target, value, seen) != seen) { }
    }

    [Fact]
    public void A_Held_Rate_Below_The_Requested_Rate_Is_Reported_With_Its_Reason()
    {
        var below = HeldWriteRate.Of(1_000, 4, acknowledged: 500, seconds: 1, "the run ended", [8.0]);
        below.HeldRequested.Should().BeFalse();
        below.Shortfall.Should().Contain("500 of 1000").And.Contain("the run ended");

        var at = HeldWriteRate.Of(1_000, 4, acknowledged: 1_000, seconds: 1, "the run ended", [2.0]);
        at.HeldRequested.Should().BeTrue();
        at.Shortfall.Should().BeNull();
    }

    [Fact]
    public async Task A_Failing_Write_Stops_Every_Writer_And_Propagates()
    {
        int sent = 0;
        var act = () => PacedWriter.RunAsync(1_000, 4, (_, _) => Interlocked.Increment(ref sent) == 5 ? throw new InvalidOperationException("boom") : Task.FromResult(true), _ => { }, CancellationToken.None);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("boom");
    }

    private static (List<AggregateDocument> Documents, Dictionary<string, long> Counts, string Tracked) SmallSet()
    {
        var documents = new AggregateDataSet(new AggregateDataSpec(5, 2_000, 64, 10, 20, new GroupDistribution(GroupDistribution.Uniform, null))).Generate().ToList();
        var counts = documents.GroupBy(d => d.Category).ToDictionary(g => g.Key, g => (long)g.Count());
        var tracked = AggregateOrdering.Top(counts.Select(c => new AggregateGroup(c.Key, c.Value)), 1)[0].Key;
        return (documents, counts, tracked);
    }

    private static long MaxOtherCount(IEnumerable<string> categories, string tracked) =>
        categories.Where(c => c != tracked).GroupBy(c => c).Max(g => (long)g.Count());

    [Fact]
    public void Bulk_Updates_Never_Touch_The_Tracked_Group_And_The_Probe_Only_Moves_Into_It()
    {
        var (documents, counts, tracked) = SmallSet();
        var bulk = UnderWriteSplit.BulkWriters(documents, counts, tracked, writers: 8, documentsPerWriter: 50, seed: 1);
        var category = documents.ToDictionary(d => d.Id, d => d.Category);

        var bulkIds = new HashSet<string>();
        for (int i = 0; i < 5_000; i++)
        {
            var op = bulk[i % bulk.Length].Next();
            category[op.Id].Should().NotBe(tracked);
            op.Category.Should().NotBe(tracked).And.NotBe(category[op.Id]);
            category[op.Id] = op.Category;
            bulkIds.Add(op.Id);
        }
        var originalCategory = documents.ToDictionary(d => d.Id, d => d.Category);
        foreach (var probe in UnderWriteSplit.ProbeSources(documents, tracked, 100))
        {
            originalCategory[probe].Should().NotBe(tracked);
            bulkIds.Should().NotContain(probe, "the probe and the bulk writers never share a document");
        }
    }

    [Fact]
    public void A_Probe_Supply_Short_Of_The_Step_Fails_Before_The_Step()
    {
        var (documents, _, tracked) = SmallSet();
        var supply = (documents.Count(d => d.Category != tracked) + 1) / 2;

        UnderWriteSplit.ProbeSources(documents, tracked, supply).Should().HaveCount(supply, "the probe takes every other document outside the tracked group");
        var act = () => UnderWriteSplit.ProbeSources(documents, tracked, supply + 5);

        act.Should().Throw<InvalidOperationException>().WithMessage($"*needs {supply + 5}*has {supply}*5 short*");
    }

    [Fact]
    public async Task A_Probe_Out_Of_Documents_Stops_Without_An_Error_And_Says_How_Many_It_Sent()
    {
        var (_, _, tracked) = SmallSet();
        IReadOnlyList<string> ids = ["a/1", "a/2", "a/3"];
        var sent = new List<AggregateUpdateOperation>();
        var write = UnderWriteSplit.ProbeWrite(ids, tracked, seed: 1, (op, _) =>
        {
            sent.Add(op);
            return Task.CompletedTask;
        });

        // Every slot is already due and nothing stops the writer, so only exhaustion ends it.
        var probe = await PacedWriter.RunAsync(double.MaxValue, 1, write, _ => { }, CancellationToken.None);

        sent.Select(op => op.Id).Should().Equal(ids);
        sent.Should().OnlyContain(op => op.Category == tracked);
        probe.Acknowledged.Should().Be(ids.Count);
        probe.StopReason.Should().Contain("no document");
    }

    [Theory]
    [InlineData(100L, 100L, true)]
    [InlineData(103L, 100L, false)]
    public async Task The_Freshness_Baseline_Is_The_Server_Answer_And_A_Different_Answer_Fails_Fast(long served, long generated, bool accepted)
    {
        using var transport = new FixedAnswerTransport([new AggregateGroup("c01", served)]);

        var act = () => AggregateRunner.ServerBaselineAsync(transport, "c01", generated, CancellationToken.None);

        if (accepted)
            (await act()).Should().Be(served);
        else
            await act.Should().ThrowAsync<InvalidDataException>().WithMessage("*did not load*");
        transport.Calls.Should().Be(1);
    }

    [Fact]
    public async Task A_Freshness_Baseline_Where_Another_Group_Leads_Fails_Fast()
    {
        using var transport = new FixedAnswerTransport([new AggregateGroup("c99", 500)]);

        await FluentActions.Awaiting(() => AggregateRunner.ServerBaselineAsync(transport, "c01", 100, CancellationToken.None))
            .Should().ThrowAsync<InvalidDataException>();
    }

    private sealed class FixedAnswerTransport(IReadOnlyList<AggregateGroup> groups) : IYcsbTransport
    {
        public int Calls;
        public string ProductName => "fake";
        public bool ReportsWireBytes => false;
        public string RecordedEndpoint => "stub";

        public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
        {
            Interlocked.Increment(ref Calls);
            return Task.FromResult(new TransportResult(0, 0, indexName: "idx", resultCount: groups.Count, isStale: false) { Groups = groups });
        }

        public Task PutAsync<T>(string id, T document) => Task.CompletedTask;
        public Task EnsureDatabaseExistsAsync(string databaseName) => Task.CompletedTask;
        public Task<long> GetDocumentCountAsync(string idPrefix) => Task.FromResult(0L);
        public Task<string> GetServerVersionAsync() => Task.FromResult("0");
        public void Dispose() { }
    }

    [Theory]
    [InlineData(2.0)]
    [InlineData(10.0)]
    [InlineData(1234.56)]
    public void The_Fixed_Rate_Targets_A_Named_Fraction_Below_The_Closed_Loop_Throughput(double closed)
    {
        FixedRate.Fraction.Should().BeGreaterThan(0).And.BeLessThan(1);

        var rate = FixedRate.For(closed);

        rate.Should().BeGreaterThan(0);
        ((double)rate!).Should().BeLessThan(closed).And.BeLessThanOrEqualTo(closed * FixedRate.Fraction);
    }

    [Fact]
    public void A_Closed_Loop_Too_Slow_For_A_Whole_Rate_Below_It_Has_No_Fixed_Rate()
    {
        FixedRate.For(1.0).Should().BeNull();
        FixedRate.For(1.25).Should().Be(1);
    }

    [Fact]
    public void The_Probe_Source_Holds_Only_The_Ids_The_Step_Can_Issue()
    {
        var (documents, _, tracked) = SmallSet();
        const int probes = 10;
        var category = documents.ToDictionary(d => d.Id, d => d.Category);

        var sources = UnderWriteSplit.ProbeSources(documents, tracked, probes);

        sources.Should().HaveCount(probes, "the source holds no more documents than the step can probe");
        sources.Should().OnlyHaveUniqueItems().And.OnlyContain(id => category[id] != tracked);
    }

    private sealed class RunFailed : Exception;

    private sealed class CleanupFailed : Exception;

    [Fact]
    public async Task A_Cleanup_Failure_After_A_Run_Failure_Never_Replaces_The_Run_Failure()
    {
        int cleanups = 0;

        var act = () => RunCleanup.AfterAsync<int>(() =>
        {
            cleanups++;
            throw new CleanupFailed();
        }, "Aggregate", () => throw new RunFailed());

        await act.Should().ThrowAsync<RunFailed>();
        cleanups.Should().Be(1);
    }

    [Fact]
    public async Task A_Ramp_Fault_Awaits_The_Writers_And_Propagates_Over_A_Writer_Fault()
    {
        using var stop = new CancellationTokenSource();
        var writerStopped = new TaskCompletionSource();
        var writer = Task.Run(async () =>
        {
            await Task.Delay(Timeout.Infinite, stop.Token).ContinueWith(_ => { }, TaskScheduler.Default);
            writerStopped.SetResult();
            throw new InvalidOperationException("writer");
        });
        var probe = Task.Run(() => Task.Delay(Timeout.Infinite, stop.Token));

        var act = () => AggregateRunner.AlongsideAsync<int>(Task.FromException<int>(new TimeoutException("ramp")), stop, writer, probe);

        (await act.Should().ThrowAsync<TimeoutException>()).WithMessage("ramp");
        writerStopped.Task.IsCompleted.Should().BeTrue();
        writer.IsFaulted.Should().BeTrue();
        probe.IsCanceled.Should().BeTrue();
    }

    [Fact]
    public void Too_Many_Bulk_Writers_For_The_Room_Below_The_Tracked_Group_Fail_Fast()
    {
        var (documents, counts, tracked) = SmallSet();
        var room = counts.Where(c => c.Key != tracked).Sum(c => Math.Max(0, counts[tracked] - c.Value - 1));

        var act = () => UnderWriteSplit.BulkWriters(documents, counts, tracked, writers: (int)room + 1, documentsPerWriter: 1, seed: 1);

        act.Should().Throw<InvalidOperationException>().WithMessage("*bulk writer*");
    }

    [Fact]
    public async Task The_Tracked_Count_Is_The_Baseline_Plus_K_Under_A_Concurrent_Bulk_Load()
    {
        var (documents, counts, tracked) = SmallSet();
        const int writers = 8;
        var bulk = UnderWriteSplit.BulkWriters(documents, counts, tracked, writers, documentsPerWriter: 50, seed: 1);
        using var probeSources = UnderWriteSplit.ProbeSources(documents, tracked, 400).GetEnumerator();
        var store = new ConcurrentDictionary<string, string>(documents.ToDictionary(d => d.Id, d => d.Category));
        long Count(string group) => store.Values.Count(c => c == group);
        var baseline = counts[tracked];
        int probesInFlight = 0, maxProbesInFlight = 0;
        var violations = new ConcurrentQueue<string>();

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var bulkRun = PacedWriter.RunAsync(20_000, writers, async (w, _) =>
        {
            var op = bulk[w].Next();
            await Task.Yield();
            store[op.Id] = op.Category;
            return true;
        }, _ =>
        {
            var snapshot = store.Values.ToList();
            if (MaxOtherCount(snapshot, tracked) >= snapshot.Count(c => c == tracked))
                violations.Enqueue("a bulk write put another group level with or above the tracked group");
        }, stop.Token);
        long k = 0;
        var probeRun = PacedWriter.RunAsync(200, 1, async (_, _) =>
        {
            var inFlight = Interlocked.Increment(ref probesInFlight);
            maxProbesInFlight = Math.Max(maxProbesInFlight, inFlight);
            probeSources.MoveNext().Should().BeTrue();
            await Task.Yield();
            store[probeSources.Current] = tracked;
            Interlocked.Decrement(ref probesInFlight);
            return true;
        }, _ =>
        {
            k++;
            if (Count(tracked) != baseline + k)
                violations.Enqueue($"after probe write {k} the tracked count is {Count(tracked)}");
        }, stop.Token);
        var (bulkHeld, probeHeld) = (await bulkRun, await probeRun);

        bulkHeld.Acknowledged.Should().BeGreaterThan(probeHeld.Acknowledged);
        violations.Should().BeEmpty();
        maxProbesInFlight.Should().Be(1);
        Count(tracked).Should().Be(baseline + probeHeld.Acknowledged);
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
        public string RecordedEndpoint => "stub";

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
    [InlineData("writers")]
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
    [InlineData("writers", "0")]
    [InlineData("writers", "-1")]
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
        var settings = new AggregateSettings { DocumentCount = 500, WriteRate = 50, Writers = 7 };

        var (resolved, overrides) = AggregateScenarioResolver.Resolve(file, settings, ["aggregate", "--documents", "500", "--write-rate=50", "--writers", "7"]);

        resolved.DocumentCount.Should().Be(500);
        resolved.WriteRate.Should().Be(50);
        resolved.Writers.Should().Be(7);
        overrides.Should().Equal(new Dictionary<string, string> { ["--documents"] = "500", ["--write-rate"] = "50", ["--writers"] = "7" });
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
        Concurrency = 2, WriteRate = 50, Writers = 4, UnderWriteQueryRate = 40, Warmup = "200ms", Duration = "1s", NonStaleTimeout = "2m", DataDirectory = dataDirectory
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
            info.FixedRate.Should().BeLessThan((int)Math.Ceiling(info.ClosedLoopRate));
            info.FixedRateFraction.Should().Be(FixedRate.Fraction);
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
        underWrite.Writer.Writers.Should().Be(4);
        underWrite.Probe.Acknowledged.Should().BeGreaterThan(0);
        underWrite.Probe.Writers.Should().Be(1);
        var freshness = underWrite.Freshness;
        freshness.Writes.Should().Be(underWrite.Probe.Acknowledged, "freshness comes from the probe alone");
        freshness.AnswersWithTrackedGroupNotFirst.Should().Be(0);
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
