using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class OperationStreamReproducibilityTests
{
    private const int DocumentSize = 1024;

    [Fact]
    public void Same_Seed_Draws_The_Same_Operation_Stream()
    {
        // INVARIANT: one seed, one stream: same kinds, same ids, same bytes, in the same order.
        var first = DrawStream(seed: 42, count: 300);
        var second = DrawStream(seed: 42, count: 300);

        first.Should().Equal(second);
        first.Select(op => op.Kind).Distinct().Should().BeEquivalentTo(new[] { "read", "insert", "update" });
    }

    [Fact]
    public void Different_Seed_Draws_A_Different_Operation_Stream()
    {
        DrawStream(seed: 42, count: 300).Should().NotEqual(DrawStream(seed: 43, count: 300));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public async Task Insert_Content_At_An_Id_Is_The_Same_At_Any_Worker_Count(int workerCount)
    {
        // INVARIANT: real workers, each with its own source, share one keyspace counter, so which
        // worker draws which id depends on scheduling. What is written at an id must not: the
        // content comes from (seed, id, size) and never from the draw position.
        const int seed = 21;
        const int insertsPerWorker = 25;
        var workload = new WriteWorkload(DocumentSize, seed);
        var observed = new ConcurrentDictionary<string, string>();

        await Task.WhenAll(Enumerable.Range(0, workerCount).Select(worker => Task.Run(() =>
        {
            var rng = new Random(1000 + worker);
            for (int i = 0; i < insertsPerWorker; i++)
            {
                var op = (InsertOperation<string>)workload.NextOperation(rng);
                observed[op.Id] = op.Payload;
            }
        })));

        observed.Should().HaveCount(workerCount * insertsPerWorker);
        foreach (var (id, payload) in observed)
            payload.Should().Be(PayloadGenerator.Generate(seed, id, DocumentSize));
    }

    [Fact]
    public void Read_Heavy_Mix_Draws_More_Reads_Than_A_Balanced_Mix()
    {
        // INVARIANT: the configured mix is honoured, asserted as a relationship between two
        // mixes over one sample size rather than as a count.
        const int sampleSize = 2000;
        var readHeavy = NewMixedWorkload(WorkloadMix.FromWeights(95, 0, 5), seed: 42);
        var balanced = NewMixedWorkload(WorkloadMix.FromWeights(50, 0, 50), seed: 42);

        var readHeavyRng = new Random(1);
        var balancedRng = new Random(1);
        int readHeavyReads = 0, balancedReads = 0;
        for (int i = 0; i < sampleSize; i++)
        {
            if (readHeavy.NextOperation(readHeavyRng) is ReadOperation)
                readHeavyReads++;
            if (balanced.NextOperation(balancedRng) is ReadOperation)
                balancedReads++;
        }

        readHeavyReads.Should().BeGreaterThan(balancedReads);
    }

    [Fact]
    public async Task Closed_Loop_Operation_Sequence_Does_Not_Depend_On_Concurrency()
    {
        // INVARIANT: one producer thread draws every operation from one shared source, so the
        // sequence is the same at any concurrency; only which worker executes an operation changes.
        var low = await DrawClosedLoop(concurrency: 1, seed: 42);
        var high = await DrawClosedLoop(concurrency: 8, seed: 42);

        low.Should().NotBeEmpty();
        high.Should().NotBeEmpty();
        var (shorter, longer) = low.Count <= high.Count ? (low, high) : (high, low);
        longer.Take(shorter.Count).Should().Equal(shorter);
    }

    [Fact]
    public async Task Rate_Workers_Seed_From_The_Run_Source_Reproducibly()
    {
        // INVARIANT: each worker's source is a successive draw from the run-seeded source, in
        // worker order, so the same configuration at the same worker count replays each worker's
        // stream. An unseeded source or a seed + workerIndex derivation fails this.
        var first = await DrawRateWorkerSeeds(seed: 42, workers: 3);
        var second = await DrawRateWorkerSeeds(seed: 42, workers: 3);

        first.Should().HaveCount(3);
        first.Should().Equal(second);
    }

    private static async Task<IReadOnlyList<(string Kind, string Id, string? Field, string? Value)>> DrawClosedLoop(int concurrency, int seed)
    {
        var recording = new RecordingWorkload(NewMixedWorkload(WorkloadMix.FromWeights(40, 30, 30), seed));
        var generator = new ClosedLoopLoadGenerator(new TestTransport(baseLatencyMs: 0), recording, concurrency, new Random(seed));
        await generator.ExecuteMeasurementAsync(TimeSpan.FromMilliseconds(250), CancellationToken.None);
        return recording.Drawn;
    }

    private static async Task<IReadOnlyList<int>> DrawRateWorkerSeeds(int seed, int workers)
    {
        var source = new RecordingRandom(seed);
        var generator = new RateLoadGenerator(new TestTransport(baseLatencyMs: 0), new ConstantReadWorkload(), targetRps: 100, maxConcurrency: workers, source);
        await generator.ExecuteMeasurementAsync(TimeSpan.FromMilliseconds(50), CancellationToken.None);
        return source.Draws;
    }

    private sealed class RecordingWorkload : IWorkload
    {
        private readonly IWorkload _inner;
        private readonly List<(string Kind, string Id, string? Field, string? Value)> _drawn = new();

        public RecordingWorkload(IWorkload inner) => _inner = inner;

        public IReadOnlyList<(string Kind, string Id, string? Field, string? Value)> Drawn => _drawn;

        public OperationBase NextOperation(Random rng)
        {
            var operation = _inner.NextOperation(rng);
            _drawn.Add(Describe(operation));
            return operation;
        }
    }

    private sealed class ConstantReadWorkload : IWorkload
    {
        public OperationBase NextOperation(Random rng) => new ReadOperation { Id = "bench/00000001" };
    }

    private sealed class RecordingRandom : Random
    {
        private readonly List<int> _draws = new();

        public RecordingRandom(int seed) : base(seed)
        {
        }

        public IReadOnlyList<int> Draws => _draws;

        public override int Next()
        {
            var value = base.Next();
            _draws.Add(value);
            return value;
        }
    }

    private static IWorkload NewMixedWorkload(WorkloadMix mix, int seed) =>
        new MixedProfileWorkload(mix, new UniformDistribution(), DocumentSize, seed, initialKeyspace: 100);

    /// <summary>
    /// Draws a stream the way a run does: the document seed and the operation-stream source both
    /// come from the one run seed.
    /// </summary>
    private static List<(string Kind, string Id, string? Field, string? Value)> DrawStream(int seed, int count)
    {
        var workload = NewMixedWorkload(WorkloadMix.FromWeights(40, 30, 30), seed);
        var rng = new Random(seed);
        return Enumerable.Range(0, count).Select(_ => Describe(workload.NextOperation(rng))).ToList();
    }

    private static (string Kind, string Id, string? Field, string? Value) Describe(OperationBase op) => op switch
    {
        ReadOperation read => ("read", read.Id, null, null),
        InsertOperation<string> insert => ("insert", insert.Id, null, insert.Payload),
        UpdateFieldOperation update => ("update", update.Id, update.FieldName, update.Value),
        _ => throw new InvalidOperationException($"Unexpected operation kind {op.GetType().Name}.")
    };
}
