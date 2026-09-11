using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
