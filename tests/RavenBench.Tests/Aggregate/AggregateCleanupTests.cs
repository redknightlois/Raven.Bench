using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Aggregate;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Aggregate;

/// <summary>The cleanup scope of an aggregate run and the aggregate parity check on a target that already holds data.</summary>
[Trait("Category", "Unit")]
public class AggregateCleanupTests
{
    private const string ForeignIndex = "Foreign/Index";

    /// <summary>An upsert store over a dictionary: a write over an existing id succeeds and replaces it.</summary>
    private sealed class FakeStore(IReadOnlyList<string> aggregateIndexNames) : IAggregateStore
    {
        public readonly Dictionary<string, AggregateDocument> Documents = new();
        public readonly HashSet<string> Indexes = new();

        public IReadOnlyList<string> AggregateIndexNames => aggregateIndexNames;

        public Task<IReadOnlyCollection<string>> ExistingIdsAsync(IReadOnlyList<string> ids, CancellationToken ct) =>
            Task.FromResult<IReadOnlyCollection<string>>(ids.Where(Documents.ContainsKey).ToList());

        public Task<IReadOnlyCollection<string>> IndexNamesAsync(CancellationToken ct) => Task.FromResult<IReadOnlyCollection<string>>(Indexes.ToList());

        public Task DeleteAsync(IReadOnlyList<string> ids, CancellationToken ct)
        {
            foreach (var id in ids)
                Documents.Remove(id);
            return Task.CompletedTask;
        }

        public Task DeleteIndexAsync(string name, CancellationToken ct)
        {
            Indexes.Remove(name);
            return Task.CompletedTask;
        }

        public void Load(IEnumerable<AggregateDocument> documents)
        {
            foreach (var d in documents)
                Documents[d.Id] = d;
            Indexes.UnionWith(aggregateIndexNames);
        }
    }

    private static readonly IReadOnlyList<string> RunIndexes = ["Aggregates/A", "Aggregates/B"];

    private static FakeStore SeededStore(IEnumerable<AggregateDocument> foreign)
    {
        var store = new FakeStore(RunIndexes);
        foreach (var d in foreign)
            store.Documents[d.Id] = d;
        store.Indexes.Add(ForeignIndex);
        store.Indexes.Add(RunIndexes[0]);
        return store;
    }

    private static List<AggregateDocument> Foreign(int count) => Enumerable.Range(1, count)
        .Select(i => new AggregateDocument($"foreign/{i}", "c01", "r01", i, "2026-01-01T00:00:00Z", "x")).ToList();

    [Fact]
    public async Task The_Run_Removes_What_It_Loaded_And_Created_And_Nothing_That_Existed_Before()
    {
        var foreign = Foreign(5);
        var store = SeededStore(foreign);
        var loaded = new AggregateParityCheck(7, 50).Sample();

        var footprint = await AggregateFootprint.BeforeLoadAsync(store, loaded.Select(d => d.Id), CancellationToken.None);
        store.Load(loaded);
        await footprint.RemoveAsync(CancellationToken.None);

        store.Documents.Keys.Should().BeEquivalentTo(foreign.Select(d => d.Id));
        store.Indexes.Should().BeEquivalentTo(new[] { ForeignIndex, RunIndexes[0] }, "an index that existed before the run stays");
    }

    [Fact]
    public async Task A_Target_That_Holds_An_Id_The_Run_Loads_Fails_Before_The_Load_And_Keeps_That_Document()
    {
        var loaded = new AggregateParityCheck(7, 50).Sample();
        var store = SeededStore([loaded[3]]);

        var act = () => AggregateFootprint.BeforeLoadAsync(store, loaded.Select(d => d.Id), CancellationToken.None);

        await act.Should().ThrowAsync<AggregateIdCollisionException>().WithMessage($"*{loaded[3].Id}*");
        store.Documents.Should().ContainKey(loaded[3].Id);
    }

    [Fact]
    public async Task The_Parity_Check_Keeps_Every_Foreign_Aggregate_Document_And_Index()
    {
        var foreign = Foreign(20);
        var store = SeededStore(foreign);
        int released = 0;
        var product = AggregateParityCheck.Product("fake", "stub", store,
            (sample, _) =>
            {
                store.Load(sample);
                return Task.CompletedTask;
            },
            (op, _) => Task.FromResult(new TransportResult(0, 0, indexName: op.IndexName, resultCount: 0, isStale: false)
            {
                Groups = AggregateOrdering.Compute(op, store.Documents.Values)
            }),
            () =>
            {
                released++;
                return Task.CompletedTask;
            });

        var report = await new AggregateParityCheck(7, 50).RunAsync([product], CancellationToken.None);

        report.Pairs.Should().OnlyContain(p => p.Failure == null, "the cleanup succeeded");
        store.Documents.Keys.Should().BeEquivalentTo(foreign.Select(d => d.Id));
        store.Indexes.Should().BeEquivalentTo(new[] { ForeignIndex, RunIndexes[0] });
        released.Should().Be(1);
    }
}
