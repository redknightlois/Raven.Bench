using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Dataset.Vectors;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Trait("Category", "Unit")]
public class VectorParityCleanupTests
{
    // An upsert store: a write over an existing id succeeds, so write success alone says nothing about what was there before.
    private static VectorParityProduct UpsertStore(Dictionary<string, float[]> store) => new(
        "upsert",
        _ => Task.FromResult(store.Count == 0 ? null : $"the store holds {store.Count} documents"),
        (sample, _) =>
        {
            foreach (var v in sample)
                store[v.Id] = v.Vector;
            return Task.CompletedTask;
        },
        (query, k, _) => Task.FromResult<IReadOnlyList<string>>(store.Keys.Take(k).ToList()),
        () =>
        {
            store.Clear();
            return Task.CompletedTask;
        });

    [Fact]
    public async Task ATargetThatAlreadyHoldsData_KeepsIt_AndTheCheckReportsWhy()
    {
        var seeded = Enumerable.Range(0, 20).ToDictionary(i => i.ToString(), i => new float[] { i, 1, 1, 1 });
        var store = new Dictionary<string, float[]>(seeded);

        var report = await new VectorParityCheck(seed: 1, baseCount: 20, queryCount: 2, dimensions: 4, k: 5).RunAsync([UpsertStore(store)], CancellationToken.None);

        Assert.NotNull(report.Results[0].Failure);
        Assert.Equal(seeded.Keys.Order(), store.Keys.Order());
        Assert.All(seeded, kv => Assert.Equal(kv.Value, store[kv.Key]));
    }

    [Fact]
    public async Task AnEmptyTarget_IsCleanedUpAfterTheCheck()
    {
        var store = new Dictionary<string, float[]>();
        await new VectorParityCheck(seed: 1, baseCount: 20, queryCount: 2, dimensions: 4, k: 5).RunAsync([UpsertStore(store)], CancellationToken.None);
        Assert.Empty(store);
    }
}
