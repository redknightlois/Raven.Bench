using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RavenBench.Dataset;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Trait("Category", "Unit")]
public class SphereImportSegmentsTests
{
    // Holds written items until disposed, as a bulk insert holds documents until it commits.
    private sealed class FakeBulkInsert(List<int> committed) : IAsyncDisposable
    {
        private readonly List<int> _pending = [];

        public void Store(int item) => _pending.Add(item);

        public ValueTask DisposeAsync()
        {
            committed.AddRange(_pending);
            _pending.Clear();
            return ValueTask.CompletedTask;
        }
    }

    private static async IAsyncEnumerable<int> Items(int count)
    {
        for (var i = 1; i <= count; i++)
        {
            await Task.Yield();
            yield return i;
        }
    }

    [Fact]
    public async Task Every_Checkpoint_Records_A_Position_Already_Committed()
    {
        var committed = new List<int>();
        var checkpoints = new List<(int Position, int Committed)>();

        await SphereDatasetProvider.ImportInSegmentsAsync(Items(25), 10, () => new FakeBulkInsert(committed),
            (writer, item) => { writer.Store(item); return Task.CompletedTask; },
            item => { checkpoints.Add((item, committed.Count)); return Task.CompletedTask; });

        Assert.Equal(25, committed.Count);
        Assert.Equal(2, checkpoints.Count);
        Assert.All(checkpoints, c => Assert.True(c.Position <= c.Committed, $"checkpoint {c.Position} ahead of {c.Committed} committed"));
    }
}
