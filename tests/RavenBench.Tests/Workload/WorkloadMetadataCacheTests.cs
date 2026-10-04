using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Workload;

public class WorkloadMetadataCacheTests
{
    private const string MetadataDocId = "workload/test-metadata";

    private sealed record Sample(int[] Ids);

    private sealed class InMemoryCache
    {
        public Dictionary<string, Sample> Stored { get; } = new();
        public int Discoveries { get; private set; }

        public Task<Sample> RunAsync(string documentId, Sample discovered) =>
            WorkloadMetadataCache.DiscoverOrLoadAsync<Sample>(
                id => Task.FromResult(Stored.GetValueOrDefault(id)),
                (id, metadata) => { Stored[id] = metadata; return Task.CompletedTask; },
                documentId,
                cached => cached.Ids.Length > 0,
                _ => { },
                () => { Discoveries++; return Task.FromResult(discovered); },
                _ => { });
    }

    [Fact]
    public async Task An_Incomplete_Cached_Document_Is_Replaced_By_Fresh_Discovery()
    {
        var cache = new InMemoryCache();
        var id = WorkloadMetadataCache.DocumentId(MetadataDocId, 42, 100, 1000);
        cache.Stored[id] = new Sample([]);
        var discovered = new Sample([1, 2, 3]);

        var result = await cache.RunAsync(id, discovered);

        result.Should().BeSameAs(discovered);
        cache.Discoveries.Should().Be(1);
        cache.Stored[id].Should().BeSameAs(discovered);
    }

    [Fact]
    public async Task A_Run_With_Other_Sampling_Inputs_Draws_Its_Own_Sample()
    {
        var cache = new InMemoryCache();
        var first = new Sample([1, 2]);
        var second = new Sample([7, 9]);

        await cache.RunAsync(WorkloadMetadataCache.DocumentId(MetadataDocId, 42, 100, 1000), first);
        var reused = await cache.RunAsync(WorkloadMetadataCache.DocumentId(MetadataDocId, 42, 100, 1000), second);
        var otherSeed = await cache.RunAsync(WorkloadMetadataCache.DocumentId(MetadataDocId, 43, 100, 1000), second);
        var otherSize = await cache.RunAsync(WorkloadMetadataCache.DocumentId(MetadataDocId, 42, 101, 1000), second);
        var otherRange = await cache.RunAsync(WorkloadMetadataCache.DocumentId(MetadataDocId, 42, 100, 1001), second);

        reused.Should().BeSameAs(first);
        otherSeed.Should().BeSameAs(second);
        otherSize.Should().BeSameAs(second);
        otherRange.Should().BeSameAs(second);
        cache.Discoveries.Should().Be(4);
    }
}
