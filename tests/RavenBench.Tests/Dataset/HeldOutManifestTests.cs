using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Trait("Category", "Integration")]
public class HeldOutManifestTests : EmbeddedRavenTestBase
{
    private static VerifiedFiles Files(string sha) => new("set", "dir", new Dictionary<string, (string Path, string Sha256)> { ["base"] = ("p", sha) });

    [Fact]
    public async Task Recall_Refuses_A_Database_Loaded_Under_Another_Selection_Or_Files()
    {
        using var store = GetDocumentStore();
        await Assert.ThrowsAsync<InvalidOperationException>(() => HeldOutManifest.EnsureLoadedAsync(store, Files("aa"), new QuerySelection(1, 10)));

        await HeldOutManifest.StoreAsync(store, Files("aa"), new QuerySelection(1, 10));
        await HeldOutManifest.EnsureLoadedAsync(store, Files("aa"), new QuerySelection(1, 10));
        await Assert.ThrowsAsync<InvalidOperationException>(() => HeldOutManifest.EnsureLoadedAsync(store, Files("aa"), new QuerySelection(2, 10)));
        await Assert.ThrowsAsync<InvalidOperationException>(() => HeldOutManifest.EnsureLoadedAsync(store, Files("bb"), new QuerySelection(1, 10)));
    }

    [Fact]
    public async Task A_Null_Selection_Still_Compares_The_Files()
    {
        using var store = GetDocumentStore();
        await HeldOutManifest.StoreAsync(store, Files("aa"), selection: null);

        await HeldOutManifest.EnsureLoadedAsync(store, Files("aa"), selection: null);
        await Assert.ThrowsAsync<InvalidOperationException>(() => HeldOutManifest.EnsureLoadedAsync(store, Files("bb"), selection: null));
    }

    private sealed record Passage(string Text);

    private sealed record Checkpoint(long LinesImported);

    [Fact]
    public async Task The_Load_Record_And_A_Checkpoint_Never_Make_A_Partial_Load_Look_Complete()
    {
        using var store = GetDocumentStore();
        var files = Files("aa");
        var selection = new QuerySelection(1, 2);
        const int expected = 3;
        await HeldOutManifest.StoreAsync(store, files, selection);
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Checkpoint(expected - 1), "sphere/import-checkpoint");
            for (int i = 0; i < expected - 1; i++)
                await session.StoreAsync(new Passage("t"), $"Passages/{i}");
            await session.SaveChangesAsync();
        }
        var collection = store.Conventions.FindCollectionName(typeof(Passage));
        Assert.False(await HeldOutManifest.EnsureMatchesAsync(store, files, selection, collection, expected));

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Passage("t"), $"Passages/{expected - 1}");
            await session.SaveChangesAsync();
        }
        Assert.True(await HeldOutManifest.EnsureMatchesAsync(store, files, selection, collection, expected));
    }
}
