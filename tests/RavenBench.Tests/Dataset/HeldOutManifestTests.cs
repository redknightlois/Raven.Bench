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
}
