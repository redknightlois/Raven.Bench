using System;
using System.Net.Http;
using System.Threading.Tasks;
using RavenBench.Dataset;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace RavenBench.Tests.Dataset;

/// <summary>
/// Integration tests for SPHERE dataset provider against a live RavenDB instance.
/// Skipped when server is not reachable via the RavenDB client (topology must resolve).
/// </summary>
[Trait("Category", "Integration")]
public class SphereIntegrationTests : IAsyncLifetime
{
    private const string TestDatabase = "Sphere-IntegrationTest";
    private const string TestDataPath = "/tmp/sphere_test.jsonl.tar.gz";

    private string? _serverUrl;

    private static async Task<string?> DiscoverServerUrlAsync()
    {
        // Probe candidates using the actual RavenDB client to ensure topology resolves
        var candidates = new[] { "http://localhost:8080", "http://ravendb:8080", "http://127.0.0.1:8080" };

        foreach (var url in candidates)
        {
            try
            {
                using var store = new DocumentStore { Urls = new[] { url } };
                store.Initialize();

                // This call exercises full topology discovery
                await store.Maintenance.Server.SendAsync(
                    new GetDatabaseRecordOperation("__probe__"));
                return url;
            }
            catch
            {
                // Not reachable via client — topology hostname may not resolve
            }
        }
        return null;
    }

    public async Task InitializeAsync()
    {
        _serverUrl = await DiscoverServerUrlAsync();
        if (_serverUrl != null)
            await DeleteDatabaseIfExistsAsync();
    }

    public async Task DisposeAsync()
    {
        if (_serverUrl != null)
            await DeleteDatabaseIfExistsAsync();
    }

    [Fact]
    public async Task ImportAsync_StreamsFromTarGz_ImportsDocuments()
    {
        if (_serverUrl == null) return;

        var provider = new SphereDatasetProvider();
        await provider.ImportAsync(_serverUrl!, TestDatabase, TestDataPath, "100k");

        using var store = new DocumentStore { Urls = new[] { _serverUrl! }, Database = TestDatabase };
        store.Initialize();

        var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
        Assert.True(stats.CountOfDocuments >= 50, $"Expected >= 50 docs, got {stats.CountOfDocuments}");
    }

    [Fact]
    public async Task ImportAsync_ResumeAfterPartialImport_SkipsExisting()
    {
        if (_serverUrl == null) return;

        var provider = new SphereDatasetProvider();
        await provider.ImportAsync(_serverUrl!, TestDatabase, TestDataPath, "100k");

        using var store = new DocumentStore { Urls = new[] { _serverUrl! }, Database = TestDatabase };
        store.Initialize();
        var statsAfterFirst = await store.Maintenance.SendAsync(new GetStatisticsOperation());

        await provider.ImportAsync(_serverUrl!, TestDatabase, TestDataPath, "100k");
        var statsAfterSecond = await store.Maintenance.SendAsync(new GetStatisticsOperation());

        Assert.Equal(statsAfterFirst.CountOfDocuments, statsAfterSecond.CountOfDocuments);
    }

    [Fact]
    public async Task IsDatasetImportedAsync_AfterImport_ReturnsTrue()
    {
        if (_serverUrl == null) return;

        var provider = new SphereDatasetProvider();
        await provider.ImportAsync(_serverUrl!, TestDatabase, TestDataPath, "100k");

        var imported = await provider.IsDatasetImportedAsync(_serverUrl!, TestDatabase, expectedMinDocuments: 10);
        Assert.True(imported);
    }

    [Fact]
    public async Task IsDatasetImportedAsync_EmptyDatabase_ReturnsFalse()
    {
        if (_serverUrl == null) return;

        var provider = new SphereDatasetProvider();
        var imported = await provider.IsDatasetImportedAsync(_serverUrl!, "NonExistentDB-12345", expectedMinDocuments: 10);
        Assert.False(imported);
    }

    [Fact]
    public async Task GenerateQueryVectorsAsync_AfterImport_ReturnsMetadata()
    {
        if (_serverUrl == null) return;

        var provider = new SphereDatasetProvider();
        await provider.ImportAsync(_serverUrl!, TestDatabase, TestDataPath, "100k");

        var metadata = await provider.GenerateQueryVectorsAsync(
            _serverUrl!, TestDatabase, queryCount: 5, topK: 3);

        Assert.NotNull(metadata);
        Assert.True(metadata.QueryVectors.Length > 0);
        Assert.Equal(768, metadata.VectorDimensions);
        Assert.Equal("Vector", metadata.FieldName);
        Assert.True(metadata.BaseVectorCount > 0);

        foreach (var vec in metadata.QueryVectors)
            Assert.Equal(768, vec.Length);
    }

    private async Task DeleteDatabaseIfExistsAsync()
    {
        try
        {
            using var store = new DocumentStore { Urls = new[] { _serverUrl! } };
            store.Initialize();
            var dbRecord = await store.Maintenance.Server.SendAsync(
                new GetDatabaseRecordOperation(TestDatabase));
            if (dbRecord != null)
            {
                await store.Maintenance.Server.SendAsync(
                    new DeleteDatabasesOperation(TestDatabase, hardDelete: true));
            }
        }
        catch { }
    }
}
