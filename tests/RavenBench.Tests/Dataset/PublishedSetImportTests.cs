using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using PureHDF;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Collection(LiveServers.Name)]
public class PublishedSetImportTests : IDisposable
{
    private const string Url = "http://localhost:8081";
    private readonly string _dataDir = Path.Combine(Path.GetTempPath(), $"published-set-{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(_dataDir))
            Directory.Delete(_dataDir, recursive: true);
    }

    [RequiresRavenDbFact(8081)]
    public async Task PublishedSet_LoadsOnce_AndRecallScoresTheProductAgainstThePublishedNeighbours()
    {
        const int rows = 200, dims = 8, queries = 5, k = 5;
        var rng = new Random(3);
        var train = new float[rows, dims];
        var test = new float[queries, dims];
        foreach (var m in new[] { train, test })
            for (int r = 0; r < m.GetLength(0); r++)
                for (int c = 0; c < dims; c++)
                    m[r, c] = (float)(rng.NextDouble() * 2 - 1);

        var baseVectors = Enumerable.Range(0, rows).Select(r => new BaseVector(r.ToString(), Enumerable.Range(0, dims).Select(c => train[r, c]).ToArray())).ToArray();
        var truth = await BruteForceTruth.ComputeAsync(Enumerable.Range(0, queries).Select(q => Enumerable.Range(0, dims).Select(c => test[q, c]).ToArray()).ToArray(),
            baseVectors.ToAsyncEnumerable(), VectorMetric.Cosine, k);
        var neighbors = new int[queries, k];
        for (int q = 0; q < queries; q++)
            for (int j = 0; j < k; j++)
                neighbors[q, j] = int.Parse(truth[q][j]);

        var name = $"tiny-angular-{Guid.NewGuid():N}";
        var dir = Path.Combine(_dataDir, name);
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "set.hdf5");
        var h5 = new H5File { ["train"] = train, ["test"] = test, ["neighbors"] = neighbors, ["distances"] = new float[queries, k] };
        h5.Attributes["distance"] = "angular";
        h5.Write(path);
        var set = new AnnBenchmarksHdf5Dataset(name, dims, VectorMetric.Cosine, VectorSets.Pinned("set.hdf5", "", await PinnedFiles.Sha256Async(path), 0));

        var files = await PinnedFiles.EnsureAsync(set, _dataDir);
        var selection = new QuerySelection(1, queries);
        try
        {
            Assert.True(await PublishedSetImport.ImportAsync(Url, set, files, selection, VectorQuantization.None, IndexingEngine.Corax, null, null));
            Assert.False(await PublishedSetImport.ImportAsync(Url, set, files, new QuerySelection(2, queries), VectorQuantization.None, IndexingEngine.Corax, null, null));

            using (var store = HttpHelper.Create(Url, name, HttpVersion.Version11))
                Assert.Equal(rows, (await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation())).Collections[PublishedSetImport.CollectionName]);

            var metadata = await PublishedSetImport.MetadataAsync(set, files, selection, k, VectorQuantization.None, IndexingEngine.Corax, null, null);
            Assert.Equal(rows, metadata.BaseVectorCount);
            using var transport = new RawHttpTransport(Url, name, CompressionMode.Identity, HttpVersion.Version11);
            var recall = await new RecallMeasurement().MeasureAsync(transport, metadata, [k], VectorQuantization.None, SearchEffort.RavenDb(rows));

            Assert.Equal(1.0, recall.RecallAtK[k]);
        }
        finally
        {
            using var store = HttpHelper.Create(Url, name, HttpVersion.Version11);
            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(name, hardDelete: true));
        }
    }
}
