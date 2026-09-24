using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Transport;

/// <summary>
/// The one typed vector search operation: its RQL, its refusal of an unsupported metric before load,
/// and each RavenDB transport mode serving it against the live server.
/// </summary>
public class VectorSearchOperationTests
{
    private const string Url = "http://localhost:8081";
    private const string IndexName = "Items/ByVector";

    [Fact]
    public void Rql_CarriesTheEffortAndTheFilterAsParameters()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = [1f],
            FieldName = "Vector",
            ExpectedIndex = IndexName,
            Effort = SearchEffort.RavenDb(32),
            Filter = new VectorFilter("Label", "x' or true")
        };

        Assert.Equal($"from index '{IndexName}' where vector.search('Vector', $vector, 0.0, $numberOfCandidates) and Label = $filterValue", op.ToRqlQuery());
    }

    [Fact]
    public void Rql_EffortNamingAnotherProductsKnob_IsRefused()
    {
        var op = new VectorSearchOperation { QueryVector = [1f], FieldName = "Vector", Effort = new SearchEffort("hnsw.ef_search", 40) };

        Assert.Throws<NotSupportedException>(op.ToRqlQuery);
    }

    [Fact]
    public void Filter_FieldThatIsNotAnIdentifier_IsRejected()
    {
        Assert.Throws<ArgumentException>(() => new VectorFilter("Label = 1 or 1", "x"));
    }

    [Fact]
    public async Task UnsupportedMetric_IsRefusedBeforeAnyFileOrServerIsTouched()
    {
        var set = new AnnBenchmarksHdf5Dataset("fake-euclidean", 2, VectorMetric.L2, VectorSets.Pinned("absent.hdf5", "http://127.0.0.1:9/absent.hdf5", new string('0', 64), 0));
        var opts = new RunOptions { Url = "http://127.0.0.1:9", Database = "unused", DatasetCacheDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"absent-{Guid.NewGuid():N}") };

        var ex = await Assert.ThrowsAsync<UnsupportedVectorMetricException>(() => DatasetImportCoordinator.PrepareVectorSetAsync(set, opts));

        Assert.Equal("RavenDB", ex.Target);
        Assert.Equal(VectorMetric.L2, ex.Metric);
        Assert.False(System.IO.Directory.Exists(opts.DatasetCacheDir));
    }

    [RequiresRavenDbFact(8081)]
    public async Task EveryRavenTransportMode_ServesTheOperation_ReturningAtMostKOrderedIds()
    {
        var database = $"vector-op-{Guid.NewGuid():N}";
        using var store = HttpHelper.Create(Url, database, HttpVersion.Version11);
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)));
        try
        {
            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 20; i++)
                    await session.StoreAsync(new Item { Label = i % 2 == 0 ? "even" : "odd", Embedding = [MathF.Cos(i * 0.1f), MathF.Sin(i * 0.1f), 0.5f] }, $"Items/{i}");
                await session.SaveChangesAsync();
            }

            await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
            {
                Name = IndexName,
                Maps = { "from i in docs.Items select new { i.Label, Vector = CreateVector(i.Embedding) }" },
                Fields = { ["Vector"] = new IndexFieldOptions { Vector = new VectorOptions { Dimensions = 3, SourceEmbeddingType = VectorEmbeddingType.Single, DestinationEmbeddingType = VectorEmbeddingType.Single } } }
            }));
            using (var session = store.OpenAsyncSession())
                await session.Query<Item>(IndexName).Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(1))).Take(0).ToListAsync<Item>();

            var transports = new ITransport[]
            {
                new RawHttpTransport(Url, database, CompressionMode.Identity, HttpVersion.Version11),
                new RavenClientTransport(Url, database, CompressionMode.Identity, HttpVersion.Version11),
                new RavenClientTransport(Url, database, CompressionMode.Identity, HttpVersion.Version11, mapEntities: true)
            };
            foreach (var transport in transports)
            {
                using (transport)
                {
                    var plain = await SearchAsync(transport, filter: null);
                    Assert.Equal("Items/0", plain[0], ignoreCase: true);
                    Assert.InRange(plain.Count, 1, 5);

                    var filtered = await SearchAsync(transport, new VectorFilter("Label", "odd"));
                    Assert.InRange(filtered.Count, 1, 5);
                    Assert.All(filtered, id => Assert.True(int.Parse(id.Split('/')[1]) % 2 == 1, id));
                }
            }
        }
        finally
        {
            await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true));
        }
    }

    private static async Task<IReadOnlyList<string>> SearchAsync(ITransport transport, VectorFilter? filter)
    {
        var result = await transport.ExecuteAsync(new VectorSearchOperation
        {
            QueryVector = [1f, 0f, 0.5f],
            FieldName = "Vector",
            TopK = 5,
            ExpectedIndex = IndexName,
            Effort = SearchEffort.RavenDb(16),
            Filter = filter
        }, CancellationToken.None);

        Assert.True(result.IsSuccess, result.ErrorDetails);
        return result.NeighborIds!;
    }

    private sealed class Item
    {
        public string Label { get; set; } = "";
        public float[] Embedding { get; set; } = [];
    }
}
