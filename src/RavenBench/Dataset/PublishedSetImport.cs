using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Core;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;

namespace RavenBench.Dataset;

/// <summary>
/// Loads a set with a published query split into RavenDB: one document per base vector, in a database named
/// after the set, and a vector index over the documents.
/// </summary>
public static class PublishedSetImport
{
    public const string CollectionName = "VectorDocuments";
    public const string DocumentIdPrefix = CollectionName + "/";

    private sealed record VectorDocument(float[] Embedding);

    public static string DatabaseName(IVectorDataset set) => set.Name;

    public static string IndexName(VectorQuantization quantization, IndexingEngine engine, int? numberOfEdges, int? numberOfCandidatesForIndexing) =>
        VectorIndexNaming.GetIndexName(CollectionName, quantization, VectorIndexMapping.GetEngineSuffix(engine), numberOfEdges, numberOfCandidatesForIndexing);

    /// <summary>
    /// Loads the base vectors unless the database already holds this set's load, then builds the index.
    /// </summary>
    /// <returns>True when documents were loaded; false when the database already held them.</returns>
    public static async Task<bool> ImportAsync(string serverUrl, IVectorDataset set, VerifiedFiles files, QuerySelection selection,
        VectorQuantization quantization, IndexingEngine engine, int? numberOfEdges, int? numberOfCandidatesForIndexing, Version? httpVersion = null, CancellationToken ct = default)
    {
        using var store = HttpHelper.Create(serverUrl, DatabaseName(set), httpVersion);
        if (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database), ct) == null)
            await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)), ct);

        // The load record is written after the last document, so a record means a complete load.
        var loaded = await HeldOutManifest.EnsureMatchesAsync(store, files, selection: null, expectedDocuments: 0);
        if (loaded == false)
        {
            Console.WriteLine($"[Dataset] {set.Name}: loading base vectors into '{store.Database}'");
            long count = 0;
            await using (var bulkInsert = store.BulkInsert(token: ct))
            {
                await foreach (var vector in set.ReadBaseAsync(files, selection, ct))
                {
                    await bulkInsert.StoreAsync(new VectorDocument(vector.Vector), DocumentIdPrefix + vector.Id);
                    if (++count % 100_000 == 0)
                        Console.Write($"\r[Dataset] {set.Name}: loaded {count:N0}");
                }
            }
            await HeldOutManifest.StoreAsync(store, files, selection: null);
            Console.WriteLine($"\n[Dataset] {set.Name}: loaded {count:N0} base vectors");
        }

        await CreateVectorIndexAsync(store, set.Dimensions, quantization, engine, numberOfEdges, numberOfCandidatesForIndexing);
        return loaded == false;
    }

    public static async Task CreateVectorIndexAsync(IDocumentStore store, int dimensions, VectorQuantization quantization, IndexingEngine engine, int? numberOfEdges, int? numberOfCandidatesForIndexing)
    {
        var (sourceType, destType) = VectorIndexMapping.GetEmbeddingTypes(quantization);
        await VectorIndexHelper.CreateAndWaitForIndexAsync(store, new IndexDefinition
        {
            Name = IndexName(quantization, engine, numberOfEdges, numberOfCandidatesForIndexing),
            Maps = { $"from v in docs.{CollectionName} select new {{ Vector = CreateVector(v.Embedding) }}" },
            Fields =
            {
                ["Vector"] = new IndexFieldOptions
                {
                    Vector = new VectorOptions
                    {
                        Dimensions = dimensions,
                        SourceEmbeddingType = sourceType,
                        DestinationEmbeddingType = destType,
                        NumberOfEdges = numberOfEdges,
                        NumberOfCandidatesForIndexing = numberOfCandidatesForIndexing
                    }
                }
            },
            Configuration = { ["Indexing.Static.SearchEngineType"] = engine == IndexingEngine.Lucene ? "Lucene" : "Corax" }
        }, $"[Dataset] {store.Database}");
    }

    /// <summary>
    /// The selection's published queries and neighbours, aimed at the set's index.
    /// </summary>
    public static async Task<VectorWorkloadMetadata> MetadataAsync(IVectorDataset set, VerifiedFiles files, QuerySelection selection, int k,
        VectorQuantization quantization, IndexingEngine engine, int? numberOfEdges, int? numberOfCandidatesForIndexing)
    {
        var metadata = await VectorSets.BuildMetadataAsync(set, files, selection, k, fieldName: "Embedding", DocumentIdPrefix);
        metadata.IndexName = IndexName(quantization, engine, numberOfEdges, numberOfCandidatesForIndexing);
        metadata.CollectionName = CollectionName;
        metadata.IndexedFieldName = "Vector";
        metadata.EnsureIndexExists = (store, _) => CreateVectorIndexAsync((IDocumentStore)store, set.Dimensions, quantization, engine, numberOfEdges, numberOfCandidatesForIndexing);
        return metadata;
    }
}
