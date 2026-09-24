using RavenBench.Core.Workload;

namespace RavenBench.Dataset.Vectors;

/// <summary>
/// The pinned vector sets, and the bridge from a set to the workload metadata a run consumes.
/// </summary>
public static class VectorSets
{
    public static readonly IReadOnlyList<IVectorDataset> Published =
    [
        new AnnBenchmarksHdf5Dataset("glove-100-angular", 100, VectorMetric.Cosine, Pinned(
            "glove-100-angular.hdf5", "http://ann-benchmarks.com/glove-100-angular.hdf5",
            "544af1d5e84e112cd4749571dcfd8ca109818a572f850af75a3a09e093a953c4", 485_413_888)),
        new AnnBenchmarksHdf5Dataset("dbpedia-openai-1000k-angular", 1536, VectorMetric.Cosine, Pinned(
            "dbpedia-openai-1000k-angular.hdf5", "http://ann-benchmarks.com/dbpedia-openai-1000k-angular.hdf5",
            "62c1c0b235e26952857139537b65f8272026cd1c385c1bf6dba20481ee8a6619", 6_400_000_000)),
        Cohere("cohere-768-100k", "cohere_small_100k",
            train: ("a8719014d8e86bbfb64f53a21287aecf10d048d7de28c7bebb1d0559b9181b42", 312_652_957),
            test: ("252a25003060713a268cd2bf5c5f8fab6159f13772924859487907bef64f1391", 3_126_879),
            neighbors: ("ee07cdb43a7919bc1ad0525b3bf646036b43be5b7a0aee3ddcef06c21a42594a", 3_163_592)),
        Cohere("cohere-768-1m", "cohere_medium_1m",
            train: ("de6b84eb08fc5203a56eef619970a14f5cae08d0a0ae0bcd23b8058d7045f381", 3_131_995_162),
            test: ("a2ec76c907da22e5533ce31c1105e4fd4d3c4597915a7771d5e41897a95ecbed", 3_133_165),
            neighbors: ("30e7467766be3e2a6b167df16388a4a35e38fa6cc9e593fb659cce81ffde0940", 3_704_127)),
    ];

    /// <summary>
    /// The published set of that name, or null when no published set carries it.
    /// </summary>
    public static IVectorDataset? FindPublished(string name) => Published.FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// The selection's query vectors and product-neutral truth as workload metadata.
    /// </summary>
    public static async Task<VectorWorkloadMetadata> BuildMetadataAsync(IVectorDataset set, VerifiedFiles files, QuerySelection selection, int k, string fieldName, string documentIdPrefix, CancellationToken ct = default)
    {
        var queries = await set.GetQueriesAsync(files, selection, k, ct).ConfigureAwait(false);
        return new VectorWorkloadMetadata
        {
            FieldName = fieldName,
            QueryVectors = queries.Queries,
            VectorDimensions = set.Dimensions,
            BaseVectorCount = await set.BaseCountAsync(files, selection, ct).ConfigureAwait(false),
            Metric = set.Metric,
            DocumentIdPrefix = documentIdPrefix,
            GroundTruth = queries.Neighbors.Select((n, i) => (n, i)).ToDictionary(x => x.i, x => x.n)
        };
    }

    private static VectorDbBenchParquetDataset Cohere(string name, string folder, (string Sha, long Size) train, (string Sha, long Size) test, (string Sha, long Size) neighbors)
    {
        var baseUrl = $"https://assets.zilliz.com/benchmark/{folder}";
        return new VectorDbBenchParquetDataset(name, 768, VectorMetric.Cosine,
            Pinned("train.parquet", $"{baseUrl}/train.parquet", train.Sha, train.Size),
            Pinned("test.parquet", $"{baseUrl}/test.parquet", test.Sha, test.Size),
            Pinned("neighbors.parquet", $"{baseUrl}/neighbors.parquet", neighbors.Sha, neighbors.Size));
    }

    internal static DatasetFile Pinned(string fileName, string url, string sha256, long size) => new()
    {
        FileName = fileName,
        Url = url,
        Sha256 = sha256,
        Type = "vectors",
        EstimatedSizeBytes = size
    };
}
