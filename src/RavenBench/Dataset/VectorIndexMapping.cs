using Raven.Client.Documents.Indexes.Vector;
using RavenBench.Core;
using RavenBench.Core.Workload;

namespace RavenBench.Dataset;

public static class VectorIndexMapping
{
    /// <summary>
    /// Maps a quantization mode to source/destination embedding types.
    /// Int2=4, Int3=5, Int4=6 in the turboquant VectorEmbeddingType enum — cast directly
    /// since the NuGet client library doesn't define these values yet.
    /// </summary>
    public static (VectorEmbeddingType SourceType, VectorEmbeddingType DestinationType) GetEmbeddingTypes(VectorQuantization quantization)
    {
        return quantization switch
        {
            VectorQuantization.Int8 => (VectorEmbeddingType.Single, VectorEmbeddingType.Int8),
            VectorQuantization.Binary => (VectorEmbeddingType.Single, VectorEmbeddingType.Binary),
            VectorQuantization.Int4 => (VectorEmbeddingType.Single, (VectorEmbeddingType)6),
            VectorQuantization.Int3 => (VectorEmbeddingType.Single, (VectorEmbeddingType)5),
            VectorQuantization.Int2 => (VectorEmbeddingType.Single, (VectorEmbeddingType)4),
            _ => (VectorEmbeddingType.Single, VectorEmbeddingType.Single)
        };
    }

    public static string GetEngineSuffix(IndexingEngine searchEngine)
    {
        return searchEngine == IndexingEngine.Lucene ? "-lucene" : "-corax";
    }
}
