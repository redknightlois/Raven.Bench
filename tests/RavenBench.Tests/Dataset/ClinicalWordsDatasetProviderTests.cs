using RavenBench.Dataset;
using RavenBench.Core;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;
using System;
using System.IO;
using RavenBench.Tests.Infrastructure;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace RavenBench.Tests.Dataset;

public class ClinicalWordsDatasetProviderTests
{
    [RequiresClinicalWordsTheory]
    [InlineData(100)]
    [InlineData(300)]
    [InlineData(600)]
    public async Task LoadWordVectors_AllDimensions_LoadsSuccessfully(int dimensions)
    {
        // Arrange
        var (provider, files) = await PinLocalParquetAsync(dimensions);

        // Act
        var vectors = await provider.GenerateQueryVectorsAsync(files, new QuerySelection(42, 5), k: 10);

        // Assert
        Assert.Equal(dimensions, vectors.VectorDimensions);
        Assert.Equal(5, vectors.QueryVectors.Length);
        Assert.True(vectors.BaseVectorCount > 300000, $"Expected >300k words, got {vectors.BaseVectorCount}");
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(32, 64)]
    public void TheImportedIndex_IsTheIndexTheMetadataNames_WithTheSameHnswParameters(int? edges, int? candidates)
    {
        var index = new ClinicalWordsDatasetProvider(300).VectorIndex(VectorQuantization.Int8, IndexingEngine.Corax, edges, candidates);
        Assert.Equal(ClinicalWordsDatasetProvider.IndexName(VectorQuantization.Int8, IndexingEngine.Corax, edges, candidates), index.Name);
        Assert.Equal(edges, index.Fields["Vector"].Vector.NumberOfEdges);
        Assert.Equal(candidates, index.Fields["Vector"].Vector.NumberOfCandidatesForIndexing);
        Assert.Equal(300, index.Fields["Vector"].Vector.Dimensions);
    }

    [RequiresClinicalWordsFact]
    public async Task ComputeDocumentEmbedding_WithValidText_ReturnsVector()
    {
        // Arrange
        var (provider, files) = await PinLocalParquetAsync(100);
        var text = "The patient presented with chest pain and shortness of breath.";

        // Act
        var embedding = await provider.ComputeDocumentEmbeddingAsync(files, text);

        // Assert
        Assert.Equal(100, embedding.Length);
        Assert.True(embedding.Any(v => v != 0), "Embedding should not be all zeros");
    }

    [RequiresClinicalWordsFact]
    public async Task GetWordVector_ExistingWord_ReturnsVector()
    {
        // Arrange
        var (provider, files) = await PinLocalParquetAsync(100);

        // Act
        var vector = await provider.GetWordVectorAsync(files, "patient");

        // Assert
        Assert.NotNull(vector);
        Assert.Equal(100, vector!.Length);
    }

    [RequiresClinicalWordsFact]
    public async Task GetWordVector_NonExistingWord_ReturnsNull()
    {
        // Arrange
        var (provider, files) = await PinLocalParquetAsync(100);

        // Act
        var vector = await provider.GetWordVectorAsync(files, "xyznonexistent123");

        // Assert
        Assert.Null(vector);
    }

    [RequiresClinicalWordsFact]
    public async Task GetWordVector_ReturnsCorrectDimensionVectors()
    {
        // Arrange
        var (provider, files) = await PinLocalParquetAsync(100);

        // Act - get a known clinical word
        var vector = await provider.GetWordVectorAsync(files, "patient");

        // Assert - verify it's a proper 100D vector with realistic values
        Assert.NotNull(vector);
        Assert.Equal(100, vector!.Length);

        // Verify values are in reasonable range (Word2Vec typically -1 to 1)
        foreach (var v in vector)
        {
            Assert.True(v >= -10f && v <= 10f, $"Vector value {v} is outside expected range");
        }

        // Verify not all zeros and not all same value
        var distinctValues = vector.Distinct().Count();
        Assert.True(distinctValues > 10, $"Expected diverse vector values, got only {distinctValues} distinct values");
    }

    [RequiresClinicalWordsFact]
    public async Task GenerateQueryVectors_ReturnsValidVectors()
    {
        // Arrange
        var (provider, files) = await PinLocalParquetAsync(100);

        // Act
        var metadata = await provider.GenerateQueryVectorsAsync(files, new QuerySelection(42, 10), k: 10);

        // Assert
        Assert.Equal(10, metadata.QueryVectors.Length);
        Assert.Equal(100, metadata.VectorDimensions);

        foreach (var vec in metadata.QueryVectors)
        {
            Assert.Equal(100, vec.Length);
            Assert.True(vec.Any(v => v != 0), "Query vector should not be all zeros");
        }
    }

    // The parquet derives locally, so the test pins the local file as found, as an operator does with --dataset-sha256.
    private static async Task<(ClinicalWordsDatasetProvider, VerifiedFiles)> PinLocalParquetAsync(int dimensions)
    {
        var provider = new ClinicalWordsDatasetProvider(dimensions);
        var source = ClinicalWordsAvailability.PathOf(dimensions)
            ?? throw new FileNotFoundException($"No local parquet for {provider.Name}.");
        var dataDir = Path.Combine(Path.GetTempPath(), $"clinical-pin-{Guid.NewGuid():N}");
        return (provider, await PinnedFiles.EnsureAsync(provider, dataDir, sourcePath: source, sha256: await PinnedFiles.Sha256Async(source)));
    }
}
