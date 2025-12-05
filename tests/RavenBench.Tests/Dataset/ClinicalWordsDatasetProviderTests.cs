using RavenBench.Dataset;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace RavenBench.Tests.Dataset;

public class ClinicalWordsDatasetProviderTests
{
    [Theory]
    [InlineData(100)]
    [InlineData(300)]
    [InlineData(600)]
    public async Task LoadWordVectors_AllDimensions_LoadsSuccessfully(int dimensions)
    {
        // Arrange
        var provider = new ClinicalWordsDatasetProvider(dimensions);

        // Act
        var vectors = await provider.GenerateQueryVectorsAsync(count: 5);

        // Assert
        Assert.Equal(dimensions, vectors.VectorDimensions);
        Assert.Equal(5, vectors.QueryVectors.Length);
        Assert.True(vectors.BaseVectorCount > 300000, $"Expected >300k words, got {vectors.BaseVectorCount}");
    }

    [Fact]
    public async Task ComputeDocumentEmbedding_WithValidText_ReturnsVector()
    {
        // Arrange
        var provider = new ClinicalWordsDatasetProvider(100);
        var text = "The patient presented with chest pain and shortness of breath.";

        // Act
        var embedding = await provider.ComputeDocumentEmbeddingAsync(text);

        // Assert
        Assert.Equal(100, embedding.Length);
        Assert.True(embedding.Any(v => v != 0), "Embedding should not be all zeros");
    }

    [Fact]
    public async Task GetWordVector_ExistingWord_ReturnsVector()
    {
        // Arrange
        var provider = new ClinicalWordsDatasetProvider(100);

        // Act
        var vector = await provider.GetWordVectorAsync("patient");

        // Assert
        Assert.NotNull(vector);
        Assert.Equal(100, vector!.Length);
    }

    [Fact]
    public async Task GetWordVector_NonExistingWord_ReturnsNull()
    {
        // Arrange
        var provider = new ClinicalWordsDatasetProvider(100);

        // Act
        var vector = await provider.GetWordVectorAsync("xyznonexistent123");

        // Assert
        Assert.Null(vector);
    }

    [Fact]
    public async Task GetWordVector_ReturnsCorrectDimensionVectors()
    {
        // Arrange
        var provider = new ClinicalWordsDatasetProvider(100);

        // Act - get a known clinical word
        var vector = await provider.GetWordVectorAsync("patient");

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

    [Fact]
    public async Task GenerateQueryVectors_ReturnsValidVectors()
    {
        // Arrange
        var provider = new ClinicalWordsDatasetProvider(100);

        // Act
        var metadata = await provider.GenerateQueryVectorsAsync(count: 10);

        // Assert
        Assert.Equal(10, metadata.QueryVectors.Length);
        Assert.Equal(100, metadata.VectorDimensions);

        foreach (var vec in metadata.QueryVectors)
        {
            Assert.Equal(100, vec.Length);
            Assert.True(vec.Any(v => v != 0), "Query vector should not be all zeros");
        }
    }
}
