using System;
using System.Linq;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class VectorSearchWorkloadTests
{
    [Fact]
    public void VectorSearchOperation_ShouldHaveCorrectProperties()
    {
        // Arrange
        var queryVector = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
        
        // Act
        var operation = new VectorSearchOperation
        {
            QueryVector = queryVector,
            FieldName = "Embedding",
            TopK = 10,
            MinimumSimilarity = 0.5f,
            UseExactSearch = false,
            Quantization = VectorQuantization.Int8
        };

        // Assert
        Assert.NotNull(operation.QueryVector);
        Assert.Equal(4, operation.QueryVector.Length);
        Assert.Equal("Embedding", operation.FieldName);
        Assert.Equal(10, operation.TopK);
        Assert.Equal(0.5f, operation.MinimumSimilarity);
        Assert.False(operation.UseExactSearch);
        Assert.Equal(VectorQuantization.Int8, operation.Quantization);
    }

    [Fact]
    public void VectorSearchWorkload_ShouldGenerateOperations()
    {
        // Arrange
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = new[] 
            { 
                new float[] { 0.1f, 0.2f, 0.3f },
                new float[] { 0.4f, 0.5f, 0.6f },
                new float[] { 0.7f, 0.8f, 0.9f }
            },
            VectorDimensions = 3,
            BaseVectorCount = 1000
        };

        var workload = new VectorSearchWorkload(
            metadata: metadata,
            topK: 5,
            minimumSimilarity: 0.3f,
            useExactSearch: true,
            quantization: VectorQuantization.None
        );

        var rng = new Random(42);

        // Act
        var operation = workload.NextOperation(rng) as VectorSearchOperation;

        // Assert
        Assert.NotNull(operation);
        Assert.Equal("Embedding", operation.FieldName);
        Assert.Equal(5, operation.TopK);
        Assert.Equal(0.3f, operation.MinimumSimilarity);
        Assert.True(operation.UseExactSearch);
        Assert.Equal(VectorQuantization.None, operation.Quantization);
        Assert.Equal(3, operation.QueryVector.Length);
    }

    [Fact]
    public void VectorSearchWorkload_ShouldRandomlySelectQueries()
    {
        // Arrange
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = new[] 
            { 
                new float[] { 1.0f, 0.0f },
                new float[] { 0.0f, 1.0f },
                new float[] { 0.5f, 0.5f }
            },
            VectorDimensions = 2,
            BaseVectorCount = 1000
        };

        var workload = new VectorSearchWorkload(metadata);
        var rng = new Random(42);
        var operations = new VectorSearchOperation[10];

        // Act - Generate multiple operations
        for (int i = 0; i < 10; i++)
        {
            operations[i] = (VectorSearchOperation)workload.NextOperation(rng);
        }

        // Assert - Should have selected different query vectors
        var uniqueVectors = operations
            .Select(op => string.Join(",", op.QueryVector))
            .Distinct()
            .Count();

        Assert.True(uniqueVectors > 1, "Should randomly select from multiple query vectors");
    }

    [Theory]
    [InlineData(VectorQuantization.None)]
    [InlineData(VectorQuantization.Int8)]
    [InlineData(VectorQuantization.Binary)]
    public void VectorSearchOperation_ShouldSupportAllQuantizationTypes(VectorQuantization quantization)
    {
        // Arrange & Act
        var operation = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f, 0.2f },
            FieldName = "Embedding",
            Quantization = quantization
        };

        // Assert
        Assert.Equal(quantization, operation.Quantization);
    }

    [Fact]
    public void VectorWorkloadMetadata_ShouldProvideCorrectCounts()
    {
        // Arrange & Act
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = new float[50][],
            VectorDimensions = 128,
            BaseVectorCount = 1_000_000
        };

        // Assert
        Assert.Equal(50, metadata.QueryVectorCount);
        Assert.Equal(128, metadata.VectorDimensions);
        Assert.Equal(1_000_000, metadata.BaseVectorCount);
    }

    // ===== Validation Tests =====

    [Fact]
    public void Throws_On_Null_Metadata()
    {
        var ex = Assert.Throws<ArgumentNullException>(() =>
            new VectorSearchWorkload(null!));
        Assert.Equal("metadata", ex.ParamName);
    }

    [Fact]
    public void Throws_On_Empty_QueryVectors()
    {
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = Array.Empty<float[]>(),
            VectorDimensions = 128
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            new VectorSearchWorkload(metadata));
        Assert.Contains("at least one query vector", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void Throws_On_Invalid_TopK(int topK)
    {
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = new[] { new float[] { 0.1f } },
            VectorDimensions = 1
        };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new VectorSearchWorkload(metadata, topK: topK));
        Assert.Equal("topK", ex.ParamName);
    }

    [Theory]
    [InlineData(-0.1f)]
    [InlineData(1.1f)]
    [InlineData(2.0f)]
    public void Throws_On_Invalid_MinimumSimilarity(float similarity)
    {
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = new[] { new float[] { 0.1f } },
            VectorDimensions = 1
        };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new VectorSearchWorkload(metadata, minimumSimilarity: similarity));
        Assert.Equal("minimumSimilarity", ex.ParamName);
    }

    [Fact]
    public void ToRqlQuery_GeneratesCorrectQuery_WithoutQuantization()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            TopK = 10,
            Quantization = VectorQuantization.None
        };

        var query = op.ToRqlQuery();

        Assert.Equal("from index 'Words/ByEmbedding' where vector.search('Embedding', $vector)", query);
    }

    [Fact]
    public void ToRqlQuery_GeneratesCorrectQuery_WithInt8Quantization()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            Quantization = VectorQuantization.Int8
        };

        var query = op.ToRqlQuery();

        Assert.Equal("from index 'Words/ByEmbeddingInt8' where vector.search(embedding.f32_i8('Embedding'), $vector)", query);
    }

    [Fact]
    public void ToRqlQuery_GeneratesCorrectQuery_WithBinaryQuantization()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            Quantization = VectorQuantization.Binary
        };

        var query = op.ToRqlQuery();

        Assert.Equal("from index 'Words/ByEmbeddingBinary' where vector.search(embedding.f32_i1('Embedding'), $vector)", query);
    }

    [Fact]
    public void ToRqlQuery_GeneratesCorrectQuery_WithExactSearch()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            UseExactSearch = true
        };

        var query = op.ToRqlQuery();

        Assert.Contains("exact(vector.search", query);
    }

    [Fact]
    public void ToRqlQuery_GeneratesCorrectQuery_WithMinimumSimilarity()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            MinimumSimilarity = 0.8f
        };

        var query = op.ToRqlQuery();

        Assert.Contains(">= $minSimilarity", query);
    }

    [Fact]
    public void GetEmbeddingSelector_ThrowsOnNullFieldName()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = null!
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            op.GetEmbeddingSelector());
        Assert.Contains("FieldName", ex.Message);
    }

    [Fact]
    public void GetEmbeddingSelector_ThrowsOnEmptyFieldName()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = ""
        };

        var ex = Assert.Throws<InvalidOperationException>(() =>
            op.GetEmbeddingSelector());
        Assert.Contains("FieldName", ex.Message);
    }

    [Fact]
    public void GetEmbeddingSelector_ReturnsCorrectSelector_ForNone()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            Quantization = VectorQuantization.None
        };

        var selector = op.GetEmbeddingSelector();

        Assert.Equal("'Embedding'", selector);
    }

    [Fact]
    public void GetEmbeddingSelector_ReturnsCorrectSelector_ForInt8()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            Quantization = VectorQuantization.Int8
        };

        var selector = op.GetEmbeddingSelector();

        Assert.Equal("embedding.f32_i8('Embedding')", selector);
    }

    [Fact]
    public void GetEmbeddingSelector_ReturnsCorrectSelector_ForBinary()
    {
        var op = new VectorSearchOperation
        {
            QueryVector = new float[] { 0.1f },
            FieldName = "Embedding",
            Quantization = VectorQuantization.Binary
        };

        var selector = op.GetEmbeddingSelector();

        Assert.Equal("embedding.f32_i1('Embedding')", selector);
    }
}
