using System;
using System.Collections.Generic;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Metrics;

public class RecallResultTests
{
    [Fact]
    public void RecallResult_Holds_Multiple_K_Values()
    {
        var result = new RecallResult
        {
            RecallAtK = new Dictionary<int, double>
            {
                { 1, 0.85 },
                { 5, 0.92 },
                { 10, 0.97 }
            },
            QueryCount = 100,
            GroundTruthDepth = 10,
            GroundTruthCached = false,
            GroundTruthComputeTime = TimeSpan.FromSeconds(5),
            MeasurementTime = TimeSpan.FromSeconds(2)
        };

        Assert.Equal(3, result.RecallAtK.Count);
        Assert.Equal(0.85, result.RecallAtK[1]);
        Assert.Equal(0.92, result.RecallAtK[5]);
        Assert.Equal(0.97, result.RecallAtK[10]);
        Assert.Equal(100, result.QueryCount);
        Assert.Equal(10, result.GroundTruthDepth);
        Assert.False(result.GroundTruthCached);
    }

    [Fact]
    public void VectorWorkloadMetadata_Supports_IndexName_And_CollectionName()
    {
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = [new float[] { 0.1f, 0.2f }],
            VectorDimensions = 2,
            BaseVectorCount = 1000,
            IndexName = "Passages/ByEmbedding-corax",
            CollectionName = "Passages"
        };

        Assert.Equal("Passages/ByEmbedding-corax", metadata.IndexName);
        Assert.Equal("Passages", metadata.CollectionName);
    }

    [Fact]
    public void VectorWorkloadMetadata_GroundTruth_Uses_String_Ids()
    {
        var metadata = new VectorWorkloadMetadata
        {
            FieldName = "Embedding",
            QueryVectors = [new float[] { 0.1f }],
            VectorDimensions = 1,
            GroundTruth = new Dictionary<int, string[]>
            {
                { 0, new[] { "Words/hello", "Words/world", "Words/test" } }
            }
        };

        Assert.NotNull(metadata.GroundTruth);
        Assert.Equal(3, metadata.GroundTruth[0].Length);
        Assert.Equal("Words/hello", metadata.GroundTruth[0][0]);
    }
}
