using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.VectorBench;

[Collection(LiveServers.Name)]
public class RavenDbEmbeddingTypeTests
{
    [Fact]
    public void An_Unknown_Embedding_Type_Is_Refused_By_Name()
    {
        FluentActions.Invoking(() => new RavenDbVectorTarget("http://localhost:1", "x", VectorMetric.Cosine, "Int4"))
            .Should().Throw<ArgumentException>().WithMessage("*Int4*Single, Int8, Binary*");
    }

    [Theory]
    [InlineData("Single", "float32, unquantized")]
    [InlineData("Int8", "int8, quantized")]
    [InlineData("Binary", "binary 1-bit, quantized")]
    public void Each_Embedding_Type_Names_Its_Storage_And_Its_Own_Index(string type, string storage)
    {
        using var target = new RavenDbVectorTarget("http://localhost:1", "x", VectorMetric.Cosine, type);
        target.VectorStorage.Should().Be(storage);
        target.ExpectedIndex.Should().Be(type == "Single" ? "VectorBench/Float32" : "VectorBench/" + type, "the Single index keeps its name");
    }

    [RequiresRavenDbFact(8081)]
    public Task Int8_Reaches_The_Index_Definition_And_Answers() => LoadsAndAnswers("Int8");

    [RequiresRavenDbFact(8081)]
    public Task Binary_Reaches_The_Index_Definition_And_Answers() => LoadsAndAnswers("Binary");

    private static async Task LoadsAndAnswers(string type)
    {
        var random = new Random(3);
        var set = Enumerable.Range(0, 200).Select(i => new LabelledVector(i.ToString(), Enumerable.Range(0, 32).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray(), "out")).ToList();
        using var target = new RavenDbVectorTarget("http://localhost:8081", "vector_quant_" + Guid.NewGuid().ToString("N")[..8], VectorMetric.Cosine, type);
        try
        {
            await target.LoadAsync(set.ToAsyncEnumerable(), CancellationToken.None);
            (await target.ReportedSettingsAsync(CancellationToken.None))["vector.DestinationEmbeddingType"].Should().Be(type);
            var result = await target.Transport.ExecuteAsync(new VectorSearchOperation
            {
                QueryVector = set[7].Vector, FieldName = target.FieldName, TopK = 5, ExpectedIndex = target.ExpectedIndex, Effort = target.Effort(64)
            }, CancellationToken.None);
            result.IsSuccess.Should().BeTrue(result.ErrorDetails);
            result.NeighborIds.Should().HaveCount(5).And.Contain(target.IdPrefix + "7");
        }
        finally
        {
            await target.CleanupAsync();
        }
    }
}
