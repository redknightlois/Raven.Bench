using System;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Trait("Category", "Unit")]
public class ClinicalWordsCaseTests
{
    [Fact]
    public async Task Words_Differing_Only_By_Case_Load_As_Two_Vectors_With_Their_Own_Values()
    {
        var provider = new ClinicalWordsDatasetProvider(100);
        var directory = Path.Combine(Path.GetTempPath(), $"clinical-case-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var source = Path.Combine(directory, provider.Files[0].FileName);
        await WriteParquetAsync(source, ["Cancer", "cancer"], [1.0, 2.0]);
        var files = await PinnedFiles.EnsureAsync(provider, directory, sourcePath: source, sha256: await PinnedFiles.Sha256Async(source));

        var upper = await provider.GetWordVectorAsync(files, "Cancer");
        var lower = await provider.GetWordVectorAsync(files, "cancer");

        upper.Should().NotBeNull().And.AllBeEquivalentTo(1f);
        lower.Should().NotBeNull().And.AllBeEquivalentTo(2f);
    }

    private static async Task WriteParquetAsync(string path, string[] words, double[] fill)
    {
        const int dimensions = 100;
        var word = new DataField<string>("word");
        var element = new DataField<double>("element");
        var embedding = new ListField("embedding", element);
        await using var stream = File.Create(path);
        await using var writer = await ParquetWriter.CreateAsync(new ParquetSchema(word, embedding), stream);
        using var group = writer.CreateRowGroup();
        await group.WriteColumnAsync(new DataColumn(word, words));
        var values = new double[words.Length * dimensions];
        var levels = new int[values.Length];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = fill[i / dimensions];
            levels[i] = i % dimensions == 0 ? 0 : 1;
        }
        await group.WriteColumnAsync(new DataColumn(element, values, levels));
    }
}
