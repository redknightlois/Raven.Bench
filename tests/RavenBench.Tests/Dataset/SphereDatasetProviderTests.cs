using System;
using System.Collections.Generic;
using System.Formats.Tar;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using RavenBench.Dataset;
using Xunit;

namespace RavenBench.Tests.Dataset;

public class SphereDatasetProviderTests
{
    [Theory]
    [InlineData("100k", "Sphere-100K")]
    [InlineData("1m", "Sphere-1M")]
    [InlineData("10m", "Sphere-10M")]
    [InlineData("100m", "Sphere-100M")]
    [InlineData("full", "Sphere-Full")]
    public void GetDatabaseName_ReturnsCorrectNamePerProfile(string profile, string expectedDb)
    {
        var provider = new SphereDatasetProvider(profile);
        Assert.Equal(expectedDb, provider.GetDatabaseName(profile));
    }

    [Fact]
    public void Constructor_InvalidProfile_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SphereDatasetProvider("nonexistent"));
    }

    [Fact]
    public void GetDatasetInfo_ReturnsCorrectInfo()
    {
        var provider = new SphereDatasetProvider("1m");
        var info = provider.GetDatasetInfo("1m");

        Assert.Contains("SPHERE", info.Name);
        Assert.Contains("1,000,000", info.Description);
        Assert.Contains("768", info.Description);
    }

    [Fact]
    public void GetProfile_ReturnsCorrectTargetDocCount()
    {
        var profile = SphereDatasetProvider.GetProfile("100k");
        Assert.Equal(100_000, profile.TargetDocCount);

        var full = SphereDatasetProvider.GetProfile("full");
        Assert.Equal(899_000_000, full.TargetDocCount);
    }

    [Fact]
    public async Task StreamJsonLinesAsync_ParsesTarGzCorrectly()
    {
        // Create a tiny .jsonl.tar.gz in memory with 3 entries
        var tempFile = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}.jsonl.tar.gz");
        try
        {
            await CreateTestTarGzAsync(tempFile, 3);

            var lines = new List<SphereDatasetProvider.SphereJsonLine>();
            await foreach (var line in SphereDatasetProvider.StreamJsonLinesAsync(tempFile))
            {
                lines.Add(line);
            }

            Assert.Equal(3, lines.Count);
            Assert.All(lines, line =>
            {
                Assert.False(string.IsNullOrEmpty(line.Sha));
                Assert.False(string.IsNullOrEmpty(line.Raw));
                Assert.Equal(768, line.Vector.Length);
                Assert.True(line.Vector.Any(v => v != 0));
            });
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task StreamJsonLinesAsync_ParsesJsonlGzCorrectly()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}.jsonl.gz");
        try
        {
            await CreateTestJsonlGzAsync(tempFile, 5);

            var lines = new List<SphereDatasetProvider.SphereJsonLine>();
            await foreach (var line in SphereDatasetProvider.StreamJsonLinesAsync(tempFile))
            {
                lines.Add(line);
            }

            Assert.Equal(5, lines.Count);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal($"sha-{i}", lines[i].Sha);
                Assert.Equal($"Text content {i}", lines[i].Raw);
            }
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task StreamJsonLinesAsync_SkipsEmptyLines()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}.jsonl.gz");
        try
        {
            await WriteJsonlGzAsync(tempFile, new[] { MakeJsonLine(0), "", "   ", MakeJsonLine(1) });

            var lines = new List<SphereDatasetProvider.SphereJsonLine>();
            await foreach (var line in SphereDatasetProvider.StreamJsonLinesAsync(tempFile))
                lines.Add(line);

            Assert.Equal(2, lines.Count);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task StreamJsonLinesAsync_SkipsLinesWithEmptySha()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}.jsonl.gz");
        try
        {
            var noSha = JsonSerializer.Serialize(new { raw = "text", sha = "", title = "t", url = "u", vector = MakeVector(768) });
            await WriteJsonlGzAsync(tempFile, new[] { MakeJsonLine(0), noSha, MakeJsonLine(1) });

            var lines = new List<SphereDatasetProvider.SphereJsonLine>();
            await foreach (var line in SphereDatasetProvider.StreamJsonLinesAsync(tempFile))
                lines.Add(line);

            Assert.Equal(2, lines.Count);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void ResolveSourceFiles_SingleFile_ReturnsSingleFile()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}.jsonl.tar.gz");
        try
        {
            File.WriteAllText(tempFile, "dummy");
            var files = SphereDatasetProvider.ResolveSourceFiles(tempFile);
            Assert.Single(files);
            Assert.Equal(tempFile, files[0]);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void ResolveSourceFiles_Directory_FindsFiles()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);
        try
        {
            File.WriteAllText(Path.Combine(tempDir, "a.jsonl.tar.gz"), "dummy");
            File.WriteAllText(Path.Combine(tempDir, "b.jsonl.gz"), "dummy");
            File.WriteAllText(Path.Combine(tempDir, "c.txt"), "not this");

            var files = SphereDatasetProvider.ResolveSourceFiles(tempDir);
            Assert.Equal(2, files.Count);
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public void DatasetName_IsSphere()
    {
        var provider = new SphereDatasetProvider("100k");
        Assert.Equal("sphere", provider.DatasetName);
    }

    [Fact]
    public void VectorDimensions_Is768()
    {
        Assert.Equal(768, SphereDatasetProvider.VectorDimensions);
    }

    [Fact]
    public async Task StreamJsonLinesAsync_FiltersLinesWithEmptyVector()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"sphere-test-{Guid.NewGuid()}.jsonl.gz");
        try
        {
            var goodLine = MakeJsonLine(0); // has 768-dim vector
            var missingVector = JsonSerializer.Serialize(new { raw = "no-vector", sha = "sha-miss", title = "t", url = "u" });
            var emptyVector = JsonSerializer.Serialize(new { raw = "empty-vec", sha = "sha-empty", title = "t", url = "u", vector = Array.Empty<float>() });
            await WriteJsonlGzAsync(tempFile, new[] { goodLine, missingVector, emptyVector });

            var lines = new List<SphereDatasetProvider.SphereJsonLine>();
            await foreach (var line in SphereDatasetProvider.StreamJsonLinesAsync(tempFile))
                lines.Add(line);

            // Lines with missing or empty vectors should be filtered out —
            // storing empty embeddings would corrupt the vector index
            Assert.Single(lines);
            Assert.Equal("sha-0", lines[0].Sha);
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    // --- Helpers ---

    private static string MakeJsonLine(int i)
    {
        var obj = new
        {
            raw = $"Text content {i}",
            sha = $"sha-{i}",
            title = $"Title {i}",
            url = $"https://example.com/{i}",
            vector = MakeVector(768)
        };
        return JsonSerializer.Serialize(obj);
    }

    private static float[] MakeVector(int dims)
    {
        var rng = new Random(42);
        var vec = new float[dims];
        for (int i = 0; i < dims; i++)
            vec[i] = (float)(rng.NextDouble() * 2 - 1);
        return vec;
    }

    private static async Task CreateTestTarGzAsync(string filePath, int lineCount)
    {
        // Build the JSONL content
        var sb = new StringBuilder();
        for (int i = 0; i < lineCount; i++)
            sb.AppendLine(MakeJsonLine(i));
        var jsonlBytes = Encoding.UTF8.GetBytes(sb.ToString());

        // Create tar archive in memory
        using var tarMs = new MemoryStream();
        await using (var tarWriter = new TarWriter(tarMs, leaveOpen: true))
        {
            var entry = new PaxTarEntry(TarEntryType.RegularFile, "data.jsonl")
            {
                DataStream = new MemoryStream(jsonlBytes)
            };
            await tarWriter.WriteEntryAsync(entry);
        }
        tarMs.Position = 0;

        // Compress with gzip
        await using var fs = File.Create(filePath);
        await using var gz = new GZipStream(fs, CompressionLevel.Fastest);
        await tarMs.CopyToAsync(gz);
    }

    private static async Task CreateTestJsonlGzAsync(string filePath, int lineCount)
    {
        var jsonLines = new string[lineCount];
        for (int i = 0; i < lineCount; i++)
            jsonLines[i] = MakeJsonLine(i);
        await WriteJsonlGzAsync(filePath, jsonLines);
    }

    private static async Task WriteJsonlGzAsync(string filePath, string[] lines)
    {
        // Write to a MemoryStream first, then flush to disk to avoid disposal order issues
        using var ms = new MemoryStream();
        using (var gz = new GZipStream(ms, CompressionLevel.Fastest, leaveOpen: true))
        using (var sw = new StreamWriter(gz, Encoding.UTF8, leaveOpen: false))
        {
            foreach (var line in lines)
                await sw.WriteLineAsync(line);
        }
        ms.Position = 0;
        await using var fs = File.Create(filePath);
        await ms.CopyToAsync(fs);
    }
}
