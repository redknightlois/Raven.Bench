using System;
using System.Threading.Tasks;
using RavenBench.Dataset;
using Xunit;

namespace RavenBench.Tests.Dataset;

public class SphereDatasetProviderTests
{
    [Fact]
    public void DatasetName_ReturnsSphere()
    {
        var provider = new SphereDatasetProvider();
        Assert.Equal("sphere", provider.DatasetName);
    }

    [Theory]
    [InlineData("100k", "Sphere-100K")]
    [InlineData("1m", "Sphere-1M")]
    [InlineData("10m", "Sphere-10M")]
    [InlineData("100m", "Sphere-100M")]
    [InlineData("full", "Sphere-Full")]
    public void GetDatabaseName_ReturnsProfileBasedName(string profile, string expected)
    {
        var provider = new SphereDatasetProvider();
        Assert.Equal(expected, provider.GetDatabaseName(profile));
    }

    [Fact]
    public void GetDatabaseName_NullProfile_DefaultsTo100K()
    {
        var provider = new SphereDatasetProvider();
        Assert.Equal("Sphere-100K", provider.GetDatabaseName());
    }

    [Fact]
    public void GetDatabaseName_CaseInsensitive()
    {
        var provider = new SphereDatasetProvider();
        Assert.Equal("Sphere-1M", provider.GetDatabaseName("1M"));
        Assert.Equal("Sphere-1M", provider.GetDatabaseName("1m"));
        Assert.Equal("Sphere-Full", provider.GetDatabaseName("FULL"));
    }

    [Fact]
    public void GetDatabaseName_InvalidProfile_Throws()
    {
        var provider = new SphereDatasetProvider();
        var ex = Assert.Throws<ArgumentException>(() => provider.GetDatabaseName("invalid"));
        Assert.Contains("Invalid SPHERE profile", ex.Message);
        Assert.Contains("100k", ex.Message);
    }

    [Theory]
    [InlineData("100k", 100_000)]
    [InlineData("1m", 1_000_000)]
    [InlineData("full", 899_000_000)]
    public void GetDatasetInfo_ReturnsCorrectLineCount(string profile, long expectedLines)
    {
        var provider = new SphereDatasetProvider();
        var info = provider.GetDatasetInfo(profile);
        Assert.Contains(expectedLines.ToString("N0"), info.Description);
        Assert.StartsWith("Sphere-", info.Name);
    }

    [Fact]
    public void GetDatasetInfo_DescriptionContainsDprDimensions()
    {
        var provider = new SphereDatasetProvider();
        var info = provider.GetDatasetInfo("100k");
        Assert.Contains("768D", info.Description);
        Assert.Contains("DPR", info.Description);
    }

    [Fact]
    public void GetDatasetInfo_HasDownloadFile()
    {
        var provider = new SphereDatasetProvider();
        var info = provider.GetDatasetInfo("100k");
        Assert.Single(info.Files);
        Assert.Contains("full.sphere.100k.jsonl.tar.gz", info.Files[0].FileName);
        Assert.Contains("storage.googleapis.com", info.Files[0].Url);
    }

    [Fact]
    public async Task IsDatasetImportedAsync_NonExistentServer_ReturnsFalse()
    {
        var provider = new SphereDatasetProvider();
        var result = await provider.IsDatasetImportedAsync(
            "http://localhost:59999", "NonExistent", expectedMinDocuments: 100);
        Assert.False(result);
    }
}
