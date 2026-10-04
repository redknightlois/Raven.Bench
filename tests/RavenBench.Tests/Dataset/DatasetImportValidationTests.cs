using System;
using System.Threading.Tasks;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests.Dataset;

[Trait("Category", "Unit")]
public class DatasetImportValidationTests
{
    // The cache directory is a file, so any step past validation fails on it before a server or a download is reached.
    private static RunOptions Options(string dataset, int size, bool skipIfExists, string? profile = null) => new()
    {
        Url = "http://127.0.0.1:1",
        Database = "unused",
        Dataset = dataset,
        DatasetSize = size,
        DatasetSkipIfExists = skipIfExists,
        DatasetProfile = profile,
        DatasetCacheDir = typeof(DatasetImportValidationTests).Assembly.Location
    };

    [Theory]
    [InlineData(0, false, null)]
    [InlineData(3, false, null)]
    [InlineData(3, true, null)]
    [InlineData(0, true, "Small")]
    public async Task An_Unknown_Dataset_Name_Fails_Before_Any_Import_On_Every_Branch(int size, bool skipIfExists, string? profile) =>
        await Assert.ThrowsAsync<ArgumentException>(() => DatasetImportCoordinator.ImportDatasetAsync(Options("bogus", size, skipIfExists, profile)));

    [Fact]
    public async Task A_Negative_Dataset_Size_Fails_Before_A_Database_Name_Is_Derived() =>
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => DatasetImportCoordinator.ImportDatasetAsync(Options("stackoverflow", -2, skipIfExists: true)));
}
