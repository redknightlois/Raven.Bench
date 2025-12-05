namespace RavenBench.Dataset;

/// <summary>
/// Metadata about a known dataset that can be downloaded and imported.
/// </summary>
public sealed class DatasetInfo
{
    public required string Name { get; init; }
    public required string Description { get; init; }
    public required List<DatasetFile> Files { get; init; }
    public required int MaxQuestionId { get; init; }
    public required int MaxUserId { get; init; }
}

/// <summary>
/// A single dump file that can be downloaded and imported independently.
/// </summary>
public sealed class DatasetFile
{
    public required string FileName { get; init; }
    public required string Url { get; init; }
    public required string Type { get; init; } // "indexes", "users", "questions", "posts"
    public required long EstimatedSizeBytes { get; init; }
    public string? Description { get; init; }
    public string[]? MirrorUrls { get; init; }
}

/// <summary>
/// Dataset size profile for predefined configurations.
/// </summary>
public enum DatasetProfile
{
    Small,  // ~5GB: 5 post dumps + users
    Half,   // ~20GB: 20 post dumps + users
    Full    // ~50GB: Complete dataset
}

/// <summary>
/// Known dataset definitions.
/// </summary>
public static class KnownDatasets
{
    private const string S3Base = "https://stackoverflow-dump.s3.dualstack.us-west-2.amazonaws.com";

    /// <summary>
    /// Gets recommended database name for a dataset profile.
    /// </summary>
    public static string GetDatabaseName(DatasetProfile profile)
    {
        return profile switch
        {
            DatasetProfile.Small => "StackOverflow-5GB",   // 5 post dumps
            DatasetProfile.Half => "StackOverflow-20GB",   // 20 post dumps
            DatasetProfile.Full => "StackOverflow-50GB",   // Full dataset
            _ => throw new ArgumentException($"Unknown profile: {profile}")
        };
    }

    /// <summary>
    /// Gets database name for a custom dataset size (post dump count).
    /// </summary>
    public static string GetDatabaseNameForSize(int postDumpCount)
    {
        if (postDumpCount == 0)
            return "StackOverflow-50GB"; // Full dataset

        // Each post dump is roughly 1GB, plus ~2GB for users
        var estimatedGb = postDumpCount + 2;
        return $"StackOverflow-{estimatedGb}GB";
    }

    /// <summary>
    /// Gets dataset size (post dump count) for a profile.
    /// </summary>
    public static int GetDatasetSize(DatasetProfile profile)
    {
        return profile switch
        {
            DatasetProfile.Small => 5,
            DatasetProfile.Half => 20,
            DatasetProfile.Full => 0, // 0 = full dataset
            _ => throw new ArgumentException($"Unknown profile: {profile}")
        };
    }

    public static DatasetInfo StackOverflow => new()
    {
        Name = "StackOverflow",
        Description = "StackOverflow dataset from June 2022 with questions, users, and posts",
        MaxQuestionId = 12350817,
        MaxUserId = 5987285,
        Files = new List<DatasetFile>
        {
            new DatasetFile
            {
                FileName = "StackExchange-Indexes.ravendbdump",
                Url = $"{S3Base}/StackExchange-Indexes.ravendbdump",
                Type = "indexes",
                EstimatedSizeBytes = 100 * 1024 * 1024, // ~100 MB
                Description = "Index definitions for StackExchange data"
            },
            new DatasetFile
            {
                FileName = "StackOverflow-2022-06-06.ravendbdump",
                Url = $"{S3Base}/StackOverflow-2022-06-06.ravendbdump",
                Type = "full",
                EstimatedSizeBytes = 50L * 1024 * 1024 * 1024, // ~50 GB
                Description = "Complete StackOverflow dataset snapshot from June 2022"
            }
        }
    };

    public static DatasetInfo StackOverflowPartial(int postDumpCount) => new()
    {
        Name = "StackOverflow-Partial",
        Description = $"Partial StackOverflow dataset with users and {postDumpCount} post dump files",
        MaxQuestionId = 12350817,
        MaxUserId = 5987285,
        Files = GeneratePartialFiles(postDumpCount)
    };

    private static List<DatasetFile> GeneratePartialFiles(int postDumpCount)
    {
        var files = new List<DatasetFile>
        {
            // Index definitions must be imported first
            new DatasetFile
            {
                FileName = "Stackoverflow.indexes-dump",
                Url = $"{S3Base}/Stackoverflow.indexes-dump",
                Type = "indexes",
                EstimatedSizeBytes = 10 * 1024 * 1024, // ~10 MB
                Description = "Index definitions"
            },
            new DatasetFile
            {
                FileName = "users.dump",
                Url = $"{S3Base}/users.dump",
                Type = "users",
                EstimatedSizeBytes = 2L * 1024 * 1024 * 1024, // ~2 GB
                Description = "User data"
            }
        };

        for (int i = 1; i <= postDumpCount && i <= 45; i++)
        {
            files.Add(new DatasetFile
            {
                FileName = $"posts-{i:D3}.dump",
                Url = $"{S3Base}/posts-{i:D3}.dump",
                Type = "posts",
                EstimatedSizeBytes = 1024L * 1024 * 1024, // ~1 GB each
                Description = $"Post data part {i}"
            });
        }

        return files;
    }

    public static DatasetInfo? GetByName(string name)
    {
        return name.ToLowerInvariant() switch
        {
            "stackoverflow" => StackOverflow,
            _ => null
        };
    }
}
