using System;
using System.IO;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

internal static class ClinicalWordsAvailability
{
    private static readonly Lazy<bool> Cached = new(Check);

    public static bool IsAvailable => Cached.Value;

    private static bool Check()
    {
        // Mirror the search logic from ClinicalWordsDatasetProvider.GetParquetPath()
        const string fileName = "w2v_100d_oa_cr_embeddings.parquet";

        var startDirs = new[]
        {
            AppContext.BaseDirectory,
            Directory.GetCurrentDirectory(),
        };

        foreach (var startDir in startDirs)
        {
            var dir = new DirectoryInfo(startDir);
            while (dir != null)
            {
                var path = Path.Combine(dir.FullName, "datasets", fileName);
                if (File.Exists(path))
                    return true;
                dir = dir.Parent;
            }
        }

        return false;
    }
}

/// <summary>
/// Skips the test when the ClinicalWords parquet embeddings file is not available.
/// Download with: python datasets/prepare_clinical_embeddings.py
/// </summary>
public sealed class RequiresClinicalWordsFactAttribute : FactAttribute
{
    private const string SkipReason = "ClinicalWords embeddings not available. Run: python datasets/prepare_clinical_embeddings.py";

    public RequiresClinicalWordsFactAttribute()
    {
        if (ClinicalWordsAvailability.IsAvailable == false)
            Skip = SkipReason;
    }
}

/// <summary>
/// Skips the theory when the ClinicalWords parquet embeddings file is not available.
/// Download with: python datasets/prepare_clinical_embeddings.py
/// </summary>
public sealed class RequiresClinicalWordsTheoryAttribute : TheoryAttribute
{
    private const string SkipReason = "ClinicalWords embeddings not available. Run: python datasets/prepare_clinical_embeddings.py";

    public RequiresClinicalWordsTheoryAttribute()
    {
        if (ClinicalWordsAvailability.IsAvailable == false)
            Skip = SkipReason;
    }
}
