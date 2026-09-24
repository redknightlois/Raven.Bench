using System;
using System.IO;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

internal static class ClinicalWordsAvailability
{
    private static readonly Lazy<bool> Cached = new(() => PathOf(100) != null);

    public static bool IsAvailable => Cached.Value;

    /// <summary>
    /// The parquet the prepare script writes under the repository's datasets directory, searched upwards from the test binaries.
    /// </summary>
    public static string? PathOf(int dimensions)
    {
        var relative = Path.Combine("datasets", $"clinical-words-{dimensions}", $"w2v_{dimensions}d_oa_cr_embeddings.parquet");
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var path = Path.Combine(dir.FullName, relative);
            if (File.Exists(path))
                return path;
        }
        return null;
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
