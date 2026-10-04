using System;
using System.IO;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

internal static class ClinicalWordsAvailability
{
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
/// Skips the test when the ClinicalWords parquet embeddings of the given dimensions are not available.
/// Download with: python datasets/prepare_clinical_embeddings.py
/// </summary>
public sealed class RequiresClinicalWordsFactAttribute : FactAttribute
{
    public RequiresClinicalWordsFactAttribute(int dimensions = 100)
    {
        if (ClinicalWordsAvailability.PathOf(dimensions) == null)
            Skip = $"ClinicalWords {dimensions}d embeddings not available. Run: python datasets/prepare_clinical_embeddings.py";
    }
}
