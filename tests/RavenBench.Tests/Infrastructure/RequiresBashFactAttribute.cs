using System.IO;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// Skips the test when no bash shell is present. The ycsb entry script is a bash script, and the
/// test drives it rather than a copy of its logic.
/// </summary>
public sealed class RequiresBashFactAttribute : FactAttribute
{
    /// <summary>The skip reason of a test that starts /bin/bash; null when bash is present. Gates that also need a database add it to theirs.</summary>
    public static string? SkipReason => File.Exists("/bin/bash") ? null : "A bash shell is required to drive the benchmark scripts.";

    public RequiresBashFactAttribute()
    {
        Skip = SkipReason;
    }
}
