using System.IO;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// Skips the test when no bash shell is present. The ycsb entry script is a bash script, and the
/// test drives it rather than a copy of its logic.
/// </summary>
public sealed class RequiresBashFactAttribute : FactAttribute
{
    public RequiresBashFactAttribute()
    {
        if (File.Exists("/bin/bash") == false)
            Skip = "A bash shell is required to drive benchmarks/ycsb/run.sh.";
    }
}
