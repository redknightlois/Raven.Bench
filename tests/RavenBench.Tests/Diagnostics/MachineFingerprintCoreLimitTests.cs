using FluentAssertions;
using RavenBench.Core.Diagnostics;
using Xunit;

namespace RavenBench.Tests.Diagnostics;

[Trait("Category", "Unit")]
public class MachineFingerprintCoreLimitTests
{
    [Fact]
    public void Physical_Core_Count_Obeys_The_Logical_Limit()
    {
        NativeMachineFingerprintSource.WithinLogicalLimit(machinePhysical: 16, logical: 4).Should().Be(4);
        NativeMachineFingerprintSource.WithinLogicalLimit(machinePhysical: 8, logical: 16).Should().Be(8);
    }
}
