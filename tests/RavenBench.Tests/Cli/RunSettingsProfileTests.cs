using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Util;
using Xunit;

namespace RavenBench.Tests.Cli;

public class RunSettingsProfileTests
{
    [Fact]
    public void ToRunOptions_AllowsQueryUsersByNameProfile()
    {
        // Arrange: configure the new equality workload profile that should be supported per PRD.
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "bench",
            Profile = "query-users-by-name"
        };

        // Act
        var options = settings.ToRunOptions();

        // Assert
        options.Profile.Should().Be(WorkloadProfile.QueryUsersByName);
    }
}
