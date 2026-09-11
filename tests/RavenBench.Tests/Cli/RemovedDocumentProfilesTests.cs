using System;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using Spectre.Console.Cli;
using Xunit;

namespace RavenBench.Tests.Cli;

/// <summary>
/// Pins the single document-blend mechanism: the old reads, writes, mixed and bulk-writes
/// profiles, their mix options and the Reads profile member leave no path behind.
/// </summary>
public class RemovedDocumentProfilesTests
{
    [Theory]
    [InlineData("mixed")]
    [InlineData("reads")]
    [InlineData("read")]
    [InlineData("writes")]
    [InlineData("write")]
    [InlineData("bulk-writes")]
    [InlineData("bulkwrites")]
    public void Closed_Rejects_A_Removed_Document_Profile(string profile)
    {
        var settings = new ClosedSettings { Url = "http://localhost:8080", Database = "test", Profile = profile };

        var exception = Assert.Throws<ArgumentException>(() => settings.ToRunOptions());

        exception.Message.Should().Contain($"Invalid profile: {profile}");
        ValidProfileNames(exception.Message).Should().NotContain(profile);
    }

    [Theory]
    [InlineData("mixed")]
    [InlineData("reads")]
    [InlineData("writes")]
    [InlineData("bulk-writes")]
    public void Rate_Rejects_A_Removed_Document_Profile(string profile)
    {
        var settings = new RateSettings { Url = "http://localhost:8080", Database = "test", Profile = profile };

        var exception = Assert.Throws<ArgumentException>(() => settings.ToRunOptions());

        ValidProfileNames(exception.Message).Should().NotContain(profile);
    }

    [Theory]
    [InlineData("--reads")]
    [InlineData("--writes")]
    [InlineData("--updates")]
    public void Closed_Rejects_A_Removed_Mix_Option(string option)
    {
        var app = new CommandApp();
        app.Configure(cfg =>
        {
            cfg.PropagateExceptions();
            cfg.Settings.StrictParsing = true;
            cfg.AddCommand<ClosedCommand>("closed");
        });

        var act = () => app.Run(new[] { "closed", "--url", "http://localhost:1", "--database", "d", option, "50" });

        act.Should().Throw<CommandParseException>();
    }

    [Fact]
    public void Reads_Profile_Member_Is_Removed()
    {
        Enum.IsDefined(typeof(WorkloadProfile), "Reads").Should().BeFalse();
    }

    [Theory]
    [InlineData("query-by-id", WorkloadProfile.QueryById)]
    [InlineData("stackoverflow-random-reads", WorkloadProfile.StackOverflowRandomReads)]
    [InlineData("stackoverflow-text-search", WorkloadProfile.StackOverflowTextSearch)]
    [InlineData("query-users-by-name", WorkloadProfile.QueryUsersByName)]
    [InlineData("vector-search", WorkloadProfile.VectorSearch)]
    [InlineData("vector-search-exact", WorkloadProfile.VectorSearchExact)]
    [InlineData("patch", WorkloadProfile.Patch)]
    [InlineData("attachments", WorkloadProfile.Attachments)]
    public void Surviving_Profiles_Still_Parse(string profile, WorkloadProfile expected)
    {
        var settings = new ClosedSettings { Url = "http://localhost:8080", Database = "test", Profile = profile, Preload = 10 };

        settings.ToRunOptions().Profile.Should().Be(expected);
    }

    private static string[] ValidProfileNames(string message)
    {
        const string marker = "Valid options:";
        var start = message.IndexOf(marker, StringComparison.Ordinal);
        if (start < 0)
            return Array.Empty<string>();

        return message[(start + marker.Length)..]
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }
}
