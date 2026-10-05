using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Cli;
using Xunit;

namespace RavenBench.Tests.Cli;

[Trait("Category", "Unit")]
public class IndexBuildFiguresTests
{
    [Fact]
    public void Docs_Per_Second_Counts_Only_The_Mapped_Collections()
    {
        var counts = new Dictionary<string, long> { ["Users"] = 300, ["Questions"] = 5000, ["@hilo"] = 7 };

        IndexBuildCommand.MappedDocumentCount(["Users"], counts).Should().Be(counts["Users"]);
        IndexBuildCommand.MappedDocumentCount(["Users", "Questions"], counts).Should().Be(counts["Users"] + counts["Questions"]);
        IndexBuildCommand.MappedDocumentCount(["Empty"], counts).Should().Be(0);
    }
}
