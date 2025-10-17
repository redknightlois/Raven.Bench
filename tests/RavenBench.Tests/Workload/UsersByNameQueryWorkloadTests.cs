using System;
using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Workload;
using Xunit;

namespace RavenBench.Tests.Workload;

public class UsersByNameQueryWorkloadTests
{
    [Fact]
    public void Rejects_Empty_Metadata()
    {
        var emptyMetadata = new UsersWorkloadMetadata
        {
            SampleNames = Array.Empty<string>(),
            SampleCount = 0,
            TotalUserCount = 0,
            ComputedAt = DateTime.UtcNow
        };

        var act = () => new UsersByNameQueryWorkload(emptyMetadata);
        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain sampled user names");
    }

    [Fact]
    public void Generates_Query_Operations_With_Parameters()
    {
        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice", "Bob", "Charlie" },
            SampleCount = 3,
            TotalUserCount = 1000,
            ComputedAt = DateTime.UtcNow
        };

        var workload = new UsersByNameQueryWorkload(metadata);
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<QueryOperation>();
        var queryOp = (QueryOperation)op;

        queryOp.QueryText.Should().Be("from Users where Name = $name");
        queryOp.Parameters.Should().ContainKey("name");
        queryOp.Parameters["name"].Should().BeOneOf("Alice", "Bob", "Charlie");
        queryOp.ExpectedIndex.Should().Be("Auto/Users/ByName");
    }

    [Fact]
    public void Samples_All_Names_Uniformly()
    {
        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice", "Bob", "Charlie" },
            SampleCount = 3,
            TotalUserCount = 1000,
            ComputedAt = DateTime.UtcNow
        };

        var workload = new UsersByNameQueryWorkload(metadata);
        var rng = new Random(42);
        var namesSeen = new HashSet<string>();

        // Generate enough operations to see all names
        for (int i = 0; i < 100; i++)
        {
            var op = (QueryOperation)workload.NextOperation(rng);
            namesSeen.Add(op.Parameters["name"]!.ToString()!);
        }

        // With 100 operations and 3 names, we should see all of them
        namesSeen.Should().BeEquivalentTo("Alice", "Bob", "Charlie");
    }

    [Fact]
    public void Metadata_Tracks_Sample_And_Total_Counts()
    {
        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice", "Bob", "Charlie", "David", "Eve" },
            SampleCount = 5,
            TotalUserCount = 10000,
            ComputedAt = DateTime.UtcNow
        };

        metadata.SampleCount.Should().Be(5);
        metadata.TotalUserCount.Should().Be(10000);
        metadata.SampleNames.Length.Should().Be((int)metadata.SampleCount);
    }
}
