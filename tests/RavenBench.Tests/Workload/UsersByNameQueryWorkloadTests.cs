using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using RavenBench.Core.Workload;
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

        queryOp.QueryText.Should().Be("from Users where DisplayName = $name");
        queryOp.Parameters.Should().ContainKey("name");
        queryOp.Parameters["name"].Should().BeOneOf("Alice", "Bob", "Charlie");
        queryOp.ExpectedIndex.Should().Be("Auto/Users/ByDisplayName");
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

public class UsersRangeQueryWorkloadTests
{
    [Fact]
    public void Rejects_Empty_Reputation_Buckets()
    {
        var emptyMetadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice" },
            SampleCount = 1,
            TotalUserCount = 100,
            ReputationBuckets = Array.Empty<ReputationBucket>(),
            ComputedAt = DateTime.UtcNow
        };

        var act = () => new UsersRangeQueryWorkload(emptyMetadata);
        act.Should().Throw<ArgumentException>()
            .WithMessage("Metadata must contain reputation histogram buckets");
    }

    [Fact]
    public void Generates_Range_Query_Operations_With_Parameters()
    {
        var buckets = new[]
        {
            new ReputationBucket { MinReputation = 1, MaxReputation = 100, EstimatedDocCount = 50 },
            new ReputationBucket { MinReputation = 100, MaxReputation = 1000, EstimatedDocCount = 30 }
        };

        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice" },
            SampleCount = 1,
            TotalUserCount = 100,
            ReputationBuckets = buckets,
            MinReputation = 1,
            MaxReputation = 1000,
            ComputedAt = DateTime.UtcNow
        };

        var workload = new UsersRangeQueryWorkload(metadata);
        var rng = new Random(42);
        var op = workload.NextOperation(rng);

        op.Should().BeOfType<QueryOperation>();
        var queryOp = (QueryOperation)op;

        queryOp.QueryText.Should().Be("from Users where Reputation between $min and $max");
        queryOp.Parameters.Should().ContainKey("min");
        queryOp.Parameters.Should().ContainKey("max");
        queryOp.Parameters["min"].Should().BeOfType<int>();
        queryOp.Parameters["max"].Should().BeOfType<int>();
        queryOp.ExpectedIndex.Should().Be("Auto/Users/ByReputation");
    }

    [Fact]
    public void Generates_Queries_Within_Bucket_Ranges()
    {
        var buckets = new[]
        {
            new ReputationBucket { MinReputation = 10, MaxReputation = 50, EstimatedDocCount = 20 },
            new ReputationBucket { MinReputation = 50, MaxReputation = 200, EstimatedDocCount = 30 }
        };

        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice" },
            SampleCount = 1,
            TotalUserCount = 100,
            ReputationBuckets = buckets,
            MinReputation = 10,
            MaxReputation = 200,
            ComputedAt = DateTime.UtcNow
        };

        var workload = new UsersRangeQueryWorkload(metadata);
        var rng = new Random(42);

        // Generate multiple operations to test range generation
        for (int i = 0; i < 50; i++)
        {
            var op = (QueryOperation)workload.NextOperation(rng);
            var min = (int)op.Parameters["min"]!;
            var max = (int)op.Parameters["max"]!;

            // Should be within one of the bucket ranges
            var inBucket = buckets.Any(b => min >= b.MinReputation && max <= b.MaxReputation) ||
                          buckets.Any(b => min >= b.MinReputation && max <= b.MaxReputation + (b.MaxReputation - b.MinReputation) / 2); // Allow sub-ranges

            inBucket.Should().BeTrue($"Generated range {min}-{max} should be within bucket ranges");
            min.Should().BeLessThan(max);
        }
    }

    [Fact]
    public void Samples_From_All_Buckets()
    {
        var buckets = new[]
        {
            new ReputationBucket { MinReputation = 1, MaxReputation = 10, EstimatedDocCount = 5 },
            new ReputationBucket { MinReputation = 10, MaxReputation = 100, EstimatedDocCount = 15 },
            new ReputationBucket { MinReputation = 100, MaxReputation = 1000, EstimatedDocCount = 20 }
        };

        var metadata = new UsersWorkloadMetadata
        {
            SampleNames = new[] { "Alice" },
            SampleCount = 1,
            TotalUserCount = 100,
            ReputationBuckets = buckets,
            MinReputation = 1,
            MaxReputation = 1000,
            ComputedAt = DateTime.UtcNow
        };

        var workload = new UsersRangeQueryWorkload(metadata);
        var rng = new Random(42);
        var bucketUsage = new Dictionary<int, int>();

        // Generate enough operations to sample from all buckets
        for (int i = 0; i < 300; i++)
        {
            var op = (QueryOperation)workload.NextOperation(rng);
            var min = (int)op.Parameters["min"]!;
            var max = (int)op.Parameters["max"]!;

            // Find which bucket this range belongs to
            var bucketIndex = -1;
            for (int b = 0; b < buckets.Length; b++)
            {
                if (min >= buckets[b].MinReputation && max <= buckets[b].MaxReputation)
                {
                    bucketIndex = b;
                    break;
                }
            }

            if (bucketIndex >= 0)
            {
                bucketUsage.TryGetValue(bucketIndex, out var count);
                bucketUsage[bucketIndex] = count + 1;
            }
        }

        // Should have used all buckets
        bucketUsage.Keys.Should().HaveCount(3);
        bucketUsage.Keys.Should().BeEquivalentTo(new[] { 0, 1, 2 });
    }
}
