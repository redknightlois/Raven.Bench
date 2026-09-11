using System;
using System.Linq;
using FluentAssertions;
using RavenBench.Core.Ycsb;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins the four YCSB mixes the ycsb run sequence issues, and the two invariants a run against
/// an unloaded database used to violate: a read-only run must never substitute an insert for a
/// read when the keyspace looks empty, and an update-heavy run must issue field updates, never
/// document replaces.
/// </summary>
public class YcsbWorkloadSequenceTests
{
    private const int DocumentSize = 1024;
    private const int Seed = 42;

    [Fact]
    public void WorkloadC_Issues_Only_Reads()
    {
        var workload = new MixedProfileWorkload(YcsbRunKinds.WorkloadC, new UniformDistribution(), DocumentSize, Seed, initialKeyspace: 1000);
        var rng = new Random(1);

        var ops = Enumerable.Range(0, 500).Select(_ => workload.NextOperation(rng)).ToList();

        ops.Should().OnlyContain(op => op is ReadOperation);
    }

    [Fact]
    public void WorkloadC_Against_An_Empty_Keyspace_Still_Issues_Only_Reads()
    {
        // The mix alone decides the kind; an empty keyspace must never substitute an insert.
        // (A run driving this workload is expected to fail its own preload check first; this
        // test isolates the workload's own contract.)
        var workload = new MixedProfileWorkload(YcsbRunKinds.WorkloadC, new UniformDistribution(), DocumentSize, Seed, initialKeyspace: 0);
        var rng = new Random(1);

        var ops = Enumerable.Range(0, 200).Select(_ => workload.NextOperation(rng)).ToList();

        ops.Should().OnlyContain(op => op is ReadOperation);
    }

    [Fact]
    public void WorkloadA_Is_Half_Reads_Half_Field_Updates_Never_Inserts()
    {
        var workload = new MixedProfileWorkload(YcsbRunKinds.WorkloadA, new UniformDistribution(), DocumentSize, Seed, initialKeyspace: 1000);
        var rng = new Random(1);

        var ops = Enumerable.Range(0, 2000).Select(_ => workload.NextOperation(rng)).ToList();

        ops.Should().NotContain(op => op is InsertOperation<string>);
        var reads = ops.Count(op => op is ReadOperation);
        var updates = ops.Count(op => op is UpdateFieldOperation);
        (reads + updates).Should().Be(ops.Count);
        reads.Should().BeCloseTo(updates, 200); // ~50/50 within sampling noise
    }

    [Fact]
    public void WorkloadB_Reads_Far_More_Than_It_Updates()
    {
        var workload = new MixedProfileWorkload(YcsbRunKinds.WorkloadB, new UniformDistribution(), DocumentSize, Seed, initialKeyspace: 1000);
        var rng = new Random(1);

        var ops = Enumerable.Range(0, 2000).Select(_ => workload.NextOperation(rng)).ToList();

        ops.Should().NotContain(op => op is InsertOperation<string>);
        var reads = ops.Count(op => op is ReadOperation);
        var updates = ops.Count(op => op is UpdateFieldOperation);
        reads.Should().BeGreaterThan(updates * 10);
    }

    [Fact]
    public void InsertStream_Issues_Only_Single_Document_Inserts()
    {
        var workload = new WriteWorkload(DocumentSize, Seed, startingKey: 1000);
        var rng = new Random(1);

        var ops = Enumerable.Range(0, 50).Select(_ => workload.NextOperation(rng)).ToList();

        ops.Should().OnlyContain(op => op is InsertOperation<string>);
        ops.Select(op => ((InsertOperation<string>)op).Id).Should().OnlyHaveUniqueItems();
    }
}
