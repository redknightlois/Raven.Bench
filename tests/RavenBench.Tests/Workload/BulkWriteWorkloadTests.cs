using System;
using System.Linq;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Workload;

public class BulkWriteWorkloadTests
{
    [Fact]
    public void Drawing_A_Batch_Builds_No_Payload_And_The_Sent_Documents_Match_Seed_Id_And_Size()
    {
        const int seed = 11, size = 512;
        var workload = new BulkWriteWorkload(size, batchSize: 4, seed, targetCount: 6);

        // The closed-loop generator draws operations under its shared lock; nothing is built there.
        var first = (BulkInsertOperation<string>)workload.NextOperation(new Random(1));
        var second = (BulkInsertOperation<string>)workload.NextOperation(new Random(1));
        first.RecordCount.Should().Be(4);
        first.DocumentsBuilt.Should().BeFalse();
        second.DocumentsBuilt.Should().BeFalse();
        workload.IsExhausted.Should().BeTrue();

        var sent = first.Documents.Concat(second.Documents).ToList();

        sent.Select(d => d.Id).Should().Equal(Enumerable.Range(1, 6).Select(i => BenchIds.IdFor(i)));
        sent.Should().OnlyContain(d => d.Document == PayloadGenerator.Generate(seed, d.Id, size));
    }
}
