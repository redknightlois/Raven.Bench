using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Dataset.Vectors;
using RavenBench.VectorBench;
using Xunit;

namespace RavenBench.Tests.VectorBench;

[Trait("Category", "Unit")]
public class VectorRunnerCleanupTests
{
    private sealed class RunFailed() : InvalidOperationException("run step failed");

    private sealed class CleanupFailed() : InvalidOperationException("cleanup failed");

    private sealed class CountingTarget(bool cleanupThrows) : IVectorTarget
    {
        public int CleanupCalls { get; private set; }

        public Task CleanupAsync()
        {
            CleanupCalls++;
            return cleanupThrows ? Task.FromException(new CleanupFailed()) : Task.CompletedTask;
        }

        public IYcsbTransport Transport => throw new NotSupportedException();
        public string EffortFamily => "fake";
        public string FieldName => "v";
        public string FilterField => "label";
        public string? ExpectedIndex => null;
        public string VectorStorage => "float32";
        public string IdPrefix => "";
        public string InsertVisibility => "immediate";
        public DurabilityParity Durability => throw new NotSupportedException();
        public SearchEffort Effort(double value) => throw new NotSupportedException();
        public Task LoadAsync(IAsyncEnumerable<LabelledVector> vectors, CancellationToken ct) => Task.CompletedTask;
        public OperationBase InsertOperation(LabelledVector vector) => throw new NotSupportedException();
        public Task<OnDiskSize> StoredSizeAsync() => throw new NotSupportedException();
        public Task<IReadOnlyDictionary<string, string>> ReportedSettingsAsync(CancellationToken ct) => throw new NotSupportedException();
        public void Dispose() { }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AStepFailureAfterTheLoad_StillCleansUp_AndTheStepFailureReachesTheCaller(bool cleanupThrows)
    {
        var target = new CountingTarget(cleanupThrows);
        await Assert.ThrowsAsync<RunFailed>(() => RunCleanup.AfterAsync<int>(target.CleanupAsync, "Vector", () => throw new RunFailed()));
        Assert.Equal(1, target.CleanupCalls);
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 0)]
    public async Task ASuccessfulRun_CleansUpOnce_UnlessTheDataIsKept(bool keepData, int expectedCleanups)
    {
        var target = new CountingTarget(cleanupThrows: false);
        Assert.Equal(7, await RunCleanup.AfterAsync(keepData ? null : target.CleanupAsync, "Vector", () => Task.FromResult(7)));
        Assert.Equal(expectedCleanups, target.CleanupCalls);
    }
}
