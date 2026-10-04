using System;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

/// <summary>One outcome rule for every transport: a missing document and a cancelled run are never successes or errors by accident.</summary>
public class TransportOutcomeTests
{
    [Fact]
    public async Task Plain_OperationCanceledException_Is_Cancelled_Only_When_The_Run_Token_Is_Cancelled()
    {
        using var cts = new CancellationTokenSource();
        Task<TransportResult> Throwing() => throw new OperationCanceledException();

        var live = await TransportResult.GuardedAsync(Throwing, cts.Token);
        cts.Cancel();
        var cancelled = await TransportResult.GuardedAsync(Throwing, cts.Token);

        live.Cancelled.Should().BeFalse();
        live.IsSuccess.Should().BeFalse();
        cancelled.Cancelled.Should().BeTrue();
        cancelled.IsSuccess.Should().BeTrue();
    }
}
