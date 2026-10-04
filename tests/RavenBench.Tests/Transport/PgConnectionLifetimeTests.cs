using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

public class PgConnectionLifetimeTests
{
    private sealed class FakeConnection(Exception? disposeFailure = null) : IAsyncDisposable
    {
        public int Disposals { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposals++;
            return disposeFailure is null ? ValueTask.CompletedTask : ValueTask.FromException(disposeFailure);
        }
    }

    [Fact]
    public async Task A_Slot_Returned_After_Dispose_Is_Closed_And_No_Opened_Connection_Stays_Open()
    {
        var opened = new List<FakeConnection>();
        var set = await PgConnectionSet<FakeConnection>.OpenAsync(2, () =>
        {
            var connection = new FakeConnection();
            opened.Add(connection);
            return Task.FromResult(connection);
        });
        var rented = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var inFlight = set.UseAsync(async _ =>
        {
            rented.SetResult();
            await release.Task;
            return 0;
        }, CancellationToken.None);
        await rented.Task;
        await set.DisposeAsync();
        release.SetResult();
        await inFlight;

        opened.Should().HaveCount(2);
        opened.Should().OnlyContain(c => c.Disposals == 1);
    }

    [Fact]
    public async Task A_Statement_Dispose_Failure_Still_Disposes_The_Connection_And_Surfaces_The_First_Error()
    {
        var first = new InvalidOperationException("read statement");
        var statements = new[] { new FakeConnection(first), new FakeConnection(new InvalidOperationException("insert statement")), new FakeConnection() };
        var connection = new FakeConnection();

        var act = async () => await PostgresYcsbTransport.DisposeAllAsync([.. statements, connection]);

        (await act.Should().ThrowAsync<InvalidOperationException>()).Which.Should().BeSameAs(first);
        statements.Append(connection).Should().OnlyContain(c => c.Disposals == 1);
    }
}
