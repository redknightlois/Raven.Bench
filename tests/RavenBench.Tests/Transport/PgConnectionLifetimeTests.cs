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

    private static Exception ServerError(string sqlState, string severity)
    {
        var fields = new Dictionary<char, string> { ['S'] = severity, ['V'] = severity, ['C'] = sqlState, ['M'] = "server error" };
        return (Exception)Activator.CreateInstance(typeof(Apex.PgClient.PgException), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance, null, [fields], null)!;
    }

    private static async Task<(PgConnectionSet<FakeConnection> Set, List<FakeConnection> Opened, List<CancellationToken> Tokens)> OpenSetAsync()
    {
        var opened = new List<FakeConnection>();
        var tokens = new List<CancellationToken>();
        var set = await PgConnectionSet<FakeConnection>.OpenAsync(1, ct =>
        {
            tokens.Add(ct);
            var connection = new FakeConnection();
            opened.Add(connection);
            return Task.FromResult(connection);
        });
        return (set, opened, tokens);
    }

    [Fact]
    public async Task Cancellation_Reaches_The_Caller_Without_A_Reconnect()
    {
        var (set, opened, _) = await OpenSetAsync();
        using var cts = new CancellationTokenSource();

        var act = () => set.UseAsync<int>(_ =>
        {
            cts.Cancel();
            throw new OperationCanceledException(cts.Token);
        }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        opened.Should().ContainSingle();
        opened[0].Disposals.Should().Be(0);
    }

    [Fact]
    public async Task A_Server_Side_Sql_Error_Reaches_The_Caller_And_Keeps_The_Same_Connection()
    {
        var (set, opened, _) = await OpenSetAsync();
        var duplicateKey = ServerError("23505", "ERROR");

        var act = () => set.UseAsync<int>(_ => throw duplicateKey, CancellationToken.None);

        (await act.Should().ThrowAsync<Exception>()).Which.Should().BeSameAs(duplicateKey);
        opened.Should().ContainSingle();
        opened[0].Disposals.Should().Be(0);
        (await set.UseAsync(slot => Task.FromResult(slot), CancellationToken.None)).Should().BeSameAs(opened[0]);
    }

    [Fact]
    public async Task A_Connection_Level_Failure_Replaces_The_Connection_Under_The_Callers_Token()
    {
        var (set, opened, tokens) = await OpenSetAsync();
        using var cts = new CancellationTokenSource();

        var act = () => set.UseAsync<int>(_ => throw new System.IO.IOException("connection reset"), cts.Token);

        await act.Should().ThrowAsync<System.IO.IOException>();
        opened.Should().HaveCount(2);
        opened[0].Disposals.Should().Be(1);
        tokens[^1].Should().Be(cts.Token);
        (await set.UseAsync(slot => Task.FromResult(slot), CancellationToken.None)).Should().BeSameAs(opened[1]);
    }

    [Fact]
    public async Task A_Failed_Copy_Replaces_The_Connection_Even_For_A_Non_Fatal_Server_Error()
    {
        var (set, opened, _) = await OpenSetAsync();

        var act = () => set.UseCopyAsync<int>(_ => throw ServerError("23505", "ERROR"), CancellationToken.None);

        await act.Should().ThrowAsync<Exception>();
        opened.Should().HaveCount(2);
        opened[0].Disposals.Should().Be(1);
    }
}
