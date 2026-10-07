using System.Threading.Channels;
using Apex.PgClient;

namespace RavenBench.Core.Transport;

/// <summary>
/// The one Apex.PgClient connect path and plain connection set every PostgreSQL target shares. Every
/// connection it opens runs under the durability parity setting. The set holds exactly its size in
/// connections, each prepared once by the owner's callback; Apex's pipelining pool is not used.
/// </summary>
internal sealed class PgConnectionSet<TSlot> where TSlot : class, IAsyncDisposable
{
    private readonly Func<CancellationToken, Task<TSlot>> _openSlot;
    private readonly Channel<TSlot> _slots;

    private PgConnectionSet(int size, Func<CancellationToken, Task<TSlot>> openSlot)
    {
        _openSlot = openSlot;
        _slots = Channel.CreateBounded<TSlot>(size);
    }

    /// <summary>Opens one connection and applies <c>synchronous_commit=on</c> to it.</summary>
    public static async Task<PgConnection> ConnectAsync(PgConnectOptions options, CancellationToken ct = default)
    {
        var connection = await PgClient.ConnectAsync(options, ct).ConfigureAwait(false);
        try
        {
            await connection.ExecuteAsync(PostgresYcsbTransport.SetDurabilitySql, ct).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Opens <paramref name="size"/> connections; a failure closes the ones already open.</summary>
    public static Task<PgConnectionSet<TSlot>> OpenAsync(PgConnectOptions options, int size, Func<PgConnection, Task<TSlot>> prepare) =>
        OpenAsync(size, async ct =>
        {
            var connection = await ConnectAsync(options, ct).ConfigureAwait(false);
            try
            {
                return await prepare(connection).ConfigureAwait(false);
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        });

    /// <summary>Opens <paramref name="size"/> slots through <paramref name="openSlot"/>, which owns the slot it returns or disposes what it opened when it throws.</summary>
    internal static Task<PgConnectionSet<TSlot>> OpenAsync(int size, Func<Task<TSlot>> openSlot) =>
        OpenAsync(size, _ => openSlot());

    /// <summary>As <see cref="OpenAsync(int, Func{Task{TSlot}})"/>; <paramref name="openSlot"/> also opens each replacement, under the token of the call that needed it.</summary>
    internal static async Task<PgConnectionSet<TSlot>> OpenAsync(int size, Func<CancellationToken, Task<TSlot>> openSlot)
    {
        var set = new PgConnectionSet<TSlot>(size, openSlot);
        try
        {
            for (int i = 0; i < size; i++)
                set._slots.Writer.TryWrite(await openSlot(CancellationToken.None).ConfigureAwait(false));
            return set;
        }
        catch
        {
            await set.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs one body on a rented slot. A failure after the caller's token was cancelled, and a server-side error that leaves the connection
    /// usable, such as a duplicate key, reach the caller and the slot goes back to the set. Any other
    /// failure, including a cancel the caller's token does not own, means the connection state is unknown: the slot is disposed and replaced under the caller's token.
    /// If the server is gone, the set is closed so a later rent fails fast instead of blocking on an empty set.
    /// </summary>
    public Task<T> UseAsync<T>(Func<TSlot, Task<T>> body, CancellationToken ct) => UseAsync(body, ct, keepOnServerError: true);

    /// <summary>As <see cref="UseAsync{T}(Func{TSlot, Task{T}}, CancellationToken)"/> for a COPY: a failed COPY leaves the connection unusable even when the server error is not fatal, so any failure other than cancellation replaces the slot.</summary>
    public Task<T> UseCopyAsync<T>(Func<TSlot, Task<T>> body, CancellationToken ct) => UseAsync(body, ct, keepOnServerError: false);

    private async Task<T> UseAsync<T>(Func<TSlot, Task<T>> body, CancellationToken ct, bool keepOnServerError)
    {
        var slot = await _slots.Reader.ReadAsync(ct).ConfigureAwait(false);
        try
        {
            var result = await body(slot).ConfigureAwait(false);
            await ReturnAsync(slot).ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            if (ct.IsCancellationRequested || (keepOnServerError && ex is PgException { IsFatal: false }))
                await ReturnAsync(slot).ConfigureAwait(false);
            else
                await ReplaceAsync(slot, ct).ConfigureAwait(false);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _slots.Writer.TryComplete();
        while (_slots.Reader.TryRead(out var slot))
            await slot.DisposeAsync().ConfigureAwait(false);
    }

    // A completed set takes no slot back, so the slot's connection is closed here.
    private async ValueTask ReturnAsync(TSlot slot)
    {
        if (_slots.Writer.TryWrite(slot) == false)
            await slot.DisposeAsync().ConfigureAwait(false);
    }

    private async Task ReplaceAsync(TSlot slot, CancellationToken ct)
    {
        // Disposing a connection the driver already closed can itself throw; that must not replace
        // the operation's own error with the teardown failure.
        try { await slot.DisposeAsync().ConfigureAwait(false); } catch (Exception) { }
        try
        {
            await ReturnAsync(await _openSlot(ct).ConfigureAwait(false)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _slots.Writer.TryComplete(ex);
        }
    }
}
