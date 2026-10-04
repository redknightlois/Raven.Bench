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
    private readonly Func<Task<TSlot>> _openSlot;
    private readonly Channel<TSlot> _slots;

    private PgConnectionSet(int size, Func<Task<TSlot>> openSlot)
    {
        _openSlot = openSlot;
        _slots = Channel.CreateBounded<TSlot>(size);
    }

    /// <summary>Opens one connection and applies <c>synchronous_commit=on</c> to it.</summary>
    public static async Task<PgConnection> ConnectAsync(PgConnectOptions options)
    {
        var connection = await PgClient.ConnectAsync(options, CancellationToken.None).ConfigureAwait(false);
        try
        {
            await connection.ExecuteAsync(PostgresYcsbTransport.SetDurabilitySql, CancellationToken.None).ConfigureAwait(false);
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
        OpenAsync(size, async () =>
        {
            var connection = await ConnectAsync(options).ConfigureAwait(false);
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
    internal static async Task<PgConnectionSet<TSlot>> OpenAsync(int size, Func<Task<TSlot>> openSlot)
    {
        var set = new PgConnectionSet<TSlot>(size, openSlot);
        try
        {
            for (int i = 0; i < size; i++)
                set._slots.Writer.TryWrite(await openSlot().ConfigureAwait(false));
            return set;
        }
        catch
        {
            await set.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs one body on a rented slot. A slot the body failed on or was cancelled on is disposed and
    /// replaced: the driver closes the connection on a protocol-level error, and a cancel can leave an
    /// unread response, an open transaction or session state the slot does not know. If the server is
    /// gone, the set is closed so a later rent fails fast instead of blocking on an empty set.
    /// </summary>
    public async Task<T> UseAsync<T>(Func<TSlot, Task<T>> body, CancellationToken ct)
    {
        var slot = await _slots.Reader.ReadAsync(ct).ConfigureAwait(false);
        try
        {
            var result = await body(slot).ConfigureAwait(false);
            await ReturnAsync(slot).ConfigureAwait(false);
            return result;
        }
        catch
        {
            await ReplaceAsync(slot).ConfigureAwait(false);
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

    private async Task ReplaceAsync(TSlot slot)
    {
        // Disposing a connection the driver already closed can itself throw; that must not replace
        // the operation's own error with the teardown failure.
        try { await slot.DisposeAsync().ConfigureAwait(false); } catch (Exception) { }
        try
        {
            await ReturnAsync(await _openSlot().ConfigureAwait(false)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _slots.Writer.TryComplete(ex);
        }
    }
}
