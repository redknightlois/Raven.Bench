using System.Buffers;
using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Sources;

namespace RavenBench.Core.Transport;

/// <summary>One request as both raw paths send it. <see cref="Body"/> may point into a reused buffer and is valid only until the caller's next await.</summary>
internal readonly record struct RawRequest(
    HttpMethod Method,
    string Target,
    ReadOnlyMemory<byte> Body,
    string? ContentType,
    bool ReadsEnvelope,
    bool TimeoutCoversBody,
    string? IndexName);

internal readonly record struct Http1Response(
    int Status,
    long BytesIn,
    QueryEnvelope Envelope,
    string? ErrorBody,
    string? Failure,
    bool Cancelled)
{
    public static Http1Response Failed(string failure) => new(0, 0, default, null, failure, false);
    public static Http1Response CancelledResponse { get; } = new(0, 0, default, null, null, true);
}

/// <summary>
/// A request sent on an <see cref="Http1Connection"/>, completed exactly once: by its response, by a
/// connection failure, or by cancellation. Completion runs the awaiting caller inline on the completing thread.
/// </summary>
internal sealed class Http1Exchange : IValueTaskSource<Http1Response>
{
    private ManualResetValueTaskSourceCore<Http1Response> _core;
    private int _completed;

    public Http1Exchange(bool readsEnvelope, bool timeoutCoversBody, long bytesOut)
    {
        ReadsEnvelope = readsEnvelope;
        TimeoutCoversBody = timeoutCoversBody;
        BytesOut = bytesOut;
    }

    public bool ReadsEnvelope { get; }
    public bool TimeoutCoversBody { get; }

    /// <summary>The request's bytes as written to the socket, headers included.</summary>
    public long BytesOut { get; }

    public ValueTask<Http1Response> Response => new(this, _core.Version);

    public void Complete(in Http1Response response)
    {
        if (Interlocked.Exchange(ref _completed, 1) == 0)
            _core.SetResult(response);
    }

    public Http1Response GetResult(short token) => _core.GetResult(token);
    public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        _core.OnCompleted(continuation, state, token, flags);
}

/// <summary>
/// An HTTP/1.1 client connection over a raw socket. Requests are written in call order and may be
/// pipelined; responses are matched to requests in that order. One receive loop parses every response
/// and completes its exchange; requests queued while the loop is completing a received batch, or while a
/// send is running, leave together in the next single send.
/// </summary>
internal sealed class Http1Connection
{
    private const int MaxHeaderBytes = 64 * 1024;
    private const int MaxChunkLineBytes = 1024;
    private static readonly byte[] CrLf = "\r\n"u8.ToArray();

    private readonly Socket _socket = new(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
    private readonly byte[] _fixedHeaders;
    private readonly object _gate = new();

    // Guarded by _gate.
    private readonly Queue<Http1Exchange> _inFlight = new();
    private ArrayBufferWriter<byte> _pending = new(4096);
    private ArrayBufferWriter<byte> _sending = new(4096);
    private bool _flushing;
    private bool _dispatching;
    private bool _dead;
    private long _headSince;
    private bool _headHeadersRead;

    // Owned by the receive loop.
    private byte[] _in = new byte[16 * 1024];
    private int _start;
    private int _end;
    private readonly ArrayBufferWriter<byte> _body = new(4096);
    // Start of a Content-Length body read in place from _in, -1 when the body is in _body; valid until the loop next compacts or receives.
    private int _inPlaceStart = -1;
    private int _inPlaceLength;
    private Phase _phase = Phase.Headers;
    private long _remaining;
    private long _bytesIn;
    private int _status;
    private bool _collectBody;
    private bool _closeAfter;

    private enum Phase { Headers, Body, ChunkSize, ChunkData, ChunkEnd, Trailers }

    /// <param name="fixedHeaders">Header lines every request carries, each ending in CRLF.</param>
    public Http1Connection(string host, int port, byte[] fixedHeaders, TimeSpan connectTimeout)
    {
        _fixedHeaders = fixedHeaders;
        Ready = ConnectAsync(host, port, connectTimeout);
    }

    /// <summary>Completes when the socket is connected; faults with the connect failure.</summary>
    public Task Ready { get; }

    public bool IsDead => Volatile.Read(ref _dead);

    private async Task ConnectAsync(string host, int port, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await _socket.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            Fail("connect timed out");
            throw new TimeoutException($"Connecting to {host}:{port} timed out after {timeout.TotalSeconds:F0} seconds.", ex);
        }
        catch (SocketException ex)
        {
            Fail($"connect failed: {ex.Message}");
            throw;
        }

        _ = ReceiveLoopAsync();
    }

    /// <summary>
    /// Queues the request and returns its exchange. The body is copied before this returns. A request
    /// on a dead connection completes at once as a failure.
    /// </summary>
    public Http1Exchange Send(in RawRequest request)
    {
        var method = request.Method.Method;
        var target = request.Target;
        foreach (var c in target)
        {
            // Every dynamic part of a target is percent-escaped, so a control, space or non-ASCII char is a caller bug that would corrupt the request line.
            if (c <= ' ' || c > '~')
                throw new ArgumentException($"The request target '{target}' contains a character that is not allowed in an HTTP request line.", nameof(request));
        }

        bool flush = false;
        Http1Exchange exchange;
        lock (_gate)
        {
            if (_dead)
            {
                exchange = new Http1Exchange(request.ReadsEnvelope, request.TimeoutCoversBody, 0);
                exchange.Complete(Http1Response.Failed("The connection is closed."));
                return exchange;
            }

            var before = _pending.WrittenCount;
            WriteAscii(_pending, method);
            WriteAscii(_pending, " ");
            WriteAscii(_pending, target);
            WriteAscii(_pending, " HTTP/1.1\r\n");
            _pending.Write(_fixedHeaders);
            if (request.ContentType != null)
            {
                WriteAscii(_pending, "Content-Type: ");
                WriteAscii(_pending, request.ContentType);
                WriteAscii(_pending, "\r\nContent-Length: ");
                var span = _pending.GetSpan(20);
                Utf8Formatter.TryFormat(request.Body.Length, span, out var written);
                _pending.Advance(written);
                _pending.Write(CrLf);
            }
            _pending.Write(CrLf);
            _pending.Write(request.Body.Span);

            exchange = new Http1Exchange(request.ReadsEnvelope, request.TimeoutCoversBody, _pending.WrittenCount - before);
            _inFlight.Enqueue(exchange);
            if (_inFlight.Count == 1)
                MarkNewHead();

            if (_flushing == false && _dispatching == false)
                flush = _flushing = true;
        }

        if (flush)
            _ = FlushAsync();
        return exchange;
    }

    private static void WriteAscii(ArrayBufferWriter<byte> writer, string text)
    {
        var span = writer.GetSpan(text.Length);
        writer.Advance(Encoding.ASCII.GetBytes(text, span));
    }

    // Caller holds _gate.
    private void MarkNewHead()
    {
        _headSince = Stopwatch.GetTimestamp();
        _headHeadersRead = false;
    }

    // At most one runs at a time; it exits once nothing is pending, releasing the flush turn under the lock.
    private async Task FlushAsync()
    {
        try
        {
            while (true)
            {
                ArrayBufferWriter<byte> batch;
                lock (_gate)
                {
                    if (_pending.WrittenCount == 0 || _dead)
                    {
                        _flushing = false;
                        return;
                    }
                    batch = _pending;
                    _pending = _sending;
                    _sending = batch;
                }

                var remaining = batch.WrittenMemory;
                while (remaining.Length > 0)
                {
                    var sent = await _socket.SendAsync(remaining, SocketFlags.None).ConfigureAwait(false);
                    remaining = remaining[sent..];
                }
                batch.ResetWrittenCount();
            }
        }
        catch (SocketException ex)
        {
            Fail($"send failed: {ex.Message}");
        }
        catch (ObjectDisposedException)
        {
            Fail("send failed: the connection was closed.");
        }
    }

    private async Task ReceiveLoopAsync()
    {
        try
        {
            while (true)
            {
                if (DispatchReceived() == false)
                    return;

                MakeRoom();
                var read = await _socket.ReceiveAsync(_in.AsMemory(_end), SocketFlags.None).ConfigureAwait(false);
                if (read == 0)
                {
                    Fail("The server closed the connection.");
                    return;
                }
                _end += read;
            }
        }
        catch (SocketException ex)
        {
            Fail($"receive failed: {ex.Message}");
        }
        catch (ObjectDisposedException)
        {
            Fail("receive failed: the connection was closed.");
        }
        catch (InvalidDataException ex)
        {
            Fail($"malformed HTTP response: {ex.Message}");
        }
    }

    // Completes every response already received, then sends whatever those completions queued. False once the connection is closed.
    private bool DispatchReceived()
    {
        lock (_gate)
            _dispatching = true;

        var open = true;
        try
        {
            while (TryReadResponse())
            {
                Http1Exchange head;
                lock (_gate)
                {
                    head = _inFlight.Dequeue();
                    if (_inFlight.Count > 0)
                        MarkNewHead();
                }

                var response = BuildResponse(head);
                // Dead before the head completes: its caller continues inline and must not reuse this connection.
                if (_closeAfter)
                {
                    Fail("The server closed the connection (Connection: close).");
                    head.Complete(response);
                    open = false;
                    break;
                }

                head.Complete(response);
            }
        }
        finally
        {
            bool flush;
            lock (_gate)
            {
                _dispatching = false;
                flush = _flushing == false && _pending.WrittenCount > 0 && _dead == false;
                if (flush)
                    _flushing = true;
            }
            if (flush)
                _ = FlushAsync();
        }

        return open;
    }

    private Http1Response BuildResponse(Http1Exchange exchange)
    {
        var body = _inPlaceStart < 0 ? _body.WrittenSpan : _in.AsSpan(_inPlaceStart, _inPlaceLength);
        if (_status is < 200 or > 299)
            return new Http1Response(_status, _bytesIn, default, Encoding.UTF8.GetString(body), null, false);

        if (exchange.ReadsEnvelope == false)
            return new Http1Response(_status, _bytesIn, default, null, null, false);

        try
        {
            return new Http1Response(_status, _bytesIn, QueryEnvelope.Read(body), null, null, false);
        }
        catch (JsonException ex)
        {
            return Http1Response.Failed($"Exception: {ex.GetType().Name}: {ex.Message}");
        }
        catch (InvalidOperationException ex)
        {
            return Http1Response.Failed($"Exception: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private void MakeRoom()
    {
        if (_start == _end)
        {
            _start = _end = 0;
            return;
        }
        if (_end < _in.Length)
            return;
        if (_start > 0)
        {
            Buffer.BlockCopy(_in, _start, _in, 0, _end - _start);
            _end -= _start;
            _start = 0;
            return;
        }
        // Only a header block or a chunk-size line can fill the buffer, and both are bounded below the limits enforced while parsing.
        Array.Resize(ref _in, _in.Length * 2);
    }

    /// <summary>Advances the parser over the received bytes; true when one whole response has been read.</summary>
    private bool TryReadResponse()
    {
        while (true)
        {
            var available = _in.AsSpan(_start, _end - _start);
            switch (_phase)
            {
                case Phase.Headers:
                {
                    var end = available.IndexOf("\r\n\r\n"u8);
                    if (end < 0)
                    {
                        if (available.Length >= MaxHeaderBytes)
                            throw new InvalidDataException($"the response header block exceeds {MaxHeaderBytes} bytes.");
                        return false;
                    }
                    Http1Exchange head;
                    lock (_gate)
                    {
                        if (_inFlight.Count == 0)
                            throw new InvalidDataException("the server sent a response to no request.");
                        head = _inFlight.Peek();
                        _headHeadersRead = true;
                    }

                    var framing = ParseHeaders(available[..end]);
                    Consume(end + 4);
                    _body.ResetWrittenCount();
                    _inPlaceStart = -1;
                    _collectBody = head.ReadsEnvelope || _status is < 200 or > 299;

                    if (_status is 204 or 304)
                        return Finish();
                    if (framing == Framing.Chunked)
                    {
                        _phase = Phase.ChunkSize;
                    }
                    else
                    {
                        if (_remaining == 0)
                            return Finish();
                        _phase = Phase.Body;
                    }
                    break;
                }
                case Phase.Body:
                case Phase.ChunkData:
                {
                    if (available.Length == 0)
                        return false;
                    var take = (int)Math.Min(_remaining, available.Length);
                    if (_collectBody && _phase == Phase.Body && take == _remaining && _body.WrittenCount == 0)
                    {
                        _inPlaceStart = _start;
                        _inPlaceLength = take;
                    }
                    else if (_collectBody)
                    {
                        _body.Write(available[..take]);
                    }
                    Consume(take);
                    _remaining -= take;
                    if (_remaining > 0)
                        return false;
                    if (_phase == Phase.Body)
                        return Finish();
                    _phase = Phase.ChunkEnd;
                    break;
                }
                case Phase.ChunkEnd:
                {
                    if (available.Length < 2)
                        return false;
                    if (available[0] != '\r' || available[1] != '\n')
                        throw new InvalidDataException("a chunk is not followed by CRLF.");
                    Consume(2);
                    _phase = Phase.ChunkSize;
                    break;
                }
                case Phase.ChunkSize:
                {
                    var end = available.IndexOf(CrLf);
                    if (end < 0)
                    {
                        if (available.Length >= MaxChunkLineBytes)
                            throw new InvalidDataException("a chunk-size line is too long.");
                        return false;
                    }
                    var line = available[..end];
                    var extension = line.IndexOf((byte)';');
                    var size = ParseChunkSize(extension < 0 ? line : line[..extension]);
                    Consume(end + 2);
                    if (size == 0)
                    {
                        _phase = Phase.Trailers;
                    }
                    else
                    {
                        _remaining = size;
                        _phase = Phase.ChunkData;
                    }
                    break;
                }
                case Phase.Trailers:
                {
                    var end = available.IndexOf(CrLf);
                    if (end < 0)
                    {
                        if (available.Length >= MaxHeaderBytes)
                            throw new InvalidDataException("a trailer line is too long.");
                        return false;
                    }
                    Consume(end + 2);
                    if (end == 0)
                        return Finish();
                    break;
                }
            }
        }
    }

    private void Consume(int count)
    {
        _start += count;
        _bytesIn += count;
    }

    private bool Finish()
    {
        _phase = Phase.Headers;
        return true;
    }

    private enum Framing { ContentLength, Chunked }

    // Sets _status, _remaining and _closeAfter; starts the byte count of this response.
    private Framing ParseHeaders(ReadOnlySpan<byte> block)
    {
        _bytesIn = 0;
        _closeAfter = false;

        var lineEnd = block.IndexOf(CrLf);
        var statusLine = lineEnd < 0 ? block : block[..lineEnd];
        // "HTTP/1.x SSS", reason phrase optional.
        if (statusLine.Length < 12 || statusLine.StartsWith("HTTP/1."u8) == false || statusLine[8] != ' '
            || Utf8Parser.TryParse(statusLine.Slice(9, 3), out int status, out var consumed) == false || consumed != 3
            || (statusLine.Length > 12 && statusLine[12] != ' '))
            throw new InvalidDataException("the status line is not HTTP/1.x.");
        _status = status;
        if (status is >= 100 and < 200)
            throw new InvalidDataException($"interim response {status} is not supported.");

        long? contentLength = null;
        var chunked = false;
        var rest = lineEnd < 0 ? ReadOnlySpan<byte>.Empty : block[(lineEnd + 2)..];
        while (rest.Length > 0)
        {
            var end = rest.IndexOf(CrLf);
            var line = end < 0 ? rest : rest[..end];
            rest = end < 0 ? ReadOnlySpan<byte>.Empty : rest[(end + 2)..];

            var colon = line.IndexOf((byte)':');
            if (colon <= 0 || line[0] is (byte)' ' or (byte)'\t')
                throw new InvalidDataException("a header line is malformed.");
            var name = line[..colon];
            var value = line[(colon + 1)..].Trim(" \t"u8);

            if (Ascii.EqualsIgnoreCase(name, "Content-Length"u8))
            {
                if (Utf8Parser.TryParse(value, out long length, out var used) == false || used != value.Length || length < 0)
                    throw new InvalidDataException("Content-Length is not a non-negative integer.");
                if (contentLength.HasValue && contentLength.Value != length)
                    throw new InvalidDataException("the response carries conflicting Content-Length headers.");
                contentLength = length;
            }
            else if (Ascii.EqualsIgnoreCase(name, "Transfer-Encoding"u8))
            {
                if (Ascii.EqualsIgnoreCase(value, "chunked"u8) == false)
                    throw new InvalidDataException("only the chunked transfer coding is supported.");
                chunked = true;
            }
            else if (Ascii.EqualsIgnoreCase(name, "Connection"u8))
            {
                _closeAfter |= ContainsToken(value, "close"u8);
            }
        }

        if (chunked && contentLength.HasValue)
            throw new InvalidDataException("the response carries both Content-Length and Transfer-Encoding.");
        if (chunked)
            return Framing.Chunked;
        if (contentLength.HasValue == false && _status is not (204 or 304))
            throw new InvalidDataException("the response has neither Content-Length nor chunked framing.");

        _remaining = contentLength ?? 0;
        return Framing.ContentLength;
    }

    private static bool ContainsToken(ReadOnlySpan<byte> value, ReadOnlySpan<byte> token)
    {
        foreach (var range in value.Split((byte)','))
        {
            if (Ascii.EqualsIgnoreCase(value[range].Trim(" \t"u8), token))
                return true;
        }
        return false;
    }

    private static long ParseChunkSize(ReadOnlySpan<byte> hex)
    {
        hex = hex.Trim(" \t"u8);
        // 15 hex digits cannot overflow a long.
        if (hex.Length is 0 or > 15)
            throw new InvalidDataException("a chunk size is not a valid hexadecimal number.");
        long size = 0;
        foreach (var b in hex)
        {
            var digit = HexValue(b);
            if (digit < 0)
                throw new InvalidDataException("a chunk size is not a valid hexadecimal number.");
            size = size * 16 + digit;
        }
        return size;
    }

    private static int HexValue(byte b) => b switch
    {
        >= (byte)'0' and <= (byte)'9' => b - '0',
        >= (byte)'a' and <= (byte)'f' => b - 'a' + 10,
        >= (byte)'A' and <= (byte)'F' => b - 'A' + 10,
        _ => -1
    };

    /// <summary>Fails the connection when its oldest outstanding request has waited past <paramref name="timeoutTicks"/>.</summary>
    public void FailIfStalled(long nowTicks, long timeoutTicks, string failure)
    {
        lock (_gate)
        {
            if (_dead || _inFlight.Count == 0 || nowTicks - _headSince < timeoutTicks)
                return;
            // Some operations bound only the wait for the response headers, as the HttpClient path does.
            if (_headHeadersRead && _inFlight.Peek().TimeoutCoversBody == false)
                return;
        }
        Fail(failure);
    }

    /// <summary>Closes the connection and fails every outstanding request with <paramref name="failure"/>. Idempotent.</summary>
    public void Fail(string failure)
    {
        Http1Exchange[] outstanding;
        lock (_gate)
        {
            if (_dead)
                return;
            _dead = true;
            outstanding = _inFlight.ToArray();
            _inFlight.Clear();
        }

        _socket.Dispose();
        var response = Http1Response.Failed(failure);
        foreach (var exchange in outstanding)
            exchange.Complete(response);
    }
}

/// <summary>
/// Connections to one origin, each offering <c>depth</c> request slots. A caller rents a slot, sends one
/// request and returns the slot, so a connection never carries more than <c>depth</c> outstanding requests.
/// A new connection opens only when no live connection has a free slot.
/// </summary>
internal sealed class Http1ConnectionPool : IDisposable
{
    private static readonly TimeSpan ResponseTimeout = TimeSpan.FromSeconds(30);

    private readonly string _host;
    private readonly int _port;
    private readonly byte[] _fixedHeaders;
    private readonly int _depth;
    // A bag hands a thread the slot it returned last, so a caller completed by a connection's receive loop sends its next request on that same connection, where it joins the loop's next batch.
    private readonly ConcurrentBag<Http1Connection> _free = new();
    private readonly List<Http1Connection> _all = new();
    private readonly Timer _stallCheck;
    private int _opened;

    public Http1ConnectionPool(string host, int port, byte[] fixedHeaders, int depth)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(depth, 1);
        _host = host;
        _port = port;
        _fixedHeaders = fixedHeaders;
        _depth = depth;
        _stallCheck = new Timer(_ => FailStalled(), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
    }

    /// <summary>Rents a slot; the caller must await <see cref="Http1Connection.Ready"/> and must return the slot.</summary>
    public Http1Connection Rent()
    {
        while (_free.TryTake(out var connection))
        {
            if (connection.IsDead == false)
                return connection;
        }

        var opened = new Http1Connection(_host, _port, _fixedHeaders, ResponseTimeout);
        lock (_all)
        {
            _all.RemoveAll(c => c.IsDead);
            _all.Add(opened);
            _opened++;
        }
        for (var i = 1; i < _depth; i++)
            _free.Add(opened);
        return opened;
    }

    /// <summary>Connections opened over the pool's lifetime.</summary>
    public int OpenedConnections
    {
        get
        {
            lock (_all)
                return _opened;
        }
    }

    public void Return(Http1Connection connection)
    {
        if (connection.IsDead == false)
            _free.Add(connection);
    }

    private void FailStalled()
    {
        Http1Connection[] snapshot;
        lock (_all)
            snapshot = _all.ToArray();

        var now = Stopwatch.GetTimestamp();
        var timeoutTicks = (long)(ResponseTimeout.TotalSeconds * Stopwatch.Frequency);
        foreach (var connection in snapshot)
            connection.FailIfStalled(now, timeoutTicks, $"Operation timed out after {ResponseTimeout.TotalSeconds:F0} seconds");
    }

    public void Dispose()
    {
        _stallCheck.Dispose();
        lock (_all)
        {
            foreach (var connection in _all)
                connection.Fail("The transport was disposed.");
            _all.Clear();
        }
    }
}
