using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using RavenBench.Core.Metrics.Snmp;
using System.Text.Json;
using System.Buffers;
using RavenBench.Core.Workload;
using RavenBench.Core.Metrics;
using RavenBench.Core;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Aggregate;
using ZstdSharp;

namespace RavenBench.Core.Transport;

public sealed class RawHttpTransport : ITransport, IReportsStorageSize, IInspectsStoredDocuments
{
    private const int BufferSize = 32 * 1024;
    private const int PoolCount = 1024;
    private const string JsonContentType = "application/json; charset=utf-8";
    private const string OctetStreamContentType = "application/octet-stream";

    /// <summary>The <see cref="TransportPath"/> of a run served by raw HTTP/1.1 socket connections.</summary>
    public const string SocketPath = "raw-socket";

    /// <summary>The <see cref="TransportPath"/> of a run served by HttpClient.</summary>
    public const string HttpClientPath = "http-client";

    // ZstdSharp's Compressor is not thread-safe; one per worker thread, reused across requests.
    // Default zstd level — add a level knob here if a run ever needs to vary it.
    private static readonly ThreadLocal<Compressor> ZstdCompressor = new(() => new Compressor());

    // Request bodies are built here, then copied out before the building thread awaits.
    [ThreadStatic] private static ArrayBufferWriter<byte>? t_body;
    [ThreadStatic] private static Utf8JsonWriter? t_json;

    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string _origin;
    private readonly string _pathBase;
    private readonly string _dbPath;
    private readonly string _queriesTarget;
    private readonly string _streamsTarget;
    private readonly string _bulkDocsTarget;
    private readonly string _db;
    private readonly CompressionMode _compression;
    private readonly string _acceptEncoding;
    private readonly string? _customEndpoint; // optional format with {id}
    private readonly LockFreeRingBuffer<byte[]> _bufferPool;
    private readonly Version _httpVersion;
    private readonly TransportAdminClient _admin;
    private readonly Http1ConnectionPool? _socketPool;
    public string EffectiveCompressionMode => _acceptEncoding;
    public string EffectiveHttpVersion => HttpHelper.FormatHttpVersion(_httpVersion);

    public const string RavenDbProductName = "RavenDB";

    public string ProductName => RavenDbProductName;

    /// <summary>The URL with any password replaced by a token, safe to record or print.</summary>
    public string RecordedEndpoint { get; }

    /// <inheritdoc />
    public string TransportPath => _socketPool != null ? SocketPath : HttpClientPath;

    /// <summary>Socket connections opened so far; zero on the HttpClient path.</summary>
    internal int OpenedSocketConnections => _socketPool?.OpenedConnections ?? 0;

    /// <summary>Requests one connection carries before it reads their responses; 1 sends the next request only after the previous response.</summary>
    public int PipelineDepth { get; }

    // Wire-accurate only without transparent decompression; gzip/brotli/deflate are measured post-inflate.
    public bool ReportsWireBytes => _compression is CompressionMode.Identity or CompressionMode.Zstd;

    /// <summary>A vector search returns only each hit's id and metadata, not the stored document.</summary>
    public bool VectorSearchIdsOnly { get; init; }

    /// <param name="pipelineDepth">
    /// Requests per connection in flight at once. Above 1 it needs the socket path, which serves plain
    /// http with identity compression over HTTP/1.1; any other mode throws <see cref="ArgumentException"/>.
    /// </param>
    public RawHttpTransport(string url, string database, CompressionMode compression, Version httpVersion, string? endpoint = null, int pipelineDepth = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pipelineDepth, 1);

        _db = database;
        _baseUrl = url.TrimEnd('/');
        _compression = compression;
        _acceptEncoding = compression.ToWireFormat();
        _customEndpoint = string.IsNullOrWhiteSpace(endpoint) ? null : endpoint;
        _httpVersion = httpVersion;
        RecordedEndpoint = ConnectionStringRedaction.Redact(_baseUrl);
        PipelineDepth = pipelineDepth;

        var uri = new Uri(_baseUrl, UriKind.Absolute);
        _origin = uri.GetLeftPart(UriPartial.Authority);
        _pathBase = uri.AbsolutePath.TrimEnd('/');
        _dbPath = $"{_pathBase}/databases/{Uri.EscapeDataString(database)}";
        _queriesTarget = $"{_dbPath}/queries";
        _streamsTarget = $"{_dbPath}/streams/queries";
        _bulkDocsTarget = $"{_dbPath}/bulk_docs";

        // The socket path speaks only plain-text HTTP/1.1 with identity bodies; every other mode stays on HttpClient.
        if (uri.Scheme == Uri.UriSchemeHttp && compression == CompressionMode.Identity && httpVersion == HttpVersion.Version11)
        {
            var authority = uri.IsDefaultPort ? uri.IdnHost : $"{uri.IdnHost}:{uri.Port}";
            var fixedHeaders = Encoding.ASCII.GetBytes($"Host: {authority}\r\nAccept-Encoding: {_acceptEncoding}\r\n");
            _socketPool = new Http1ConnectionPool(uri.DnsSafeHost, uri.Port, fixedHeaders, pipelineDepth);
        }
        else if (pipelineDepth > 1)
        {
            throw new ArgumentException(
                $"Pipeline depth {pipelineDepth} needs HTTP/1.1 over plain http with identity compression; this run uses {uri.Scheme}, HTTP/{EffectiveHttpVersion} and {_acceptEncoding}.",
                nameof(pipelineDepth));
        }

        // Zstd is decoded manually; DecompressionMethods has no zstd support.
        var decompression = _compression switch
        {
            CompressionMode.Gzip => DecompressionMethods.GZip,
            CompressionMode.Deflate => DecompressionMethods.Deflate,
            CompressionMode.Brotli => DecompressionMethods.Brotli,
            _ => DecompressionMethods.None
        };

        _http = HttpHelper.CreateVersionedHttpClient(_httpVersion, decompression);

        _admin = new TransportAdminClient(_http, _baseUrl);

        _bufferPool = new LockFreeRingBuffer<byte[]>(PoolCount);
        for (int i = 0; i < PoolCount; i++)
        {
            _bufferPool.TryEnqueue(new byte[BufferSize]);
        }
    }

    private static bool NeedsZstdDecode(HttpResponseMessage resp)
    {
        var enc = resp.Content.Headers.ContentEncoding;
        if (enc == null || enc.Count == 0) return false;
        foreach (var e in enc)
        {
            if (string.Equals(e, "zstd", StringComparison.OrdinalIgnoreCase)) return true;
        }
        return false;
    }

    // UTF-8 JSON request body, zstd-encoded when the run uses zstd (Content-Encoding: zstd).
    // Content-Length is the compressed size, so wire-byte accounting reports it as-is.
    private HttpContent CreateJsonContent(ReadOnlyMemory<byte> utf8Json)
    {
        if (_compression == CompressionMode.Zstd)
            return ZstdJsonContent(utf8Json.Span);
        var content = new ReadOnlyMemoryContent(utf8Json);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
        return content;
    }

    internal static HttpContent ZstdJsonContent(ReadOnlySpan<byte> utf8Json)
    {
        var content = new ByteArrayContent(ZstdCompressor.Value!.Wrap(utf8Json).ToArray());
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
        content.Headers.ContentEncoding.Add("zstd");
        return content;
    }

    public Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct) =>
        op is GroupedAggregateOperation aggregateOp ? QueryAggregateAsync(aggregateOp, ct)
        : _socketPool != null ? ExecuteOnSocketAsync(op, ct) : ExecuteOnHttpClientAsync(op, ct);

    /// <summary>
    /// Describes <paramref name="op"/> as the request both paths send. The body lives in this thread's
    /// scratch buffer, so the caller must copy it before its next await.
    /// </summary>
    internal RawRequest Describe(OperationBase op)
    {
        switch (op)
        {
            case ReadOperation readOp:
                return new RawRequest(HttpMethod.Get, DocumentTarget(readOp.Id), default, null, ReadsEnvelope: false, TimeoutCoversBody: false, IndexName: null);
            case InsertOperation<string> insertOp:
                return new RawRequest(HttpMethod.Put, DocumentTarget(insertOp.Id), Utf8(insertOp.Payload), JsonContentType, ReadsEnvelope: false, TimeoutCoversBody: false, IndexName: null);
            case UpdateFieldOperation updateOp:
            {
                // The script is fixed: the field name and the value travel as patch arguments, so the server sees one script for every update.
                var json = StartJson(out var body);
                StartPatch(json, "this[args.field] = args.value;");
                json.WriteString("field", updateOp.FieldName);
                json.WriteString("value", updateOp.Value);
                return PatchRequest(updateOp.Id, json, body);
            }
            case AggregateUpdateOperation aggregateUpdateOp:
            {
                var json = StartJson(out var body);
                StartPatch(json, $"this.{AggregateDocument.CategoryField} = args.category; this.{AggregateDocument.AmountField} = args.amount;");
                json.WriteString("category", aggregateUpdateOp.Category);
                json.WriteNumber("amount", aggregateUpdateOp.Amount);
                return PatchRequest(aggregateUpdateOp.Id, json, body);
            }
            case DocumentPatchOperation patchOp:
            {
                var json = StartJson(out var body);
                StartPatch(json, patchOp.Script);
                return PatchRequest(patchOp.Id, json, body);
            }
            case StreamQueryOperation streamOp:
            {
                // Result counts and query stats are not parsed from the streamed body.
                var json = StartJson(out var body);
                WriteQuery(json, streamOp);
                json.WriteEndObject();
                json.Flush();
                return new RawRequest(HttpMethod.Post, _streamsTarget, body.WrittenMemory, JsonContentType, ReadsEnvelope: false, TimeoutCoversBody: true, streamOp.ExpectedIndex);
            }
            case QueryOperation queryOp:
            {
                var json = StartJson(out var body);
                WriteQuery(json, queryOp);
                json.WriteBoolean("MetadataOnly", false);
                json.WriteEndObject();
                json.Flush();
                return new RawRequest(HttpMethod.Post, _queriesTarget, body.WrittenMemory, JsonContentType, ReadsEnvelope: true, TimeoutCoversBody: false, IndexName: null);
            }
            case AttachmentOperation attachmentOp:
            {
                var method = attachmentOp.Kind switch
                {
                    AttachmentOperationKind.Put => HttpMethod.Put,
                    AttachmentOperationKind.Get => HttpMethod.Get,
                    AttachmentOperationKind.Delete => HttpMethod.Delete,
                    _ => throw new ArgumentOutOfRangeException(nameof(op), attachmentOp.Kind, null)
                };
                var target = $"{_dbPath}/attachments?id={Uri.EscapeDataString(attachmentOp.DocumentId)}&name={Uri.EscapeDataString(attachmentOp.Name)}";
                return attachmentOp.Kind == AttachmentOperationKind.Put
                    ? new RawRequest(method, target, attachmentOp.Payload!, OctetStreamContentType, ReadsEnvelope: false, TimeoutCoversBody: true, IndexName: null)
                    : new RawRequest(method, target, default, null, ReadsEnvelope: false, TimeoutCoversBody: true, IndexName: null);
            }
            case BulkInsertOperation<string> bulkOp:
            {
                var json = StartJson(out var body);
                json.WriteStartObject();
                json.WritePropertyName("Commands");
                json.WriteStartArray();
                foreach (var doc in bulkOp.Documents)
                {
                    json.WriteStartObject();
                    json.WriteString("Id", doc.Id);
                    json.WriteString("Type", "PUT");
                    json.WritePropertyName("Document");
                    json.WriteRawValue(doc.Document);
                    json.WriteEndObject();
                }
                json.WriteEndArray();
                json.WriteEndObject();
                json.Flush();
                return new RawRequest(HttpMethod.Post, _bulkDocsTarget, body.WrittenMemory, JsonContentType, ReadsEnvelope: false, TimeoutCoversBody: false, IndexName: null);
            }
            case BulkInsertOperation<AggregateDocument> aggregateBulkOp:
            {
                var json = StartJson(out var body);
                json.WriteStartObject();
                json.WritePropertyName("Commands");
                json.WriteStartArray();
                foreach (var doc in aggregateBulkOp.Documents)
                {
                    json.WriteStartObject();
                    json.WriteString("Id", doc.Id);
                    json.WriteString("Type", "PUT");
                    json.WritePropertyName("Document");
                    json.WriteRawValue(ToRavenJson(doc.Document));
                    json.WriteEndObject();
                }
                json.WriteEndArray();
                json.WriteEndObject();
                json.Flush();
                return new RawRequest(HttpMethod.Post, _bulkDocsTarget, body.WrittenMemory, JsonContentType, ReadsEnvelope: false, TimeoutCoversBody: false, IndexName: null);
            }
            case VectorSearchOperation vectorOp:
            {
                var json = StartJson(out var body);
                json.WriteStartObject();
                json.WriteString("Query", vectorOp.ToRqlQuery(VectorSearchIdsOnly));
                json.WritePropertyName("QueryParameters");
                json.WriteStartObject();
                json.WritePropertyName("vector");
                json.WriteStartArray();
                foreach (var value in vectorOp.QueryVector)
                    json.WriteNumberValue(value);
                json.WriteEndArray();
                if (vectorOp.Effort != null)
                    json.WriteNumber(vectorOp.RavenDbEffortParameter(), vectorOp.Effort.WholeValue);
                if (vectorOp.Filter != null)
                    json.WriteString("filterValue", vectorOp.Filter.Value);
                if (vectorOp.MinimumSimilarity > 0)
                    json.WriteNumber("minSimilarity", vectorOp.MinimumSimilarity);
                json.WriteEndObject();
                json.WriteBoolean("MetadataOnly", false);
                json.WriteNumber("PageSize", vectorOp.TopK);
                json.WriteEndObject();
                json.Flush();
                return new RawRequest(HttpMethod.Post, _queriesTarget, body.WrittenMemory, JsonContentType, ReadsEnvelope: true, TimeoutCoversBody: false, IndexName: null, ReadsIds: true);
            }
            default:
                throw new NotSupportedException($"{nameof(RawHttpTransport)} cannot execute operation type {op.GetType().Name}.");
        }
    }

    private static Utf8JsonWriter StartJson(out ArrayBufferWriter<byte> body)
    {
        body = t_body ??= new ArrayBufferWriter<byte>(BufferSize);
        body.ResetWrittenCount();
        var json = t_json ??= new Utf8JsonWriter(body);
        json.Reset(body);
        return json;
    }

    private static ReadOnlyMemory<byte> Utf8(string text)
    {
        var body = t_body ??= new ArrayBufferWriter<byte>(BufferSize);
        body.ResetWrittenCount();
        body.Advance(Encoding.UTF8.GetBytes(text, body.GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length))));
        return body.WrittenMemory;
    }

    private static void WriteQuery(Utf8JsonWriter json, QueryOperation queryOp)
    {
        json.WriteStartObject();
        json.WriteString("Query", queryOp.QueryText);
        json.WritePropertyName("QueryParameters");
        json.WriteStartObject();
        foreach (var (name, value) in queryOp.Parameters)
        {
            json.WritePropertyName(name);
            // Strings are the common case; every other value takes the serializer's own converter, so the bytes match a serializer-written body.
            if (value is string text)
                json.WriteStringValue(text);
            else
                JsonSerializer.Serialize(json, value);
        }
        json.WriteEndObject();
    }

    // Opens {"Patch":{"Script":...,"Values":{ and leaves the Values object open for the arguments.
    private static void StartPatch(Utf8JsonWriter json, string script)
    {
        json.WriteStartObject();
        json.WritePropertyName("Patch");
        json.WriteStartObject();
        json.WriteString("Script", script);
        json.WritePropertyName("Values");
        json.WriteStartObject();
    }

    private RawRequest PatchRequest(string id, Utf8JsonWriter json, ArrayBufferWriter<byte> body)
    {
        json.WriteEndObject();
        json.WriteEndObject();
        json.WriteEndObject();
        json.Flush();
        return new RawRequest(HttpMethod.Patch, $"{_dbPath}/docs?id={Uri.EscapeDataString(id)}", body.WrittenMemory, JsonContentType, ReadsEnvelope: false, TimeoutCoversBody: true, IndexName: null);
    }

    private async Task<TransportResult> ExecuteOnSocketAsync(OperationBase op, CancellationToken ct)
    {
        var connection = _socketPool!.Rent();
        try
        {
            if (connection.Ready.IsCompletedSuccessfully == false)
            {
                // A request that cannot be built fails as itself, not as whatever the connect reports; the body is rebuilt after the await.
                Describe(op);
                await connection.Ready.ConfigureAwait(false);
            }
            var request = Describe(op);
            var exchange = connection.Send(request);

            Http1Response response;
            if (ct.CanBeCanceled)
            {
                using var registration = ct.UnsafeRegister(static state => ((Http1Exchange)state!).Complete(Http1Response.CancelledResponse), exchange);
                response = await exchange.Response.ConfigureAwait(false);
            }
            else
            {
                response = await exchange.Response.ConfigureAwait(false);
            }

            if (response.Cancelled)
            {
                // The response is still due on this connection, so it cannot carry another request within its depth.
                connection.Fail("A request on this connection was cancelled.");
                return TransportResult.CancelledResult;
            }
            if (response.Failure != null)
                return new TransportResult(0, 0, response.Failure);
            if (response.Status is < 200 or > 299)
                return new TransportResult(0, 0, $"HTTP {response.Status} {(HttpStatusCode)response.Status}: {response.ErrorBody}") { NotFound = response.Status == (int)HttpStatusCode.NotFound };

            var envelope = response.Envelope;
            return new TransportResult(exchange.BytesOut, response.BytesIn,
                indexName: envelope.IndexName ?? request.IndexName, resultCount: envelope.ResultCount, isStale: envelope.IsStale)
            {
                NeighborIds = envelope.Ids
            };
        }
        catch (Exception ex)
        {
            // A connect failure or a request that cannot be built is a failed result, never an escape onto the hot path.
            return TransportResult.FromException(ex, ct);
        }
        finally
        {
            _socketPool.Return(connection);
        }
    }

    /// <summary>
    /// Calculates the size of HTTP headers in bytes for accurate network utilization measurement.
    /// Includes request line, all headers, and the blank line separator.
    /// </summary>
    private static long CalculateHeaderSize(HttpRequestMessage req)
    {
        // Request line: "POST /path HTTP/1.1\r\n"
        string requestLine = $"{req.Method} {req.RequestUri!.PathAndQuery} HTTP/{req.Version}\r\n";
        long size = Encoding.UTF8.GetByteCount(requestLine);

        size += SumHeaderBytes(req.Headers);
        if (req.Content != null)
            size += SumHeaderBytes(req.Content.Headers);

        // Blank line after headers
        size += Encoding.UTF8.GetByteCount("\r\n");

        return size;
    }

    private static long SumHeaderBytes(IEnumerable<KeyValuePair<string, IEnumerable<string>>> headers)
    {
        long size = 0;
        foreach (var header in headers)
            size += Encoding.UTF8.GetByteCount($"{header.Key}: {string.Join(", ", header.Value)}\r\n");
        return size;
    }

    /// <summary>
    /// Reads the response body to completion and returns the number of bytes read.
    /// The body must always be drained so latency covers the full transfer, not time-to-first-byte.
    /// </summary>
    private async Task<long> DrainResponseAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

        if (_bufferPool.TryDequeue(out var buffer) == false)
            buffer = new byte[BufferSize];

        try
        {
            long total = 0;
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
            {
                total += bytesRead;
            }
            return total;
        }
        finally
        {
            _bufferPool.TryEnqueue(buffer);
        }
    }

    /// <summary>
    /// Reads the response body to completion into a seekable buffer positioned at 0.
    /// </summary>
    private async Task<MemoryStream> BufferResponseAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

        if (_bufferPool.TryDequeue(out var buffer) == false)
            buffer = new byte[BufferSize];

        try
        {
            var ms = new MemoryStream();
            int bytesRead;
            while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
            {
                ms.Write(buffer, 0, bytesRead);
            }
            ms.Position = 0;
            return ms;
        }
        finally
        {
            _bufferPool.TryEnqueue(buffer);
        }
    }

    public async Task PutAsync<T>(string id, T document)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var json = document is string s ? s : JsonSerializer.Serialize(document);
        var result = await ExecuteAsync(new InsertOperation<string> { Id = id, Payload = json }, cts.Token).ConfigureAwait(false);
        if (result.IsSuccess == false)
            throw new InvalidOperationException($"Writing document '{id}' failed: {result.ErrorDetails ?? "cancelled"}");
    }

    private HttpRequestMessage NewRequest(HttpMethod method, string url)
    {
        var req = new HttpRequestMessage(method, url);
        req.Headers.AcceptEncoding.Clear();
        req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
        req.Headers.ExpectContinue = false;
        return req;
    }

    // Guards a whole operation, request construction included, so a serialization throw is reported
    // as a failed result instead of escaping onto the hot path.
    private async Task<TransportResult> ExecuteOnHttpClientAsync(OperationBase op, CancellationToken ct)
    {
        try
        {
            var request = Describe(op);
            var body = request.Body.ToArray();
            using var req = NewRequest(request.Method, _origin + request.Target);
            if (request.ContentType == JsonContentType)
                req.Content = CreateJsonContent(body);
            else if (request.ContentType != null)
            {
                req.Content = new ReadOnlyMemoryContent(body);
                req.Content.Headers.ContentType = new MediaTypeHeaderValue(request.ContentType);
            }

            var readDeadline = request.TimeoutCoversBody ? ResponseReadDeadline.Capped : ResponseReadDeadline.Uncapped;
            return await SendCoreAsync(req, ct, readDeadline, async (resp, readCt) =>
            {
                long bytesOut = CalculateHeaderSize(req) + (req.Content?.Headers.ContentLength ?? body.Length);
                if (request.ReadsEnvelope == false)
                {
                    long drained = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);
                    // If gzip auto-decompressed, Content-Length is the wire size; drained is post-inflate.
                    long bytesIn = resp.Content.Headers.ContentLength ?? drained;
                    return new TransportResult(bytesOut, bytesIn, indexName: request.IndexName);
                }

                // Buffered bytes are post-AutomaticDecompression for gzip/deflate/brotli; raw for zstd/identity.
                using var wireMs = await BufferResponseAsync(resp, readCt).ConfigureAwait(false);
                var wire = wireMs.GetBuffer().AsSpan(0, (int)wireMs.Length);
                // HttpClient does not decode zstd, so the body is decoded here before it is read.
                QueryEnvelope envelope;
                if (NeedsZstdDecode(resp))
                {
                    using var decompressor = new Decompressor();
                    envelope = QueryEnvelope.Read(decompressor.Unwrap(wire), request.ReadsIds);
                }
                else
                {
                    envelope = QueryEnvelope.Read(wire, request.ReadsIds);
                }
                return new TransportResult(bytesOut, wireMs.Length, indexName: envelope.IndexName, resultCount: envelope.ResultCount, isStale: envelope.IsStale)
                {
                    NeighborIds = envelope.Ids
                };
            }).ConfigureAwait(false);
        }
        catch (TaskCanceledException)
        {
            if (ct.IsCancellationRequested)
                return TransportResult.CancelledResult;
            return new TransportResult(0, 0, "Operation timed out after 30 seconds");
        }
        catch (Exception ex)
        {
            return new TransportResult(0, 0, $"Exception: {ex.GetType().Name}: {ex.Message}");
        }
    }

    // Whether the response-body read stays under the 30s send cap or runs under the caller's own token.
    private enum ResponseReadDeadline
    {
        Uncapped,
        Capped
    }

    // The 30s cap starts here, after the caller has built the request, so it bounds the round trip
    // and not the payload serialization.
    private async Task<TransportResult> SendCoreAsync(HttpRequestMessage req, CancellationToken ct, ResponseReadDeadline readDeadline,
        Func<HttpResponseMessage, CancellationToken, Task<TransportResult>> handleResponse)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));

        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
        if (resp.IsSuccessStatusCode == false)
        {
            var errorContent = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            return new TransportResult(0, 0, $"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {errorContent}") { NotFound = resp.StatusCode == HttpStatusCode.NotFound };
        }

        var readToken = readDeadline == ResponseReadDeadline.Capped ? cts.Token : ct;
        return await handleResponse(resp, readToken).ConfigureAwait(false);
    }

    public async Task<int?> GetServerMaxCoresAsync()
    {
        try
        {
            var url = $"{_baseUrl}/license/status";
            using var resp = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);
            if (doc.RootElement.TryGetProperty("MaxCores", out var cores) && cores.TryGetInt32(out var c))
                return c;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed: Unable to retrieve server max cores. {ex.Message}", ex);
        }
        return null;
    }


    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        return await RavenServerMetricsCollector.CollectAsync(_baseUrl, _db, HttpHelper.FormatHttpVersion(_httpVersion));
    }

    public Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null)
    {
        return _admin.GetSnmpMetricsAsync(snmpOptions, databaseName);
    }

    public async Task<string> GetServerVersionAsync()
    {
        try
        {
            var url = $"{_baseUrl}/build/version";
            using var resp = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            if (doc.RootElement.TryGetProperty("FullVersion", out var fullVersion))
                return fullVersion.GetString() ?? "unknown";
            if (doc.RootElement.TryGetProperty("ProductVersion", out var productVersion))
                return productVersion.GetString() ?? "unknown";

            return "unknown";
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed: Unable to retrieve server version. {ex.Message}", ex);
        }
    }

    public Task<string> GetServerLicenseTypeAsync()
    {
        return _admin.GetServerLicenseTypeAsync();
    }


    public async Task ValidateClientAsync()
    {
        try
        {
            var url = $"{_baseUrl}/build/version";
            using var response = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                throw new HttpRequestException($"Server returned {response.StatusCode}: {response.ReasonPhrase}");
            }
        }
        catch (Exception ex) when (ex is InvalidOperationException == false)
        {
            throw new InvalidOperationException($"Client validation failed: Unable to connect to RavenDB server. {ex.Message}", ex);
        }
    }


    public async Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default)
    {
        return await CalibrationHelper.ExecuteCalibrationAsync(async cancellationToken =>
        {
            var url = _admin.BuildCalibrationUrl(endpoint);
            var req = new HttpRequestMessage(HttpMethod.Get, url);

            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;
            req.Headers.ConnectionClose = false;

            return await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }, ct, fallbackHttpVersion: _httpVersion).ConfigureAwait(false);
    }

    public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints()
    {
        return _admin.GetCalibrationEndpoints();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, string>?> ReadStoredFieldsAsync(string id, CancellationToken ct)
    {
        using var response = await _http.GetAsync(_origin + DocumentTarget(id), HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();

        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        using var body = await JsonDocument.ParseAsync(stream, cancellationToken: ct).ConfigureAwait(false);
        var results = body.RootElement.GetProperty("Results");
        if (results.GetArrayLength() == 0)
            return null;

        var stored = results[0];
        var fields = new Dictionary<string, string>(PayloadGenerator.FieldCount);
        for (int i = 0; i < PayloadGenerator.FieldCount; i++)
        {
            var name = PayloadGenerator.FieldName(i);
            if (stored.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String)
                fields[name] = value.GetString()!;
        }

        return fields;
    }

    /// <inheritdoc />
    public async Task DeleteStoredDocumentAsync(string id, CancellationToken ct)
    {
        using var response = await _http.DeleteAsync(_origin + DocumentTarget(id), ct).ConfigureAwait(false);
        response.EnsureSuccessStatusCode();
    }
    private string DocumentTarget(string id)
    {
        if (_customEndpoint is { } e)
        {
            var path = e.Replace("{id}", Uri.EscapeDataString(id));
            return path.StartsWith('/') ? _pathBase + path : _pathBase + "/" + path;
        }
        return $"{_dbPath}/docs?id={Uri.EscapeDataString(id)}";
    }

    /// <summary>
    /// Creates a DocumentStore configured with the same HTTP version settings as the transport.
    /// Used for administrative operations (database creation, document counting, etc.).
    /// </summary>
    private Raven.Client.Documents.DocumentStore CreateAdminStore(string databaseName) =>
        HttpHelper.Create(_baseUrl, databaseName, _httpVersion);

    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        using var adminStore = CreateAdminStore(databaseName);

        await TransportAdminClient.EnsureDatabaseExistsAsync(adminStore, databaseName);
    }

    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        using var adminStore = CreateAdminStore(_db);

        return await TransportAdminClient.GetDocumentCountAsync(adminStore, idPrefix).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public string StorageSizeMetricName => TransportAdminClient.StorageSizeMetricName;

    /// <inheritdoc />
    public async Task<long> GetStorageSizeBytesAsync()
    {
        using var adminStore = CreateAdminStore(_db);

        return await TransportAdminClient.GetStorageSizeBytesAsync(adminStore).ConfigureAwait(false);
    }

    /// <summary>The stored form of an aggregate document: the emitted fields unchanged, in the aggregate collection.</summary>
    internal static string ToRavenJson(AggregateDocument d) => JsonSerializer.Serialize(new Dictionary<string, object>
    {
        [AggregateDocument.CategoryField] = d.Category,
        [AggregateDocument.RegionField] = d.Region,
        [AggregateDocument.AmountField] = d.Amount,
        [AggregateDocument.TimestampField] = d.Timestamp,
        [AggregateDocument.PayloadField] = d.Payload,
        ["@metadata"] = new Dictionary<string, string> { ["@collection"] = AggregateDocument.RavenDbCollection }
    });

    /// <summary>
    /// Serves a grouped aggregate from its map-reduce index. The server orders the reduced value as
    /// a number and returns N + 1 groups. When the groups at N and N + 1 tie, one second query reads
    /// every group at or above the tied value and replaces the first answer, so every returned group
    /// comes from one server answer and the shared ordering, not the server collation, decides the cut.
    /// It takes the HttpClient path in every transport mode, since a tie needs a second query.
    /// </summary>
    private async Task<TransportResult> QueryAggregateAsync(GroupedAggregateOperation op, CancellationToken ct)
    {
        try
        {
            return await QueryAggregateCoreAsync(op, ct).ConfigureAwait(false);
        }
        catch (TaskCanceledException)
        {
            if (ct.IsCancellationRequested)
                return TransportResult.CancelledResult;
            return new TransportResult(0, 0, "Operation timed out after 30 seconds");
        }
        catch (Exception ex)
        {
            return new TransportResult(0, 0, $"Exception: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private async Task<TransportResult> QueryAggregateCoreAsync(GroupedAggregateOperation op, CancellationToken ct)
    {
        op.Validate();
        var parameters = new Dictionary<string, object?>();
        var where = new List<string>(2);
        switch (op.Filter)
        {
            case null:
                break;
            case EqualityFilter eq:
                parameters["f"] = eq.Value;
                where.Add($"{eq.Field} = $f");
                break;
            case RangeFilter range:
                parameters["lo"] = range.Lower;
                parameters["hi"] = range.Upper;
                where.Add($"{range.Field} >= $lo and {range.Field} < $hi");
                break;
            default:
                throw new NotSupportedException($"{nameof(RawHttpTransport)} cannot filter by {op.Filter.GetType().Name}.");
        }
        var from = $"from index '{op.IndexName}'";
        string Rql(string tail) => $"{from}{(where.Count == 0 ? "" : " where " + string.Join(" and ", where))} order by value as long desc{tail}";

        parameters["n"] = (long)op.TopN + 1;
        var answer = await PostAggregateQueryAsync(Rql(" limit $n"), parameters, op.GroupBy, ct).ConfigureAwait(false);
        bool stale = answer.IsStale;
        long bytesOut = answer.BytesOut, bytesIn = answer.BytesIn;
        var groups = answer.Groups;
        if (groups.Count > op.TopN && groups[op.TopN - 1].Value == groups[op.TopN].Value)
        {
            parameters.Remove("n");
            parameters["v"] = groups[op.TopN - 1].Value;
            where.Add("value >= $v");
            answer = await PostAggregateQueryAsync(Rql(""), parameters, op.GroupBy, ct).ConfigureAwait(false);
            stale |= answer.IsStale;
            bytesOut += answer.BytesOut;
            bytesIn += answer.BytesIn;
        }

        var ordered = AggregateOrdering.Top(answer.Groups, op.TopN);
        return new TransportResult(bytesOut, bytesIn, indexName: answer.IndexName, resultCount: ordered.Count, isStale: stale) { Groups = ordered };
    }

    private readonly record struct AggregateQueryResponse(IReadOnlyList<AggregateGroup> Groups, bool IsStale, string? IndexName, long BytesOut, long BytesIn);

    private async Task<AggregateQueryResponse> PostAggregateQueryAsync(string rql, Dictionary<string, object?> parameters, string groupField, CancellationToken ct)
    {
        using var req = NewRequest(HttpMethod.Post, $"{_baseUrl}/databases/{_db}/queries");
        var body = JsonSerializer.Serialize(new { Query = rql, QueryParameters = parameters });
        var bodyBytes = Encoding.UTF8.GetBytes(body);
        req.Content = CreateJsonContent(bodyBytes);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
        if (resp.IsSuccessStatusCode == false)
            throw new HttpRequestException($"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false)}", null, resp.StatusCode);

        using var wireMs = await BufferResponseAsync(resp, cts.Token).ConfigureAwait(false);
        long bytesIn = wireMs.Length;
        ReadOnlyMemory<byte> json = wireMs.GetBuffer().AsMemory(0, (int)wireMs.Length);
        // HttpClient does not decode zstd, so the body is decoded here before it is read.
        if (NeedsZstdDecode(resp))
        {
            using var decompressor = new Decompressor();
            json = decompressor.Unwrap(json.Span).ToArray();
        }
        var envelope = QueryEnvelope.Read(json.Span);
        using var doc = JsonDocument.Parse(json);
        if (envelope.IsStale is not { } isStale)
            throw new InvalidDataException("The aggregate query response carries no IsStale flag.");

        var groups = new List<AggregateGroup>();
        foreach (var row in doc.RootElement.GetProperty("Results").EnumerateArray())
        {
            var key = row.GetProperty(groupField).GetString() ?? throw new InvalidDataException($"An aggregate row carries a null '{groupField}'.");
            groups.Add(new AggregateGroup(key, row.GetProperty("value").GetInt64()));
        }
        return new AggregateQueryResponse(groups, isStale, envelope.IndexName, CalculateHeaderSize(req) + (req.Content.Headers.ContentLength ?? bodyBytes.Length), bytesIn);
    }

    /// <summary>Creates the map-reduce index of every aggregate shape from the repository definitions; an existing identical index is kept.</summary>
    public async Task EnsureAggregateIndexesAsync(CancellationToken ct)
    {
        var body = JsonSerializer.Serialize(new { Indexes = AggregateShapes.RavenDbIndexes() });
        using var req = NewRequest(HttpMethod.Put, $"{_baseUrl}/databases/{_db}/admin/indexes");
        req.Content = new StringContent(body, Encoding.UTF8, "application/json");
        using var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
        if (resp.IsSuccessStatusCode == false)
            throw new HttpRequestException($"Creating the aggregate indexes on '{_db}' failed, HTTP {(int)resp.StatusCode}: {await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false)}", null, resp.StatusCode);
    }

    /// <summary>
    /// Polls the server index statistics until every aggregate index reports non-stale. Throws a
    /// <see cref="TimeoutException"/> that names the indexes still stale when the timeout passes.
    /// </summary>
    public async Task WaitForNonStaleAggregateIndexesAsync(TimeSpan timeout, CancellationToken ct)
    {
        var names = AggregateShapes.RavenDbIndexes().Select(i => i.GetProperty("Name").GetString()!).ToList();
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var stale = new List<string>();
            foreach (var name in names)
            {
                using var req = NewRequest(HttpMethod.Get, $"{_baseUrl}/databases/{_db}/indexes/stats?name={Uri.EscapeDataString(name)}");
                using var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
                if (resp.IsSuccessStatusCode == false)
                    throw new HttpRequestException($"Reading the statistics of index '{name}' on '{_db}' failed, HTTP {(int)resp.StatusCode}.", null, resp.StatusCode);
                using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false));
                if (doc.RootElement.GetProperty("Results")[0].GetProperty("IsStale").GetBoolean())
                    stale.Add(name);
            }
            if (stale.Count == 0)
                return;
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException($"The aggregate indexes {string.Join(", ", stale.Select(n => $"'{n}'"))} on '{_db}' were still stale after {timeout.TotalSeconds:0} s.");
            await Task.Delay(TimeSpan.FromMilliseconds(100), ct).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        _socketPool?.Dispose();
        _http.Dispose();
    }
}
