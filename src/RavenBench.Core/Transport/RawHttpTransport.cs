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
using ZstdSharp;

namespace RavenBench.Core.Transport;

public sealed class RawHttpTransport : ITransport
{
    private const int BufferSize = 32 * 1024;
    private const int PoolCount = 1024;

    // ZstdSharp's Compressor is not thread-safe; one per worker thread, reused across requests.
    // Default zstd level — add a level knob here if a run ever needs to vary it.
    private static readonly ThreadLocal<Compressor> ZstdCompressor = new(() => new Compressor());

    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string _db;
    private readonly CompressionMode _compression;
    private readonly string _acceptEncoding;
    private readonly string? _customEndpoint; // optional format with {id}
    private readonly LockFreeRingBuffer<byte[]> _bufferPool;
    private readonly Version _httpVersion;
    private readonly TransportAdminClient _admin;
    public string EffectiveCompressionMode => _acceptEncoding;
    public string EffectiveHttpVersion => HttpHelper.FormatHttpVersion(_httpVersion);

    public string ProductName => "RavenDB";

    // Wire-accurate only without transparent decompression; gzip/brotli/deflate are measured post-inflate.
    public bool ReportsWireBytes => _compression is CompressionMode.Identity or CompressionMode.Zstd;


    public RawHttpTransport(string url, string database, CompressionMode compression, Version httpVersion, string? endpoint = null)
    {
        _db = database;
        _baseUrl = url.TrimEnd('/');
        _compression = compression;
        _acceptEncoding = compression.ToWireFormat();
        _customEndpoint = string.IsNullOrWhiteSpace(endpoint) ? null : endpoint;
        _httpVersion = httpVersion;

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
    private HttpContent CreateJsonContent(string json)
        => _compression == CompressionMode.Zstd
            ? ZstdJsonContent(Encoding.UTF8.GetBytes(json))
            : new StringContent(json, Encoding.UTF8, "application/json");

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

    // Parses a JSON response, decoding zstd first. HttpClient does not auto-decode zstd, so any
    // response path that parses the body must route through here. wireMs holds the raw wire bytes
    // (already counted for bytesIn); identity passes straight through.
    private static async Task<JsonDocument> ParseJsonResponseAsync(MemoryStream wireMs, HttpResponseMessage resp, CancellationToken ct)
    {
        if (NeedsZstdDecode(resp) == false)
            return await JsonDocument.ParseAsync(wireMs, cancellationToken: ct).ConfigureAwait(false);

        await using var zstdStream = new DecompressionStream(wireMs);
        return await JsonDocument.ParseAsync(zstdStream, cancellationToken: ct).ConfigureAwait(false);
    }

    private readonly record struct QueryEnvelope(string? IndexName, int? ResultCount, bool? IsStale);

    // Metadata fields of a /queries response; each is absent for a query that does not report it.
    private static QueryEnvelope ParseQueryEnvelope(JsonDocument doc)
    {
        var indexName = doc.RootElement.TryGetProperty("IndexName", out var indexProp)
            ? indexProp.GetString()
            : null;

        var resultCount = doc.RootElement.TryGetProperty("Results", out var resultsProp) && resultsProp.ValueKind == JsonValueKind.Array
            ? resultsProp.GetArrayLength()
            : (int?)null;

        var isStale = doc.RootElement.TryGetProperty("IsStale", out var staleProp)
            ? staleProp.GetBoolean()
            : (bool?)null;

        return new QueryEnvelope(indexName, resultCount, isStale);
    }

    public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
    {
        try
        {
            switch (op)
            {
                case ReadOperation readOp:
                    return await GetAsync(readOp.Id, ct).ConfigureAwait(false);
                case InsertOperation<string> insertOp:
                    return await PutAsyncInternal(insertOp.Id, insertOp.Payload, ct).ConfigureAwait(false);
                case UpdateOperation<string> updateOp:
                    return await PutAsyncInternal(updateOp.Id, updateOp.Payload, ct).ConfigureAwait(false);
                case StreamQueryOperation streamOp:
                    return await PostStreamQueryAsync(streamOp, ct).ConfigureAwait(false);
                case QueryOperation queryOp:
                    return await PostQueryAsync(queryOp, ct).ConfigureAwait(false);
                case DocumentPatchOperation patchOp:
                    return await PatchDocumentAsync(patchOp, ct).ConfigureAwait(false);
                case AttachmentOperation attachmentOp:
                    return await ExecuteAttachmentAsync(attachmentOp, ct).ConfigureAwait(false);
                case BulkInsertOperation<string> bulkOp:
                    return await PostBulkDocsAsync(bulkOp.Documents, ct).ConfigureAwait(false);
                case VectorSearchOperation vectorOp:
                    return await PostVectorSearchAsync(vectorOp, ct).ConfigureAwait(false);
                default:
                    return new TransportResult(0, 0);
            }
        }
        catch (Exception ex)
        {
            return TransportResult.FromException(ex, ct);
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
        await PutAsyncInternal(id, document, cts.Token).ConfigureAwait(false);
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
    private async Task<TransportResult> SendAsync(CancellationToken ct, Func<Task<TransportResult>> body)
    {
        try
        {
            return await body().ConfigureAwait(false);
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
            return new TransportResult(0, 0, $"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {errorContent}");
        }

        var readToken = readDeadline == ResponseReadDeadline.Capped ? cts.Token : ct;
        return await handleResponse(resp, readToken).ConfigureAwait(false);
    }

    private Task<TransportResult> PostQueryAsync(QueryOperation queryOp, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = $"{_baseUrl}/databases/{_db}/queries";
        using var req = NewRequest(HttpMethod.Post, url);

        var payload = new
        {
            Query = queryOp.QueryText,
            QueryParameters = queryOp.Parameters,
            MetadataOnly = false
        };

        var queryPayload = JsonSerializer.Serialize(payload);
        req.Content = CreateJsonContent(queryPayload);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Uncapped, async (resp, readCt) =>
        {
            using var wireMs = await BufferResponseAsync(resp, readCt).ConfigureAwait(false);
            long bytesIn = wireMs.Length;

            using var doc = await ParseJsonResponseAsync(wireMs, resp, readCt).ConfigureAwait(false);
            var envelope = ParseQueryEnvelope(doc);

            long bodyBytes = req.Content?.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(queryPayload);
            long bytesOut = CalculateHeaderSize(req) + bodyBytes;

            return new TransportResult(bytesOut, bytesIn, indexName: envelope.IndexName, resultCount: envelope.ResultCount, isStale: envelope.IsStale);
        }).ConfigureAwait(false);
    });

    /// <summary>
    /// Executes a query through the streams endpoint and drains the full result stream.
    /// Result counts and query stats are not parsed from the streamed body.
    /// </summary>
    private Task<TransportResult> PostStreamQueryAsync(StreamQueryOperation streamOp, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = $"{_baseUrl}/databases/{_db}/streams/queries";
        using var req = NewRequest(HttpMethod.Post, url);

        var payload = new
        {
            Query = streamOp.QueryText,
            QueryParameters = streamOp.Parameters
        };

        var queryPayload = JsonSerializer.Serialize(payload);
        req.Content = CreateJsonContent(queryPayload);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Capped, async (resp, readCt) =>
        {
            long bytesIn = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);

            long bodyBytes = req.Content?.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(queryPayload);
            long bytesOut = CalculateHeaderSize(req) + bodyBytes;

            return new TransportResult(bytesOut, bytesIn, indexName: streamOp.ExpectedIndex);
        }).ConfigureAwait(false);
    });

    private Task<TransportResult> PatchDocumentAsync(DocumentPatchOperation patchOp, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = $"{_baseUrl}/databases/{_db}/docs?id={Uri.EscapeDataString(patchOp.Id)}";
        using var req = NewRequest(HttpMethod.Patch, url);

        var payload = new { Patch = new { Script = patchOp.Script, Values = new { } } };
        var jsonPayload = JsonSerializer.Serialize(payload);
        req.Content = CreateJsonContent(jsonPayload);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Capped, async (resp, readCt) =>
        {
            long drained = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);
            long bytesIn = resp.Content.Headers.ContentLength ?? drained;
            long bytesOut = CalculateHeaderSize(req) + (req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(jsonPayload));
            return new TransportResult(bytesOut, bytesIn);
        }).ConfigureAwait(false);
    });

    private Task<TransportResult> ExecuteAttachmentAsync(AttachmentOperation op, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = $"{_baseUrl}/databases/{_db}/attachments?id={Uri.EscapeDataString(op.DocumentId)}&name={Uri.EscapeDataString(op.Name)}";

        var method = op.Kind switch
        {
            AttachmentOperationKind.Put => HttpMethod.Put,
            AttachmentOperationKind.Get => HttpMethod.Get,
            AttachmentOperationKind.Delete => HttpMethod.Delete,
            _ => throw new ArgumentOutOfRangeException(nameof(op.Kind), op.Kind, null)
        };

        using var req = NewRequest(method, url);

        long bodyBytes = 0;
        if (op.Kind == AttachmentOperationKind.Put)
        {
            req.Content = new ReadOnlyMemoryContent(op.Payload!);
            req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            bodyBytes = op.Payload!.Length;
        }

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Capped, async (resp, readCt) =>
        {
            long drained = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);
            long bytesIn = resp.Content.Headers.ContentLength ?? drained;
            long bytesOut = CalculateHeaderSize(req) + bodyBytes;
            return new TransportResult(bytesOut, bytesIn);
        }).ConfigureAwait(false);
    });

    private Task<TransportResult> PostBulkDocsAsync(List<DocumentToWrite<string>> documents, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = $"{_baseUrl}/databases/{_db}/bulk_docs";
        using var req = NewRequest(HttpMethod.Post, url);

        var bufferWriter = new ArrayBufferWriter<byte>(BufferSize);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Commands");
            writer.WriteStartArray();
            foreach (var doc in documents)
            {
                writer.WriteStartObject();
                writer.WriteString("Id", doc.Id);
                writer.WriteString("Type", "PUT");
                writer.WritePropertyName("Document");
                writer.WriteRawValue(doc.Document);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
        int jsonByteCount = bufferWriter.WrittenCount;
        req.Content = CreateJsonContent(bufferWriter.WrittenMemory);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Uncapped, async (resp, readCt) =>
        {
            long drained = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);
            long bytesIn = resp.Content.Headers.ContentLength ?? drained;
            long bytesOut = CalculateHeaderSize(req) + (req.Content.Headers.ContentLength ?? jsonByteCount);
            return new TransportResult(bytesOut, bytesIn);
        }).ConfigureAwait(false);
    });

    private Task<TransportResult> GetAsync(string id, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = BuildUrl(id);
        using var req = NewRequest(HttpMethod.Get, url);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Uncapped, async (resp, readCt) =>
        {
            long drained = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);
            // If gzip auto-decompressed, Content-Length is the wire size; drained is post-inflate.
            long bytesIn = resp.Content.Headers.ContentLength ?? drained;
            long bytesOut = CalculateHeaderSize(req);
            return new TransportResult(bytesOut, bytesIn);
        }).ConfigureAwait(false);
    });

    private Task<TransportResult> PutAsyncInternal<T>(string id, T document, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = BuildUrl(id);
        using var req = NewRequest(HttpMethod.Put, url);

        string jsonPayload = document is string s ? s : JsonSerializer.Serialize(document);
        req.Content = CreateJsonContent(jsonPayload);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Uncapped, async (resp, readCt) =>
        {
            long drained = await DrainResponseAsync(resp, readCt).ConfigureAwait(false);
            long bytesIn = resp.Content.Headers.ContentLength ?? drained;
            long bytesOut = CalculateHeaderSize(req) + (req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(jsonPayload));
            return new TransportResult(bytesOut, bytesIn);
        }).ConfigureAwait(false);
    });

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

    private string BuildUrl(string id)
    {
        if (_customEndpoint is { } e)
        {
            var path = e.Replace("{id}", Uri.EscapeDataString(id));
            if (path.StartsWith("/"))
                return _baseUrl + path;
            return _baseUrl + "/" + path;
        }
        return $"{_baseUrl}/databases/{_db}/docs?id={Uri.EscapeDataString(id)}";
    }

    /// <summary>
    /// Creates a DocumentStore configured with the same HTTP version settings as the transport.
    /// Used for administrative operations (database creation, document counting, etc.).
    /// </summary>
    private Raven.Client.Documents.DocumentStore CreateAdminStore(string databaseName) =>
        HttpHelper.Create(_baseUrl, databaseName, _httpVersion);

    private Task<TransportResult> PostVectorSearchAsync(VectorSearchOperation vectorOp, CancellationToken ct) => SendAsync(ct, async () =>
    {
        var url = $"{_baseUrl}/databases/{_db}/queries";
        using var req = NewRequest(HttpMethod.Post, url);

        string queryText = vectorOp.ToRqlQuery();

        // Pre-sized for the cohere-768 worst case (~10KB); 16KB avoids a Grow.
        var bufferWriter = new ArrayBufferWriter<byte>(16384);
        using (var writer = new Utf8JsonWriter(bufferWriter))
        {
            writer.WriteStartObject();
            writer.WriteString("Query", queryText);
            writer.WritePropertyName("QueryParameters");
            writer.WriteStartObject();
            writer.WritePropertyName("vector");
            writer.WriteStartArray();
            var vec = vectorOp.QueryVector;
            for (int i = 0; i < vec.Length; i++)
                writer.WriteNumberValue(vec[i]);
            writer.WriteEndArray();
            if (vectorOp.EfSearch.HasValue)
                writer.WriteNumber("efSearch", vectorOp.EfSearch.Value);
            if (vectorOp.MinimumSimilarity > 0)
                writer.WriteNumber("minSimilarity", vectorOp.MinimumSimilarity);
            writer.WriteEndObject();
            writer.WriteBoolean("MetadataOnly", false);
            writer.WriteNumber("PageSize", vectorOp.TopK);
            writer.WriteEndObject();
        }
        var jsonBytes = bufferWriter.WrittenMemory;
        var jsonByteCount = jsonBytes.Length;
        req.Content = CreateJsonContent(jsonBytes);

        return await SendCoreAsync(req, ct, ResponseReadDeadline.Uncapped, async (resp, readCt) =>
        {
            // Buffered bytes are post-AutomaticDecompression for gzip/deflate/brotli; raw for zstd/identity.
            using var wireMs = await BufferResponseAsync(resp, readCt).ConfigureAwait(false);
            long bytesIn = wireMs.Length;

            using var doc = await ParseJsonResponseAsync(wireMs, resp, readCt).ConfigureAwait(false);
            var envelope = ParseQueryEnvelope(doc);

            long bodyBytes = req.Content?.Headers.ContentLength ?? jsonByteCount;
            long bytesOut = CalculateHeaderSize(req) + bodyBytes;

            return new TransportResult(bytesOut, bytesIn, indexName: envelope.IndexName, resultCount: envelope.ResultCount, isStale: envelope.IsStale);
        }).ConfigureAwait(false);
    });

    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        using var adminStore = CreateAdminStore(databaseName);

        try
        {
            var dbRecord = new Raven.Client.ServerWide.DatabaseRecord(databaseName);
            await adminStore.Maintenance.Server.SendAsync(new Raven.Client.ServerWide.Operations.CreateDatabaseOperation(dbRecord));
        }
        catch (Raven.Client.Exceptions.ConcurrencyException)
        {
            // Database already exists, ignore
        }
    }

    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        using var adminStore = CreateAdminStore(_db);

        return await TransportAdminClient.GetDocumentCountAsync(adminStore, idPrefix).ConfigureAwait(false);
    }

    public void Dispose()
    {
        _http.Dispose();
    }
}
