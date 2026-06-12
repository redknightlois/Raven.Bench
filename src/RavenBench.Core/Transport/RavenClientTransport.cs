using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using RavenBench.Core.Workload;
using RavenBench.Core.Metrics;
using RavenBench.Core;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics.Snmp;
using System.Text.Json;
using System.Text;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using Raven.Client.Documents.Linq;

namespace RavenBench.Core.Transport;

/// <summary>
/// Transport implementation using the official RavenDB .NET client.
/// Provides high-level document operations through sessions for benchmarking.
/// </summary>
public sealed class RavenClientTransport : ITransport
{
    /// <summary>
    /// Estimated JSON serialization size per float32 value in characters.
    /// Example: "-0.123456789" = 12 characters including sign and decimal point.
    /// </summary>
    private const int EstimatedJsonFloatSize = 12;

    private readonly IDocumentStore _store;
    private readonly HttpClient _calibrationHttp;
    private readonly TransportAdminClient _admin;
    private readonly string _db;
    private readonly CompressionMode _compression;
    private readonly Version _httpVersion;

    public string EffectiveCompressionMode => _compression.ToWireFormat();
    public string EffectiveHttpVersion => HttpHelper.FormatHttpVersion(_httpVersion);

    // The client library abstracts the socket, so byte counts are estimated, not measured on the wire.
    public bool ReportsWireBytes => false;


    public RavenClientTransport(string url, string database, CompressionMode compression, Version httpVersion)
    {
        _db = database;
        _compression = compression;
        _httpVersion = httpVersion;

        _store = new DocumentStore
        {
            Urls = [url],
            Database = database
        };

        ConfigureCompression();
        HttpHelper.ConfigureHttpVersion((DocumentStore)_store, _httpVersion, HttpVersionPolicy.RequestVersionExact);

        _store.Initialize();

        _calibrationHttp = CreateCalibrationHttpClient(url);
        _admin = new TransportAdminClient(_calibrationHttp, url);
    }

    private HttpClient CreateCalibrationHttpClient(string url)
    {
        var handler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
        var httpVersionInfo = (_httpVersion, HttpVersionPolicy.RequestVersionExact);
        return new HttpClient(new HttpHelper.HttpVersionHandler(handler, httpVersionInfo))
        {
            BaseAddress = new Uri(url),
            Timeout = Timeout.InfiniteTimeSpan
        };
    }

    private void ConfigureCompression()
    {
        var conventions = _store.Conventions;

        if (_compression == CompressionMode.Identity)
        {
            conventions.UseHttpCompression = false;
            conventions.UseHttpDecompression = false;
            return;
        }

        conventions.UseHttpCompression = true;
        conventions.UseHttpDecompression = true;
        conventions.HttpCompressionAlgorithm = _compression switch
        {
            CompressionMode.Gzip => HttpCompressionAlgorithm.Gzip,
            CompressionMode.Zstd => HttpCompressionAlgorithm.Zstd,
            _ => conventions.HttpCompressionAlgorithm
        };
    }

    /// <summary>
    /// Stores the raw JSON payload as the document body, byte-identical to what RawHttpTransport PUTs.
    /// </summary>
    private async Task PutRawJsonAsync(string id, string jsonPayload, CancellationToken ct)
    {
        var requestExecutor = _store.GetRequestExecutor();
        using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(jsonPayload));
            var document = await context.ReadForMemoryAsync(stream, id).ConfigureAwait(false);
            var command = new PutDocumentCommand(_store.Conventions, id, changeVector: null, document);
            await requestExecutor.ExecuteAsync(command, context, sessionInfo: null, token: ct).ConfigureAwait(false);
        }
    }

    public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
    {
        try
        {
            switch (op)
            {
                case ReadOperation readOp:
                {
                    using (var s = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
                    {
                        var doc = await s.LoadAsync<BlittableJsonReaderObject>(readOp.Id, ct).ConfigureAwait(false);
                        long bytesIn = doc?.Size ?? 0;
                        long headerBytes = EstimateHeaderSize("GET", $"/databases/{_db}/docs?id={Uri.EscapeDataString(readOp.Id)}");
                        return new TransportResult(headerBytes, bytesIn);
                    }
                }
                case InsertOperation<string> insertOp:
                {
                    await PutRawJsonAsync(insertOp.Id, insertOp.Payload, ct).ConfigureAwait(false);
                    var outBytes = insertOp.Payload?.Length ?? 0;
                    long headerBytes = EstimateHeaderSize("PUT", $"/databases/{_db}/docs?id={Uri.EscapeDataString(insertOp.Id)}", outBytes);
                    return new TransportResult(headerBytes + outBytes, 256);
                }
                case UpdateOperation<string> updateOp:
                {
                    await PutRawJsonAsync(updateOp.Id, updateOp.Payload, ct).ConfigureAwait(false);
                    var updateOutBytes = updateOp.Payload?.Length ?? 0;
                    long headerBytes = EstimateHeaderSize("PUT", $"/databases/{_db}/docs?id={Uri.EscapeDataString(updateOp.Id)}", updateOutBytes);
                    return new TransportResult(headerBytes + updateOutBytes, 256);
                }
                case StreamQueryOperation streamOp:
                {
                    using (var s = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
                    {
                        var query = s.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(streamOp.QueryText);
                        foreach (var param in streamOp.Parameters)
                            query = query.AddParameter(param.Key, param.Value);

                        int count = 0;
                        long bytesIn = 0;
                        await using (var stream = await s.Advanced.StreamAsync(query, ct).ConfigureAwait(false))
                        {
                            while (await stream.MoveNextAsync().ConfigureAwait(false))
                            {
                                count++;
                                bytesIn += stream.Current.Document?.Size ?? 0;
                            }
                        }

                        long queryPayloadBytes = EstimateQueryPayloadSize(streamOp);
                        long headerBytes = EstimateHeaderSize("POST", $"/databases/{_db}/streams/queries", queryPayloadBytes);

                        return new TransportResult(queryPayloadBytes + headerBytes, bytesIn, indexName: streamOp.ExpectedIndex, resultCount: count);
                    }
                }
                case QueryOperation queryOp:
                {
                    using (var s = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
                    {
                        var query = s.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(queryOp.QueryText);

                        foreach (var param in queryOp.Parameters)
                        {
                            query = query.AddParameter(param.Key, param.Value);
                        }

                        var results = await query.Statistics(out var stats).ToListAsync(ct).ConfigureAwait(false);

                        long queryPayloadBytes = EstimateQueryPayloadSize(queryOp);
                        long headerBytes = EstimateHeaderSize("POST", $"/databases/{_db}/queries", queryPayloadBytes);
                        long bytesOut = queryPayloadBytes + headerBytes;

                        long bytesIn = EstimateQueryResponseSize(results, stats);

                        return new TransportResult(
                            bytesOut: bytesOut,
                            bytesIn: bytesIn,
                            indexName: stats.IndexName,
                            resultCount: results.Count,
                            isStale: stats.IsStale,
                            queryDurationMs: stats.DurationInMs
                        );
                    }
                }
                case DocumentPatchOperation patchOp:
                {
                    var operation = new Raven.Client.Documents.Operations.PatchOperation(
                        patchOp.Id,
                        changeVector: null,
                        new Raven.Client.Documents.Operations.PatchRequest { Script = patchOp.Script });
                    await _store.Operations.SendAsync(operation, token: ct).ConfigureAwait(false);

                    long payloadBytes = patchOp.Script.Length + 64;
                    long headerBytes = EstimateHeaderSize("PATCH", $"/databases/{_db}/docs?id={Uri.EscapeDataString(patchOp.Id)}", payloadBytes);
                    return new TransportResult(headerBytes + payloadBytes, 256);
                }
                case AttachmentOperation attachmentOp:
                {
                    var path = $"/databases/{_db}/attachments?id={Uri.EscapeDataString(attachmentOp.DocumentId)}&name={Uri.EscapeDataString(attachmentOp.Name)}";
                    switch (attachmentOp.Kind)
                    {
                        case AttachmentOperationKind.Put:
                        {
                            using var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = false });
                            using var ms = new MemoryStream(attachmentOp.Payload!);
                            session.Advanced.Attachments.Store(attachmentOp.DocumentId, attachmentOp.Name, ms);
                            await session.SaveChangesAsync(ct).ConfigureAwait(false);
                            long headerBytes = EstimateHeaderSize("PUT", path, attachmentOp.Payload!.Length);
                            return new TransportResult(headerBytes + attachmentOp.Payload!.Length, 256);
                        }
                        case AttachmentOperationKind.Get:
                        {
                            using var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = true });
                            using var result = await session.Advanced.Attachments.GetAsync(attachmentOp.DocumentId, attachmentOp.Name, ct).ConfigureAwait(false);
                            long bytesIn = 0;
                            if (result != null)
                            {
                                var buffer = new byte[64 * 1024];
                                int read;
                                while ((read = await result.Stream.ReadAsync(buffer, ct).ConfigureAwait(false)) > 0)
                                    bytesIn += read;
                            }
                            return new TransportResult(EstimateHeaderSize("GET", path), bytesIn);
                        }
                        case AttachmentOperationKind.Delete:
                        {
                            using var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = false });
                            session.Advanced.Attachments.Delete(attachmentOp.DocumentId, attachmentOp.Name);
                            await session.SaveChangesAsync(ct).ConfigureAwait(false);
                            return new TransportResult(EstimateHeaderSize("DELETE", path), 256);
                        }
                        default:
                            throw new ArgumentOutOfRangeException(nameof(attachmentOp.Kind), attachmentOp.Kind, null);
                    }
                }
                case BulkInsertOperation<string> bulkOp:
                {
                    var requestExecutor = _store.GetRequestExecutor();
                    using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (var bulkInsert = _store.BulkInsert(token: ct))
                    {
                        foreach (var docToWrite in bulkOp.Documents)
                        {
                            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(docToWrite.Document));
                            // Blittable entities pass through the bulk-insert serializer unchanged.
                            var document = await context.ReadForMemoryAsync(stream, docToWrite.Id).ConfigureAwait(false);
                            await bulkInsert.StoreAsync(document, docToWrite.Id).ConfigureAwait(false);
                        }
                    }
                    var bulkOutBytes = bulkOp.Documents.Sum(d => (d.Document?.Length ?? 0) + 50);
                    long headerBytes = EstimateHeaderSize("POST", $"/databases/{_db}/bulk_docs", bulkOutBytes);
                    return new TransportResult(headerBytes + bulkOutBytes, 256 * bulkOp.Documents.Count);
                }
                case VectorSearchOperation vectorOp:
                {
                    using (var s = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
                    {
                        var query = s.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(vectorOp.ToRqlQuery())
                            .AddParameter("vector", vectorOp.QueryVector)
                            .Take(vectorOp.TopK);

                        if (vectorOp.MinimumSimilarity > 0)
                        {
                            query = query.AddParameter("minSimilarity", vectorOp.MinimumSimilarity);
                        }

                        if (vectorOp.EfSearch.HasValue)
                        {
                            query = query.AddParameter("efSearch", vectorOp.EfSearch.Value);
                        }

                        var results = await query.Statistics(out var stats).ToListAsync(ct).ConfigureAwait(false);

                        long vectorPayloadBytes = EstimateVectorPayloadSize(vectorOp);
                        long headerBytes = EstimateHeaderSize("POST", $"/databases/{_db}/queries", vectorPayloadBytes);
                        long bytesOut = vectorPayloadBytes + headerBytes;

                        long bytesIn = EstimateQueryResponseSize(results, stats);

                        return new TransportResult(
                            bytesOut: bytesOut,
                            bytesIn: bytesIn,
                            indexName: stats.IndexName,
                            resultCount: results.Count,
                            isStale: stats.IsStale,
                            queryDurationMs: stats.DurationInMs
                        );
                    }
                }
                default:
                    return new TransportResult(0, 0);
            }
        }
        catch (TaskCanceledException)
        {
            if (ct.IsCancellationRequested)
                return TransportResult.CancelledResult;
            return new TransportResult(0, 0, "Operation timed out");
        }
        catch (HttpRequestException httpEx)
        {
            var errorMsg = $"HTTP {httpEx.Data["StatusCode"] ?? "Error"}: {httpEx.Message}";
            return new TransportResult(0, 0, errorMsg);
        }
        catch (Exception ex)
        {
            return new TransportResult(0, 0, ex.Message);
        }
    }

    public async Task PutAsync<T>(string id, T document)
    {
        string jsonPayload = document is string s ? s : JsonSerializer.Serialize(document);
        await PutRawJsonAsync(id, jsonPayload, CancellationToken.None).ConfigureAwait(false);
    }


    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        var baseUrl = _store.Urls[0].TrimEnd('/');
        return await RavenServerMetricsCollector.CollectAsync(baseUrl, _store.Database, HttpHelper.FormatHttpVersion(_httpVersion));
    }

    public Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null)
    {
        return _admin.GetSnmpMetricsAsync(snmpOptions, databaseName);
    }

    public async Task<string> GetServerVersionAsync()
    {
        try
        {
            var operation = new GetBuildNumberOperation();
            var buildNumber = await _store.Maintenance.Server.SendAsync(operation).ConfigureAwait(false);
            return buildNumber.FullVersion ?? buildNumber.ProductVersion ?? "unknown";
        }
        catch
        {
            return "unknown";
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
            using var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = true });
            await session.LoadAsync<object>("validation-test-key-that-does-not-exist").ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed: Unable to connect to RavenDB server. {ex.Message}", ex);
        }
    }

    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        try
        {
            var dbRecord = new Raven.Client.ServerWide.DatabaseRecord(databaseName);
            await _store.Maintenance.Server.SendAsync(new Raven.Client.ServerWide.Operations.CreateDatabaseOperation(dbRecord));
        }
        catch (Raven.Client.Exceptions.ConcurrencyException)
        {
            // Database already exists, ignore
        }
    }

    public Task<long> GetDocumentCountAsync(string idPrefix)
    {
        return TransportAdminClient.GetDocumentCountAsync(_store, idPrefix);
    }

    public void Dispose()
    {
        _calibrationHttp.Dispose();
        _store.Dispose();
    }

    public async Task<int?> GetServerMaxCoresAsync()
    {
        try
        {
            using var resp = await _calibrationHttp.GetAsync("/license/status", HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            if (doc.RootElement.TryGetProperty("MaxCores", out var maxCores) && maxCores.TryGetInt32(out var cores))
                return cores;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Raven.Bench] Warning: Failed to get max cores: {ex.GetType().Name}: {ex.Message}");
        }
        return null;
    }


    public async Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default)
    {
        return await CalibrationHelper.ExecuteCalibrationAsync(async cancellationToken =>
        {
            var url = _admin.BuildCalibrationUrl(endpoint);
            var req = new HttpRequestMessage(HttpMethod.Get, url);
            req.Headers.ExpectContinue = false;
            req.Headers.ConnectionClose = false;

            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_compression.ToWireFormat()));

            return await _calibrationHttp.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }, ct, fallbackHttpVersion: _httpVersion).ConfigureAwait(false);
    }

    public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints()
    {
        return _admin.GetCalibrationEndpoints();
    }

    /// <summary>
    /// Estimates the size of HTTP headers for a request, similar to RawHttpTransport.CalculateHeaderSize.
    /// Includes request line, standard headers, and blank line.
    /// </summary>
    private long EstimateHeaderSize(string method, string path, long contentLength = 0)
    {
        string requestLine = $"{method} {path} HTTP/1.1\r\n";
        long size = Encoding.UTF8.GetByteCount(requestLine);

        var headers = new[]
        {
            "Accept: application/json",
            "Content-Type: application/json",
            $"Content-Length: {contentLength}",
            "User-Agent: RavenDB-Client",
            "Connection: keep-alive"
        };

        foreach (var header in headers)
        {
            size += Encoding.UTF8.GetByteCount($"{header}\r\n");
        }

        size += Encoding.UTF8.GetByteCount("\r\n");

        return size;
    }

    /// <summary>
    /// Estimates the size of the query JSON payload sent to RavenDB server.
    /// This includes the query text and serialized parameters.
    /// </summary>
    private static long EstimateQueryPayloadSize(QueryOperation queryOp)
    {
        // Approximate JSON structure: {"Query":"...", "QueryParameters":{...}}
        long size = Encoding.UTF8.GetByteCount("{\"Query\":\"") +
                    Encoding.UTF8.GetByteCount(queryOp.QueryText) +
                    Encoding.UTF8.GetByteCount("\",\"QueryParameters\":{");

        bool first = true;
        foreach (var param in queryOp.Parameters)
        {
            if (first == false) size += Encoding.UTF8.GetByteCount(",");
            first = false;

            size += Encoding.UTF8.GetByteCount($"\"{param.Key}\":");
            if (param.Value is string strValue)
            {
                size += Encoding.UTF8.GetByteCount($"\"{strValue}\"");
            }
            else
            {
                size += Encoding.UTF8.GetByteCount(param.Value?.ToString() ?? "null");
            }
        }

        size += Encoding.UTF8.GetByteCount("}}");
        return size;
    }

    /// <summary>
    /// Estimates the size of the query response JSON payload received from RavenDB server.
    /// This includes the results array and metadata fields (IndexName, IsStale, DurationInMs).
    /// </summary>
    private static long EstimateQueryResponseSize(List<BlittableJsonReaderObject> results, QueryStatistics stats)
    {
        // Blittable sizes avoid re-serializing results just for byte estimation.
        long size = Encoding.UTF8.GetByteCount("{\"Results\":[]}");
        foreach (var doc in results)
        {
            size += doc?.Size ?? 0;
        }

        if (stats.IndexName != null)
            size += Encoding.UTF8.GetByteCount($",\"IndexName\":\"{stats.IndexName}\"");
        size += Encoding.UTF8.GetByteCount($",\"IsStale\":{stats.IsStale.ToString().ToLower()}");
        if (stats.DurationInMs > 0)
            size += Encoding.UTF8.GetByteCount($",\"DurationInMs\":{stats.DurationInMs}");

        return size;
    }

    /// <summary>
    /// Estimates the size of the vector search payload sent to RavenDB server.
    /// </summary>
    private static long EstimateVectorPayloadSize(VectorSearchOperation op)
    {
        string queryText = $"from @all_docs where vector.search('{op.FieldName}', $vector)";
        long size = Encoding.UTF8.GetByteCount("{\"Query\":\"") +
                    Encoding.UTF8.GetByteCount(queryText) +
                    Encoding.UTF8.GetByteCount("\",\"QueryParameters\":{");

        size += Encoding.UTF8.GetByteCount("\"$vector\":[");
        size += op.QueryVector.Length * EstimatedJsonFloatSize;
        size += op.QueryVector.Length - 1;  // Commas between elements
        size += Encoding.UTF8.GetByteCount("]");

        if (op.MinimumSimilarity > 0)
        {
            size += Encoding.UTF8.GetByteCount(",\"$minSimilarity\":");
            size += Encoding.UTF8.GetByteCount(op.MinimumSimilarity.ToString());
        }

        size += Encoding.UTF8.GetByteCount("}}");
        return size;
    }

}
