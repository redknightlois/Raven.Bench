using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations;
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
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Collections.Generic;
using System.Diagnostics;
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
    private readonly string _db;
    private readonly string _compressionMode;
    private readonly Version _httpVersion;

    public string EffectiveCompressionMode => _compressionMode;
    public string EffectiveHttpVersion => HttpHelper.FormatHttpVersion(_httpVersion);


    public RavenClientTransport(string url, string database, string compressionMode, Version httpVersion)
    {
        _db = database;
        _compressionMode = compressionMode.ToLowerInvariant();
        _httpVersion = httpVersion;

        _store = new DocumentStore
        {
            Urls = [url],
            Database = database
        };

        ConfigureCompression();
        ConfigureHttpVersion();

        _store.Initialize();

        _calibrationHttp = CreateCalibrationHttpClient(url);
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
        
        if (_compressionMode == "identity")
        {
            conventions.UseHttpCompression = false;
            conventions.UseHttpDecompression = false;
            return;
        }

        // Allow compression modes explicitly requested via CLI
        conventions.UseHttpCompression = true;
        conventions.UseHttpDecompression = true;
        conventions.HttpCompressionAlgorithm = _compressionMode switch
        {
            "gzip" => HttpCompressionAlgorithm.Gzip,
            "zstd" => HttpCompressionAlgorithm.Zstd,
            _ => conventions.HttpCompressionAlgorithm // Keep current default
        };
    }

    private void ConfigureHttpVersion()
    {
        HttpHelper.ConfigureHttpVersion((DocumentStore)_store, _httpVersion, HttpVersionPolicy.RequestVersionExact);
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
                    // Store raw JSON to maintain consistency with RawHttpTransport
                    using (var session = _store.OpenAsyncSession())
                    {
                        // Store as raw JSON document to match RawHttpTransport behavior
                        var doc = new { Json = insertOp.Payload };
                        await session.StoreAsync(doc, insertOp.Id, ct).ConfigureAwait(false);
                        await session.SaveChangesAsync(ct).ConfigureAwait(false);
                    }
                    var outBytes = insertOp.Payload?.Length ?? 0;
                    long headerBytes = EstimateHeaderSize("PUT", $"/databases/{_db}/docs?id={Uri.EscapeDataString(insertOp.Id)}", outBytes);
                    return new TransportResult(headerBytes + outBytes, 256);
                }
                case UpdateOperation<string> updateOp:
                {
                    // Store raw JSON to maintain consistency with RawHttpTransport
                    using (var session = _store.OpenAsyncSession())
                    {
                        // Store as raw JSON document to match RawHttpTransport behavior
                        var doc = new { Json = updateOp.Payload };
                        await session.StoreAsync(doc, updateOp.Id, ct).ConfigureAwait(false);
                        await session.SaveChangesAsync(ct).ConfigureAwait(false);
                    }
                    var updateOutBytes = updateOp.Payload?.Length ?? 0;
                    long headerBytes = EstimateHeaderSize("PUT", $"/databases/{_db}/docs?id={Uri.EscapeDataString(updateOp.Id)}", updateOutBytes);
                    return new TransportResult(headerBytes + updateOutBytes, 256);
                }
                case QueryOperation queryOp:
                {
                    using (var s = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
                    {
                        // Build parameterized query from QueryOperation
                        var query = s.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(queryOp.QueryText);

                        // Add all parameters
                        foreach (var param in queryOp.Parameters)
                        {
                            query = query.AddParameter(param.Key, param.Value);
                        }

                        // Execute query and capture statistics
                        var results = await query.Statistics(out var stats).ToListAsync(ct).ConfigureAwait(false);

                        // Estimate bytes out: query payload + headers
                        long queryPayloadBytes = EstimateQueryPayloadSize(queryOp);
                        long headerBytes = EstimateHeaderSize("POST", $"/databases/{_db}/queries", queryPayloadBytes);
                        long bytesOut = queryPayloadBytes + headerBytes;

                        // Estimate bytes in: serialize results and add metadata overhead
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
                case BulkInsertOperation<string> bulkOp:
                {
                    using (var bulkInsert = _store.BulkInsert())
                    {
                        foreach (var docToWrite in bulkOp.Documents)
                        {
                            var doc = new { Json = docToWrite.Document };
                            await bulkInsert.StoreAsync(doc, docToWrite.Id).ConfigureAwait(false);
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
                        // Build the RQL query based on quantization type
                        string queryText = BuildVectorSearchQuery(vectorOp);

                        var query = s.Advanced.AsyncRawQuery<BlittableJsonReaderObject>(queryText)
                            .AddParameter("vector", vectorOp.QueryVector)
                            .Take(vectorOp.TopK);

                        if (vectorOp.MinimumSimilarity > 0)
                        {
                            query = query.AddParameter("minSimilarity", vectorOp.MinimumSimilarity);
                        }

                        // Execute query and capture statistics
                        var results = await query.Statistics(out var stats).ToListAsync(ct).ConfigureAwait(false);

                        // Estimate bytes out: vector payload + query text + headers
                        long vectorPayloadBytes = EstimateVectorPayloadSize(vectorOp);
                        long headerBytes = EstimateHeaderSize("POST", $"/databases/{_db}/queries", vectorPayloadBytes);
                        long bytesOut = vectorPayloadBytes + headerBytes;

                        // Estimate bytes in: results + metadata
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
            // TaskCanceledException from timeout is expected during benchmark runs
            // Return a result without error details to avoid logging as error
            return new TransportResult(0, 0);
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
        // Store raw JSON to maintain consistency with RawHttpTransport
        using var session = _store.OpenAsyncSession();
        var doc = new { Json = document };
        await session.StoreAsync(doc, id).ConfigureAwait(false);
        await session.SaveChangesAsync().ConfigureAwait(false);
    }


    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        var baseUrl = _store.Urls[0].TrimEnd('/');
        return await RavenServerMetricsCollector.CollectAsync(baseUrl, _store.Database, HttpHelper.FormatHttpVersion(_httpVersion));
    }

    public async Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null)
    {
        long? databaseIndex = null;

        // Discover database SNMP index if database name is provided
        if (string.IsNullOrEmpty(databaseName) == false && await TryGetDatabaseSnmpIndexAsync(databaseName) is { } index)
        {
            databaseIndex = index;
        }

        var snmpClient = new SnmpClient();
        var oids = SnmpOids.GetOidsForProfile(snmpOptions.Profile, databaseIndex);
        var host = new Uri(_store.Urls[0]).Host;
        var timeoutMs = (int)snmpOptions.Timeout.TotalMilliseconds;
        var snmpResults = await snmpClient.GetManyAsync(oids, host, snmpOptions.Port, SnmpOptions.Community, timeoutMs);
        return SnmpMetricMapper.MapToSample(snmpResults);
    }

    private async Task<long?> TryGetDatabaseSnmpIndexAsync(string databaseName)
    {
        try
        {
            using var resp = await _calibrationHttp.GetAsync("/monitoring/snmp/oids", HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            // Navigate to Databases section
            if (doc.RootElement.TryGetProperty("Databases", out var databases) == false)
            {
                Console.WriteLine("[WARN] SNMP OID response missing 'Databases' property");
                return null;
            }

            // Look for the database by name
            if (databases.TryGetProperty(databaseName, out var dbElement) == false)
            {
                Console.WriteLine($"[WARN] Database '{databaseName}' not found in SNMP OID mapping");
                return null;
            }

            // Extract database index from the first OID
            if (dbElement.TryGetProperty("@General", out var general) == false || general.GetArrayLength() == 0)
            {
                Console.WriteLine($"[WARN] No '@General' section or empty for database '{databaseName}'");
                return null;
            }

            var firstOid = general[0].GetProperty("OID").GetString();
            if (SnmpOids.TryParseDatabaseIndexFromOid(firstOid, out var index))
            {
                return index;
            }

            Console.WriteLine($"[WARN] Failed to parse database index from OID: {firstOid}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[WARN] Failed to discover SNMP database index for '{databaseName}': {ex.Message}");
            Console.WriteLine("[WARN] Falling back to server-wide SNMP metrics (IO/request metrics will be unavailable)");
            return null;
        }
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

    public async Task<string> GetServerLicenseTypeAsync()
    {
        try
        {
            using var resp = await _calibrationHttp.GetAsync("/license/status", HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            if (doc.RootElement.TryGetProperty("Type", out var licenseType))
                return licenseType.GetString() ?? "unknown";
            if (doc.RootElement.TryGetProperty("LicenseType", out var altLicenseType))
                return altLicenseType.GetString() ?? "unknown";

            return "unknown";
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Raven.Bench] Warning: Failed to get license type: {ex.GetType().Name}: {ex.Message}");
            return "unknown";
        }
    }


    public async Task ValidateClientAsync()
    {
        try
        {
            // Test connection and capture HTTP version if possible
            using var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = true });
            await session.LoadAsync<object>("validation-test-key-that-does-not-exist").ConfigureAwait(false);

        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed: Unable to connect to RavenDB server. {ex.Message}", ex);
        }
    }

    public async Task ValidateClientAsync(bool strictHttpVersion)
    {
        await ValidateClientAsync().ConfigureAwait(false);

        Console.WriteLine($"[Raven.Bench] Client validation: Using HTTP/{HttpHelper.FormatHttpVersion(_httpVersion)}");
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

    public async Task<long> GetDocumentCountAsync(string idPrefix)
    {
        using var session = _store.OpenAsyncSession();
        // Use streaming to count documents without indexing delay
        var count = 0L;
        await using (var stream = await session.Advanced.StreamAsync<object>(startsWith: idPrefix))
        {
            while (await stream.MoveNextAsync())
            {
                count++;
            }
        }
        return count;
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
            // Use maintenance operation if exposed; fallback to HTTP endpoint via server URL.
            // We don't depend on server admin permissions here; non-fatal if denied.
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
            var requestUri = CreateCalibrationRequestUri(endpoint);
            using var req = new HttpRequestMessage(HttpMethod.Get, requestUri);
            req.Version = _httpVersion;
            req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            req.Headers.ExpectContinue = false;
            req.Headers.ConnectionClose = false;

            var acceptEncoding = GetAcceptEncodingHeaderValue(_compressionMode);
            if (string.IsNullOrWhiteSpace(acceptEncoding) == false)
            {
                req.Headers.AcceptEncoding.Clear();
                req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(acceptEncoding));
            }

            return await _calibrationHttp.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }, ct, fallbackHttpVersion: _httpVersion).ConfigureAwait(false);
    }

    private static Uri CreateCalibrationRequestUri(string endpoint)
    {
        // Absolute URLs bypass BaseAddress, relative URLs use it.
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out var absoluteUri) &&
            string.IsNullOrWhiteSpace(absoluteUri.Scheme) == false &&
            (absoluteUri.Scheme == "http" || absoluteUri.Scheme == "https"))
        {
            return absoluteUri;
        }

        if (endpoint.StartsWith('/') == false)
            endpoint = "/" + endpoint;

        return new Uri(endpoint, UriKind.Relative);
    }

    public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints()
    {
        return new List<(string, string)>
        {
            ("server-version", "/build/version"),
            ("license-status", "/license/status")
        };
    }

    private static string? GetAcceptEncodingHeaderValue(string compression)
    {
        if (string.IsNullOrWhiteSpace(compression))
            return null;

        var normalized = compression.Trim().ToLowerInvariant();
        return normalized == "brotli" ? "br" : normalized;
    }

    /// <summary>
    /// Estimates the size of HTTP headers for a request, similar to RawHttpTransport.CalculateHeaderSize.
    /// Includes request line, standard headers, and blank line.
    /// </summary>
    private long EstimateHeaderSize(string method, string path, long contentLength = 0)
    {
        // Request line: "METHOD /path HTTP/1.1\r\n"
        string requestLine = $"{method} {path} HTTP/1.1\r\n";
        long size = Encoding.UTF8.GetByteCount(requestLine);

        // Standard headers (approximated)
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

        // Blank line after headers
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
            if (!first) size += Encoding.UTF8.GetByteCount(",");
            first = false;

            // Parameter key and value
            size += Encoding.UTF8.GetByteCount($"\"{param.Key}\":");
            if (param.Value is string strValue)
            {
                size += Encoding.UTF8.GetByteCount($"\"{strValue}\"");
            }
            else
            {
                // For non-string values, approximate
                size += Encoding.UTF8.GetByteCount(param.Value?.ToString() ?? "null");
            }
        }

        size += Encoding.UTF8.GetByteCount("}}");
        return size;
    }

    /// <summary>
    /// Estimates the size of the query response JSON payload received from RavenDB server.
    /// This includes the results array, metadata fields (IndexName, IsStale, DurationInMs), and JSON overhead.
    /// </summary>
    private static long EstimateQueryResponseSize(List<BlittableJsonReaderObject> results, QueryStatistics stats)
    {
        // Use blittable sizes (avoids re-serializing results just for byte estimation)
        long resultsSize = 0;
        foreach (var doc in results)
        {
            resultsSize += doc?.Size ?? 0;
        }

        // Add metadata fields size
        long metadataSize = 0;
        if (stats.IndexName != null)
            metadataSize += Encoding.UTF8.GetByteCount($"\"IndexName\":\"{stats.IndexName}\",");
        metadataSize += Encoding.UTF8.GetByteCount($"\"IsStale\":{stats.IsStale.ToString().ToLower()},");
        if (stats.DurationInMs > 0)
            metadataSize += Encoding.UTF8.GetByteCount($"\"DurationInMs\":{stats.DurationInMs},");

        // JSON structure overhead: {"Results":[...], "IndexName":..., "IsStale":..., "DurationInMs":...}
        long overhead = Encoding.UTF8.GetByteCount("{\"Results\":[],\"IndexName\":\"\",\"IsStale\":false,\"DurationInMs\":0}");

        return resultsSize + metadataSize + overhead;
    }

    /// <summary>
    /// Builds the RQL query for vector search based on quantization type and exact search flag.
    /// </summary>
    private string BuildVectorSearchQuery(VectorSearchOperation op)
    {
        return op.ToRqlQuery();
    }

    /// <summary>
    /// Estimates the size of the vector search payload sent to RavenDB server.
    /// </summary>
    private static long EstimateVectorPayloadSize(VectorSearchOperation op)
    {
        // Base query text
        string queryText = $"from @all_docs where vector.search('{op.FieldName}', $vector)";
        long size = Encoding.UTF8.GetByteCount("{\"Query\":\"") +
                    Encoding.UTF8.GetByteCount(queryText) +
                    Encoding.UTF8.GetByteCount("\",\"QueryParameters\":{");

        // Vector parameter: array of floats
        // Plus commas and brackets
        size += Encoding.UTF8.GetByteCount("\"$vector\":[");
        size += op.QueryVector.Length * EstimatedJsonFloatSize;
        size += op.QueryVector.Length - 1;  // Commas between elements
        size += Encoding.UTF8.GetByteCount("]");

        // Minimum similarity parameter if present
        if (op.MinimumSimilarity > 0)
        {
            size += Encoding.UTF8.GetByteCount(",\"$minSimilarity\":");
            size += Encoding.UTF8.GetByteCount(op.MinimumSimilarity.ToString());
        }

        size += Encoding.UTF8.GetByteCount("}}");
        return size;
    }

}
