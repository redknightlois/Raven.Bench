using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using RavenBench.Metrics.Snmp;
using System.Text.Json;
using System.Buffers;
using System.Diagnostics;
using RavenBench.Workload;
using RavenBench.Metrics;
using RavenBench.Util;
using RavenBench.Diagnostics;

namespace RavenBench.Transport;

public sealed class RawHttpTransport : ITransport
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string _db;
    private readonly string _acceptEncoding;
    private readonly string? _customEndpoint; // optional format with {id}
    private readonly LockFreeRingBuffer<byte[]> _bufferPool;
    private readonly Version _httpVersion;
    public string EffectiveCompressionMode { get; } = "identity";
    public string EffectiveHttpVersion => HttpHelper.FormatHttpVersion(_httpVersion);


    public RawHttpTransport(string url, string database, string compressionMode, Version httpVersion, string? endpoint = null)
    {
        _db = database;
        _baseUrl = url.TrimEnd('/');
        _acceptEncoding = compressionMode.Equals("identity", StringComparison.OrdinalIgnoreCase) ? "identity" : compressionMode;
        EffectiveCompressionMode = _acceptEncoding;
        _customEndpoint = string.IsNullOrWhiteSpace(endpoint) ? null : endpoint;
        _httpVersion = httpVersion;

        // Configure automatic decompression for supported formats
        // Note: Zstd requires third-party library as it's not supported in .NET's DecompressionMethods
        var decompression = _acceptEncoding.ToLowerInvariant() switch
        {
            "gzip" => DecompressionMethods.GZip,
            "deflate" => DecompressionMethods.Deflate,
            "br" or "brotli" => DecompressionMethods.Brotli,
            _ => DecompressionMethods.None
        };

        var handler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
        handler.AutomaticDecompression = decompression;

        // Set up HTTP client with the specific negotiated version
        var httpVersionInfo = (_httpVersion, HttpVersionPolicy.RequestVersionExact);
        var client = new HttpClient(new HttpHelper.HttpVersionHandler(handler, httpVersionInfo))
        {
            Timeout = Timeout.InfiniteTimeSpan
        };

        _http = client;
        
        // Initialize buffer pool with 32KB buffers (2x max concurrency)
        _bufferPool = new LockFreeRingBuffer<byte[]>(32768);
        for (int i = 0; i < 32768; i++)
        {
            _bufferPool.TryEnqueue(new byte[32768]); // 32KB buffers
        }
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
                case QueryOperation queryOp:
                    return await PostQueryAsync(queryOp, ct).ConfigureAwait(false);
                case BulkInsertOperation<string> bulkOp:
                    return await PostBulkDocsAsync(bulkOp.Documents, ct).ConfigureAwait(false);
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

    /// <summary>
    /// Calculates the size of HTTP headers in bytes for accurate network utilization measurement.
    /// Includes request line, all headers, and the blank line separator.
    /// </summary>
    private static long CalculateHeaderSize(HttpRequestMessage req)
    {
        // Request line: "POST /path HTTP/1.1\r\n"
        string requestLine = $"{req.Method} {req.RequestUri!.PathAndQuery} HTTP/{req.Version}\r\n";
        long size = Encoding.UTF8.GetByteCount(requestLine);

        // Headers
        foreach (var header in req.Headers)
        {
            string headerLine = $"{header.Key}: {string.Join(", ", header.Value)}\r\n";
            size += Encoding.UTF8.GetByteCount(headerLine);
        }

        // Content headers (if any)
        if (req.Content != null)
        {
            foreach (var header in req.Content.Headers)
            {
                string headerLine = $"{header.Key}: {string.Join(", ", header.Value)}\r\n";
                size += Encoding.UTF8.GetByteCount(headerLine);
            }
        }

        // Blank line after headers
        size += Encoding.UTF8.GetByteCount("\r\n");

        return size;
    }

    public async Task PutAsync<T>(string id, T document)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await PutAsyncInternal(id, document, cts.Token).ConfigureAwait(false);
    }

    private async Task<TransportResult> PostQueryAsync(QueryOperation queryOp, CancellationToken ct)
    {
        try
        {
            var url = $"{_baseUrl}/databases/{_db}/queries";

            using var req = CreateRequest(HttpMethod.Post, url);
            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;

            // Build parameterized query payload
            var payload = new
            {
                Query = queryOp.QueryText,
                QueryParameters = queryOp.Parameters,
                MetadataOnly = false
            };

            var queryPayload = JsonSerializer.Serialize(payload);
            req.Content = new StringContent(queryPayload, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            using (var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false))
            {
                if (resp.IsSuccessStatusCode == false)
                {
                    var errorContent = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                    var errorDetails = $"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {errorContent}";
                    return new TransportResult(0, 0, errorDetails);
                }

                // Parse response to extract query metadata (IndexName, ResultCount, IsStale)
                // Always buffer the response to accurately measure bytes and avoid stream consumption issues
                await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

                if (_bufferPool.TryDequeue(out var buffer) == false)
                    buffer = new byte[32768];

                using var ms = new MemoryStream();
                int bytesRead;
                while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
                {
                    ms.Write(buffer, 0, bytesRead);
                }
                _bufferPool.TryEnqueue(buffer);

                long bytesIn = ms.Length;
                ms.Position = 0;

                // Parse JSON from buffered response
                using var doc = await JsonDocument.ParseAsync(ms, cancellationToken: ct).ConfigureAwait(false);

                var indexName = doc.RootElement.TryGetProperty("IndexName", out var indexProp)
                    ? indexProp.GetString()
                    : null;

                var resultCount = doc.RootElement.TryGetProperty("Results", out var resultsProp) && resultsProp.ValueKind == JsonValueKind.Array
                    ? resultsProp.GetArrayLength()
                    : (int?)null;

                var isStale = doc.RootElement.TryGetProperty("IsStale", out var staleProp)
                    ? staleProp.GetBoolean()
                    : (bool?)null;

                // Extract query duration from response (server-side execution time)
                double? queryDurationMs = null;
                if (doc.RootElement.TryGetProperty("DurationInMs", out var durationProp))
                {
                    if (durationProp.ValueKind == JsonValueKind.Number)
                    {
                        queryDurationMs = durationProp.GetDouble();
                    }
                }

                // Calculate byte counts
                long bodyBytes = req.Content?.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(queryPayload);
                long headerBytes = CalculateHeaderSize(req);
                long bytesOut = headerBytes + bodyBytes;

                return new TransportResult(bytesOut, bytesIn, indexName: indexName, resultCount: resultCount, isStale: isStale, queryDurationMs: queryDurationMs);
            }
        }
        catch (TaskCanceledException)
        {
            if (ct.IsCancellationRequested)
                return new TransportResult(0, 0);
            return new TransportResult(0, 0, "Operation timed out after 30 seconds");
        }
        catch (Exception ex)
        {
            var errorDetails = $"Exception: {ex.GetType().Name}: {ex.Message}";
            return new TransportResult(0, 0, errorDetails);
        }
    }

    private async Task<TransportResult> PostBulkDocsAsync(List<DocumentToWrite<string>> documents, CancellationToken ct)
    {
        try
        {
            var url = $"{_baseUrl}/databases/{_db}/bulk_docs";

            using var req = CreateRequest(HttpMethod.Post, url);
            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;

            // Build RavenDB bulk_docs command array matching batch-writes.lua format
            var commands = new List<object>();
            foreach (var doc in documents)
            {
                commands.Add(new
                {
                    Method = "PUT",
                    Type = "PUT",
                    Id = doc.Id,
                    Document = JsonSerializer.Deserialize<Dictionary<string, object>>(doc.Document)
                });
            }

            var bulkPayload = JsonSerializer.Serialize(new { Commands = commands });
            req.Content = new StringContent(bulkPayload, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            using (var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false))
            {
                if (resp.IsSuccessStatusCode == false)
                {
                    var errorContent = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                    var errorDetails = $"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {errorContent}";
                    return new TransportResult(0, 0, errorDetails);
                }

                long bytesIn = 0;
                if (resp.Content.Headers.ContentLength.HasValue)
                {
                    bytesIn = resp.Content.Headers.ContentLength.Value;
                }
                else
                {
                    await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                    if (_bufferPool.TryDequeue(out var buffer) == false)
                        buffer = new byte[32768];
                    int totalRead = 0, bytesRead;
                    while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
                        totalRead += bytesRead;
                    bytesIn = totalRead;
                    _bufferPool.TryEnqueue(buffer);
                }

                long bytesOut = req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(bulkPayload);
                bytesOut += 400; // headers approx
                return new TransportResult(bytesOut, bytesIn);
            }
        }
        catch (TaskCanceledException)
        {
            if (ct.IsCancellationRequested)
                return new TransportResult(0, 0);
            return new TransportResult(0, 0, "Operation timed out after 30 seconds");
        }
        catch (Exception ex)
        {
            var errorDetails = $"Exception: {ex.GetType().Name}: {ex.Message}";
            return new TransportResult(0, 0, errorDetails);
        }
    }

    private async Task<TransportResult> GetAsync(string id, CancellationToken ct)
    {
        try
        {
            var url = BuildUrl(id);
            using var req = CreateRequest(HttpMethod.Get, url);

            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            using (var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false))
            {
                if (resp.IsSuccessStatusCode == false)
                {
                    var errorContent = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                    var errorDetails = $"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {errorContent}";
                    return new TransportResult(0, 0, errorDetails);
                }

                long bytesIn = 0;
                if (resp.Content.Headers.ContentLength.HasValue)
                {
                    bytesIn = resp.Content.Headers.ContentLength.Value;
                    // If gzip auto-decompressed, Content-Length is post-decompression; treat as payload bytes
                }
                else
                {
                    await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

                    if (_bufferPool.TryDequeue(out var buffer) == false)
                    {
                        buffer = new byte[32768]; // Fallback if pool is empty
                    }

                    int totalRead = 0;
                    int bytesRead;
                    while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
                    {
                        totalRead += bytesRead;
                    }
                    bytesIn = totalRead;

                    _bufferPool.TryEnqueue(buffer); // Return to pool
                }
                // crude header bytes estimate
                var bytesOut = 400; // request headers approx
                return new TransportResult(bytesOut, bytesIn);
            }
        }
        catch (TaskCanceledException)
        {
            // If the external token (from benchmark) is cancelled, it's end-of-run, not an error
            if (ct.IsCancellationRequested)
            {
                // External cancellation (benchmark ending) - not an error
                return new TransportResult(0, 0);
            }
            // If we get here, it was likely our internal 30-second timeout
            return new TransportResult(0, 0, "Operation timed out after 30 seconds");
        }
        catch (Exception ex)
        {
            var errorDetails = $"Exception: {ex.GetType().Name}: {ex.Message}";
            return new TransportResult(0, 0, errorDetails);
        }
    }

    private async Task<TransportResult> PutAsyncInternal<T>(string id, T document, CancellationToken ct)
    {
        try
        {
            var url = BuildUrl(id);

            using var req = CreateRequest(HttpMethod.Put, url);
            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;

            string jsonPayload = document is string s ? s : JsonSerializer.Serialize(document);
            req.Content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(30));

            using (var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false))
            {
                if (resp.IsSuccessStatusCode == false)
                {
                    var errorContent = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                    var errorDetails = $"HTTP {(int)resp.StatusCode} {resp.StatusCode}: {errorContent}";
                    return new TransportResult(0, 0, errorDetails);
                }

                long bytesIn = 0;
                if (resp.Content.Headers.ContentLength.HasValue)
                {
                    bytesIn = resp.Content.Headers.ContentLength.Value;
                }
                else
                {
                    await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);

                    if (_bufferPool.TryDequeue(out var buffer) == false)
                        buffer = new byte[32768]; // Fallback if pool is empty

                    int totalRead = 0;
                    int bytesRead;
                    while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
                    {
                        totalRead += bytesRead;
                    }
                    bytesIn = totalRead;

                    _bufferPool.TryEnqueue(buffer); // Return to pool
                }
                long bytesOut = req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(jsonPayload);
                bytesOut += 400; // headers approx
                return new TransportResult(bytesOut, bytesIn);
            }
        }
        catch (TaskCanceledException)
        {
            // If the external token (from benchmark) is cancelled, it's end-of-run, not an error
            if (ct.IsCancellationRequested)
            {
                // External cancellation (benchmark ending) - not an error
                return new TransportResult(0, 0);
            }
            // If we get here, it was likely our internal 30-second timeout
            return new TransportResult(0, 0, "Operation timed out after 30 seconds");
        }
        catch (Exception ex)
        {
            var errorDetails = $"Exception: {ex.GetType().Name}: {ex.Message}";
            return new TransportResult(0, 0, errorDetails);
        }
    }

    public async Task<int?> GetServerMaxCoresAsync()
    {
        try
        {
            var url = $"{_baseUrl}/license/status";
            using var req = CreateRequest(HttpMethod.Get, url);

            using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
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
        var host = new Uri(_baseUrl).Host;
        var timeoutMs = (int)snmpOptions.Timeout.TotalMilliseconds;
        var snmpResults = await snmpClient.GetManyAsync(oids, host, snmpOptions.Port, SnmpOptions.Community, timeoutMs);
        return SnmpMetricMapper.MapToSample(snmpResults);
    }

    private async Task<long?> TryGetDatabaseSnmpIndexAsync(string databaseName)
    {
        try
        {
            var url = $"{_baseUrl}/monitoring/snmp/oids";
            using var req = CreateRequest(HttpMethod.Get, url);

            using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
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
            var url = $"{_baseUrl}/build/version";
            using var req = CreateRequest(HttpMethod.Get, url);

            using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
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

    public async Task<string> GetServerLicenseTypeAsync()
    {
        try
        {
            var url = $"{_baseUrl}/license/status";
            using var req = CreateRequest(HttpMethod.Get, url);

            using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);

            if (doc.RootElement.TryGetProperty("Type", out var licenseType))
                return licenseType.GetString() ?? "unknown";

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
        await ValidateClientAsync(false);
    }

    public async Task ValidateClientAsync(bool strictHttpVersion)
    {
        try
        {
            var url = $"{_baseUrl}/build/version";
            using var req = CreateRequest(HttpMethod.Get, url);

            using var response = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead).ConfigureAwait(false);

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
            var url = BuildCalibrationUrl(endpoint);
            using var req = CreateRequest(HttpMethod.Get, url);

            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;
            req.Headers.ConnectionClose = false;

            return await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }, ct, fallbackHttpVersion: _httpVersion).ConfigureAwait(false);
    }

    private string BuildCalibrationUrl(string endpoint)
    {
        // Check if endpoint is truly absolute (has a scheme like http:// or https://)
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) &&
            uri.Scheme != null && (uri.Scheme == "http" || uri.Scheme == "https"))
            return endpoint;

        if (endpoint.StartsWith('/') == false)
            endpoint = "/" + endpoint;

        return _baseUrl + endpoint;
    }

    public IReadOnlyList<(string name, string path)> GetCalibrationEndpoints()
    {
        return new List<(string, string)>
        {
            ("server-version", "/build/version"),
            ("license-status", "/license/status")
        };
    }

    private static string? ExtractAcceptEncoding(string compression)
    {
        if (string.IsNullOrWhiteSpace(compression))
            return null;

        var parts = compression.Split(':', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2)
            return null;

        return parts[1].ToLowerInvariant();
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
    /// Creates an HttpRequestMessage with proper HTTP version and policy settings.
    /// </summary>
    private HttpRequestMessage CreateRequest(HttpMethod method, string url)
    {
        var req = new HttpRequestMessage(method, url);
        req.Version = _httpVersion;
        req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
        return req;
    }

    public async Task EnsureDatabaseExistsAsync(string databaseName)
    {
        // Use RavenDB client for administrative tasks
        using var adminStore = new Raven.Client.Documents.DocumentStore
        {
            Urls = [_baseUrl],
            Database = databaseName
        };
        adminStore.Initialize();

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
        // Use RavenDB client for querying document count
        using var adminStore = new Raven.Client.Documents.DocumentStore
        {
            Urls = [_baseUrl],
            Database = _db
        };
        adminStore.Initialize();

        using var session = adminStore.OpenAsyncSession();
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
        _http.Dispose();
    }
}
