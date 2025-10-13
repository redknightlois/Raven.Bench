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
        return op switch
        {
            ReadOperation readOp => await GetAsync(readOp.Id, ct).ConfigureAwait(false),
            InsertOperation<string> insertOp => await PutAsyncInternal(insertOp.Id, insertOp.Payload, ct).ConfigureAwait(false),
            UpdateOperation<string> updateOp => await PutAsyncInternal(updateOp.Id, updateOp.Payload, ct).ConfigureAwait(false),
            QueryOperation queryOp => await PostQueryAsync(queryOp.Id, ct).ConfigureAwait(false),
            _ => new TransportResult(0, 0)
        };
    }

    public async Task PutAsync<T>(string id, T document)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await PutAsyncInternal(id, document, cts.Token).ConfigureAwait(false);
    }

    private async Task<TransportResult> PostQueryAsync(string id, CancellationToken ct)
    {
        try
        {
            var url = $"{_baseUrl}/databases/{_db}/queries";

            using var req = CreateRequest(HttpMethod.Post, url);
            req.Headers.AcceptEncoding.Clear();
            req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
            req.Headers.ExpectContinue = false;

            // RavenDB parameterized query against @all_docs by id
            var queryPayload = JsonSerializer.Serialize(new
            {
                Query = "from @all_docs where id() = $id",
                QueryParameters = new { id }
            });
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

                long bytesOut = req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(queryPayload);
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
            req.Content = new StringContent(JsonSerializer.Serialize(document), Encoding.UTF8, "application/json");

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
                long bytesOut = req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(document));
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
