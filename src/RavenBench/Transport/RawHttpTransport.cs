using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Buffers;
using RavenBench.Workload;
using RavenBench.Metrics;
using RavenBench.Util;

namespace RavenBench.Transport;

public sealed class RawHttpTransport : ITransport
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string _db;
    private readonly string _acceptEncoding;
    private readonly string? _customEndpoint; // optional format with {id}
    private readonly LockFreeRingBuffer<byte[]> _bufferPool;
    private readonly string _requestedHttpVersion;
    private string? _negotiatedHttpVersion;
    public string EffectiveCompressionMode { get; }
    public string EffectiveHttpVersion => _negotiatedHttpVersion ?? "unknown";

    private static string FormatHttpVersion(Version version) => version.ToString() switch
    {
        "3.0" => "3",
        "2.0" => "2",
        "1.1" => "1.1",
        "1.0" => "1.0",
        _ => version.ToString()
    };

    private static string NormalizeHttpVersion(string httpVersion) => httpVersion.ToLowerInvariant() switch
    {
        "http2" or "2.0" => "2",
        "http3" or "3.0" => "3",
        "http1.1" or "1.1" => "1.1",
        "http1.0" or "1.0" => "1.0",
        "auto" => "auto",
        _ => httpVersion // Pass through other values like "2", "3", etc.
    };

    public RawHttpTransport(string url, string database, string compressionMode, string httpVersion, string? endpoint = null)
    {
        _db = database;
        _baseUrl = url.TrimEnd('/');
        _acceptEncoding = compressionMode.Equals("identity", StringComparison.OrdinalIgnoreCase) ? "identity" : compressionMode;
        EffectiveCompressionMode = _acceptEncoding;
        _customEndpoint = string.IsNullOrWhiteSpace(endpoint) ? null : endpoint;
        _requestedHttpVersion = NormalizeHttpVersion(httpVersion);

        // Configure automatic decompression for supported formats
        // Note: Zstd requires third-party library as it's not supported in .NET's DecompressionMethods
        var decompression = _acceptEncoding.ToLowerInvariant() switch
        {
            "gzip" => DecompressionMethods.GZip,
            "deflate" => DecompressionMethods.Deflate,
            "br" or "brotli" => DecompressionMethods.Brotli,
            _ => DecompressionMethods.None
        };
        
        var handler = new SocketsHttpHandler
        {
            AutomaticDecompression = decompression,
            PooledConnectionLifetime = TimeSpan.FromMinutes(2),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(1),
            EnableMultipleHttp2Connections = true,
            MaxConnectionsPerServer = int.MaxValue,
            UseCookies = false,
            AllowAutoRedirect = false,
            KeepAlivePingDelay = TimeSpan.FromSeconds(60),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(30)
        };
        
        var client = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };

        (client.DefaultRequestVersion, client.DefaultVersionPolicy, _negotiatedHttpVersion) = _requestedHttpVersion switch
        {
            "2" => (HttpVersion.Version20, HttpVersionPolicy.RequestVersionExact, "2"),
            "3" => (HttpVersion.Version30, HttpVersionPolicy.RequestVersionExact, "3"),
            "auto" => (HttpVersion.Version30, HttpVersionPolicy.RequestVersionOrLower, "auto"),
            _ => (HttpVersion.Version11, HttpVersionPolicy.RequestVersionExact, "1.1")
        };

        _http = client;
        
        // Initialize buffer pool with 32KB buffers (2x max concurrency)
        _bufferPool = new LockFreeRingBuffer<byte[]>(32768);
        for (int i = 0; i < 32768; i++)
        {
            _bufferPool.TryEnqueue(new byte[32768]); // 32KB buffers
        }
    }

    public async Task<TransportResult> ExecuteAsync(Operation op, CancellationToken ct)
    {
        return op.Type switch
        {
            OperationType.ReadById => await GetAsync(op.Id, ct).ConfigureAwait(false),
            OperationType.Insert => await PutAsyncInternal(op.Id, op.Payload!, ct).ConfigureAwait(false),
            OperationType.Update => await PutAsyncInternal(op.Id, op.Payload!, ct).ConfigureAwait(false),
            _ => new TransportResult(0, 0)
        };
    }

    public async Task PutAsync(string id, string json)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await PutAsyncInternal(id, json, cts.Token).ConfigureAwait(false);
    }

    private async Task<TransportResult> GetAsync(string id, CancellationToken ct)
    {
        var url = BuildUrl(id);
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.AcceptEncoding.Clear();
        req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
        req.Headers.ExpectContinue = false;
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();

        // Update negotiated version if not yet set (fallback for when validation was skipped)
        if (_negotiatedHttpVersion == null)
        {
            _negotiatedHttpVersion = FormatHttpVersion(resp.Version);
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

    private async Task<TransportResult> PutAsyncInternal(string id, string json, CancellationToken ct)
    {
        var url = BuildUrl(id);
        using var req = new HttpRequestMessage(HttpMethod.Put, url);
        req.Headers.AcceptEncoding.Clear();
        req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(_acceptEncoding));
        req.Headers.ExpectContinue = false;
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");
        
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(TimeSpan.FromSeconds(30));
        
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        
        // Update negotiated version if not yet set (fallback for when validation was skipped)
        if (_negotiatedHttpVersion == null)
        {
            _negotiatedHttpVersion = FormatHttpVersion(resp.Version);
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
        long bytesOut = req.Content.Headers.ContentLength ?? Encoding.UTF8.GetByteCount(json);
        bytesOut += 400; // headers approx
        return new TransportResult(bytesOut, bytesIn);
    }

    public async Task<int?> GetServerMaxCoresAsync()
    {
        try
        {
            var url = $"{_baseUrl}/admin/license/status";
            using var resp = await _http.GetAsync(url).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);
            if (doc.RootElement.TryGetProperty("Details", out var details))
            {
                if (details.TryGetProperty("MaxCores", out var cores) && cores.TryGetInt32(out var c))
                    return c;
            }
        }
        catch { }
        return null;
    }

    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        return await RavenServerMetricsCollector.CollectAsync(_baseUrl, _db);
    }

    public async Task<string> GetServerVersionAsync()
    {
        try
        {
            var url = $"{_baseUrl}/build/version";
            using var resp = await _http.GetAsync(url).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);
            
            if (doc.RootElement.TryGetProperty("FullVersion", out var fullVersion))
                return fullVersion.GetString() ?? "unknown";
            if (doc.RootElement.TryGetProperty("ProductVersion", out var productVersion))
                return productVersion.GetString() ?? "unknown";
                
            return "unknown";
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
            var url = $"{_baseUrl}/admin/license/status";
            using var resp = await _http.GetAsync(url).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            await using var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false);
            using var doc = await JsonDocument.ParseAsync(stream).ConfigureAwait(false);
            
            if (doc.RootElement.TryGetProperty("Type", out var licenseType))
                return licenseType.GetString() ?? "unknown";
            if (doc.RootElement.TryGetProperty("LicenseType", out var altLicenseType))
                return altLicenseType.GetString() ?? "unknown";
                
            return "unknown";
        }
        catch 
        { 
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
            var url = $"{_baseUrl}/databases/{_db}/stats";
            using var response = await _http.GetAsync(url).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                throw new HttpRequestException($"Server returned {response.StatusCode}: {response.ReasonPhrase}");
            }

            // Always detect actual HTTP version during validation
            var actualVersion = FormatHttpVersion(response.Version);

            // Update negotiated version for auto mode or set it if not already set
            if (_requestedHttpVersion == "auto" || _negotiatedHttpVersion == null)
            {
                _negotiatedHttpVersion = actualVersion;
            }

            // Validate HTTP version match if strict mode is enabled
            if (strictHttpVersion && _requestedHttpVersion != "auto")
            {
                if (actualVersion != _requestedHttpVersion)
                {
                    var requestedForDisplay = _requestedHttpVersion switch
                    {
                        "2" => "2",
                        "3" => "3",
                        "1.1" => "1.1",
                        "1.0" => "1.0",
                        _ => _requestedHttpVersion
                    };
                    throw new InvalidOperationException($"HTTP version mismatch: requested HTTP/{requestedForDisplay} but server negotiated HTTP/{actualVersion}. Use --http-version=auto or configure server to support HTTP/{requestedForDisplay}.");
                }
            }
        }
        catch (Exception ex) when (!(ex is InvalidOperationException))
        {
            throw new InvalidOperationException($"Client validation failed: Unable to connect to RavenDB server. {ex.Message}", ex);
        }
    }

    public void Dispose()
    {
        _http.Dispose();
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
}
