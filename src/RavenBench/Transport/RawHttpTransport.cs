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
    public string EffectiveCompressionMode { get; }
    public string EffectiveHttpVersion { get; }

    public RawHttpTransport(string url, string database, string compressionMode, string httpVersion, string? endpoint = null)
    {
        _db = database;
        _baseUrl = url.TrimEnd('/');
        _acceptEncoding = compressionMode.Equals("identity", StringComparison.OrdinalIgnoreCase) ? "identity" : compressionMode;
        EffectiveCompressionMode = _acceptEncoding;
        _customEndpoint = string.IsNullOrWhiteSpace(endpoint) ? null : endpoint;

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
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
            MaxConnectionsPerServer = int.MaxValue, // Maximum connection pool
            PooledConnectionLifetime = TimeSpan.FromMinutes(15),
            EnableMultipleHttp2Connections = true
        };
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(30) };

        if (httpVersion == "2")
        {
            client.DefaultRequestVersion = HttpVersion.Version20;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
            EffectiveHttpVersion = "2";
        }
        else
        {
            client.DefaultRequestVersion = HttpVersion.Version11;
            client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
            EffectiveHttpVersion = "1.1";
        }

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
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();

        long bytesIn = 0;
        if (resp.Content.Headers.ContentLength.HasValue)
        {
            bytesIn = resp.Content.Headers.ContentLength.Value;
            // If gzip auto-decompressed, Content-Length is post-decompression; treat as payload bytes
        }
        else
        {
            await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            
            byte[] buffer;
            if (_bufferPool.TryDequeue(out buffer) == false)
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
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");
        using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        long bytesIn = 0;
        if (resp.Content.Headers.ContentLength.HasValue)
            bytesIn = resp.Content.Headers.ContentLength.Value;
        else
        {
            await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            
            byte[] buffer;
            if (_bufferPool.TryDequeue(out buffer) == false)
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
