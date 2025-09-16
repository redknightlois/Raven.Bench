using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;
using RavenBench.Workload;
using RavenBench.Metrics;
using RavenBench.Util;
using RavenBench.Diagnostics;
using System.Text.Json;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Collections.Generic;
using System.Diagnostics;

namespace RavenBench.Transport;

/// <summary>
/// Transport implementation using the official RavenDB .NET client.
/// Provides high-level document operations through sessions for benchmarking.
/// </summary>
public sealed class RavenClientTransport : ITransport
{
    private readonly IDocumentStore _store;
    private readonly string _compressionMode;
    private readonly Version _httpVersion;

    public string EffectiveCompressionMode => _compressionMode;
    public string EffectiveHttpVersion => HttpHelper.FormatHttpVersion(_httpVersion);


    public RavenClientTransport(string url, string database, string compressionMode, Version httpVersion)
    {
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
        if (_httpVersion.Equals(HttpVersion.Version11) || _httpVersion.Equals(HttpVersion.Version10))
        {
            // Default HTTP/1.x configuration - no special handling needed
            return;
        }

        // Configure HTTP/2 and HTTP/3 through DocumentConventions.CreateHttpClient
        var conventions = _store.Conventions;
        conventions.CreateHttpClient = (handler) =>
        {
            var configuredHandler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
            var httpVersionInfo = (_httpVersion, HttpVersionPolicy.RequestVersionExact);
            var client = new HttpClient(new HttpHelper.HttpVersionHandler(configuredHandler, httpVersionInfo))
            {
                Timeout = Timeout.InfiniteTimeSpan
            };
            return client;
        };
    }


    public async Task<TransportResult> ExecuteAsync(Workload.Operation op, CancellationToken ct)
    {
        try
        {
            switch (op.Type)
            {
                case OperationType.ReadById:
                    using (var s = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
                    {
                        var _ = await s.LoadAsync<object>(op.Id, ct).ConfigureAwait(false);
                    }
                    // bytes unknown; estimate ~doc size
                    return new TransportResult(300, 4096);
                case OperationType.Insert:
                case OperationType.Update:
                    // Store raw JSON to maintain consistency with RawHttpTransport
                    using (var session = _store.OpenAsyncSession())
                    {
                        // Store as raw JSON document to match RawHttpTransport behavior
                        var doc = new { Json = op.Payload };
                        await session.StoreAsync(doc, op.Id, ct).ConfigureAwait(false);
                        await session.SaveChangesAsync(ct).ConfigureAwait(false);
                    }
                    var outBytes = op.Payload?.Length ?? 0;
                    return new TransportResult(outBytes + 300, 256);
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

    public async Task PutAsync(string id, string json)
    {
        // Store raw JSON to maintain consistency with RawHttpTransport
        using var session = _store.OpenAsyncSession();
        var doc = new { Json = json };
        await session.StoreAsync(doc, id).ConfigureAwait(false);
        await session.SaveChangesAsync().ConfigureAwait(false);
    }

    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        var baseUrl = _store.Urls[0].TrimEnd('/');
        return await RavenServerMetricsCollector.CollectAsync(baseUrl, _store.Database, HttpHelper.FormatHttpVersion(_httpVersion));
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
            // Create HTTP client with same configuration as the store
            var handler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
            var httpVersionInfo = (_httpVersion, HttpVersionPolicy.RequestVersionExact);
            using var http = new HttpClient(new HttpHelper.HttpVersionHandler(handler, httpVersionInfo));

            var url = _store.Urls[0].TrimEnd('/') + "/license/status";
            using var resp = await http.GetAsync(url).ConfigureAwait(false);
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

    public void Dispose()
    {
        _store.Dispose();
    }

    public async Task<int?> GetServerMaxCoresAsync()
    {
        try
        {
            // Use maintenance operation if exposed; fallback to HTTP endpoint via server URL.
            // We don't depend on server admin permissions here; non-fatal if denied.
            // Create HTTP client with same configuration as the store
            var handler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
            var httpVersionInfo = (_httpVersion, HttpVersionPolicy.RequestVersionExact);
            using var http = new HttpClient(new HttpHelper.HttpVersionHandler(handler, httpVersionInfo));

            var url = _store.Urls[0].TrimEnd('/') + "/license/status";
            using var resp = await http.GetAsync(url).ConfigureAwait(false);
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
            // Prepare endpoint path (ensure it starts with /)
            if (!endpoint.StartsWith('/'))
                endpoint = "/" + endpoint;

            // Create HTTP client with same configuration as the store
            var handler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
            var httpVersionInfo = (_httpVersion, HttpVersionPolicy.RequestVersionExact);
            using var http = new HttpClient(new HttpHelper.HttpVersionHandler(handler, httpVersionInfo))
            {
                BaseAddress = new Uri(_store.Urls[0])
            };

            using var req = new HttpRequestMessage(HttpMethod.Get, endpoint);
            req.Version = _httpVersion;
            req.VersionPolicy = HttpVersionPolicy.RequestVersionExact;
            req.Headers.ExpectContinue = false;
            req.Headers.ConnectionClose = false;

            var acceptEncoding = ExtractAcceptEncoding(_compressionMode);
            if (!string.IsNullOrWhiteSpace(acceptEncoding))
            {
                req.Headers.AcceptEncoding.Clear();
                req.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue(acceptEncoding));
            }

            return await http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }, ct, fallbackHttpVersion: _httpVersion).ConfigureAwait(false);
    }

    private string BuildCalibrationUrl(string endpoint)
    {
        // Check if endpoint is truly absolute (has a scheme like http:// or https://)
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) &&
            uri.Scheme != null && (uri.Scheme == "http" || uri.Scheme == "https"))
            return endpoint;

        if (!endpoint.StartsWith('/'))
            endpoint = "/" + endpoint;

        return _store.Urls[0].TrimEnd('/') + endpoint;
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

}
