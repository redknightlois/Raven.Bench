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
using System.Text.Json;
using System.Net;
using System.Net.Http;
using System.Collections.Generic;

namespace RavenBench.Transport;

/// <summary>
/// Transport implementation using the official RavenDB .NET client.
/// Provides high-level document operations through sessions for benchmarking.
/// </summary>
public sealed class RavenClientTransport : ITransport
{
    private readonly IDocumentStore _store;
    private readonly string _compressionMode;
    private readonly string _requestedHttpVersion;
    private readonly (Version version, HttpVersionPolicy policy) _httpVersionInfo;
    private string? _negotiatedHttpVersion;

    public string EffectiveCompressionMode => _compressionMode;
    public string EffectiveHttpVersion => _negotiatedHttpVersion ?? "unknown";


    public RavenClientTransport(string url, string database, string compressionMode, string httpVersion = "1.1")
    {
        _compressionMode = compressionMode.ToLowerInvariant();
        _requestedHttpVersion = HttpHelper.NormalizeHttpVersion(httpVersion);
        _httpVersionInfo = HttpHelper.GetRequestVersionInfo(_requestedHttpVersion);

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
        if (_requestedHttpVersion == "1.1" || _requestedHttpVersion == "1.0")
        {
            // Default HTTP/1.x configuration - no special handling needed
            return;
        }

        // Configure HTTP/2 and HTTP/3 through DocumentConventions.CreateHttpClient
        var conventions = _store.Conventions;
        conventions.CreateHttpClient = (handler) =>
        {
            // Use configured handler with HTTP/2 settings
            var configuredHandler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();
            var client = new HttpClient(new HttpHelper.HttpVersionHandler(configuredHandler, _httpVersionInfo))
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
                    // Use session to store raw JSON - simpler approach
                    using (var session = _store.OpenAsyncSession())
                    {
                        // Deserialize JSON into a dynamic object and store it
                        var jsonObj = Newtonsoft.Json.JsonConvert.DeserializeObject(op.Payload!);
                        await session.StoreAsync(jsonObj, op.Id, ct).ConfigureAwait(false);
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
        // Use session to store raw JSON - simpler approach
        using var session = _store.OpenAsyncSession();
        var jsonObj = Newtonsoft.Json.JsonConvert.DeserializeObject(json);
        await session.StoreAsync(jsonObj, id).ConfigureAwait(false);
        await session.SaveChangesAsync().ConfigureAwait(false);
    }

    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        var baseUrl = _store.Urls[0].TrimEnd('/');
        return await RavenServerMetricsCollector.CollectAsync(baseUrl, _store.Database, _requestedHttpVersion);
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
            using var http = new HttpClient(new HttpHelper.HttpVersionHandler(handler, _httpVersionInfo));

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

            // Set negotiated version based on request (we can't easily capture actual negotiated version from RavenDB client)
            if (_negotiatedHttpVersion == null)
            {
                _negotiatedHttpVersion = _requestedHttpVersion;
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed: Unable to connect to RavenDB server. {ex.Message}", ex);
        }
    }

    public async Task ValidateClientAsync(bool strictHttpVersion)
    {
        await ValidateClientAsync().ConfigureAwait(false);

        // Note: RavenDB client doesn't provide easy access to negotiated HTTP version
        // We assume the requested version was negotiated successfully
        if (strictHttpVersion && _requestedHttpVersion != "auto")
        {
            var requestedForDisplay = _requestedHttpVersion switch
            {
                "2" => "HTTP/2",
                "3" => "HTTP/3",
                "1.1" => "HTTP/1.1",
                "1.0" => "HTTP/1.0",
                _ => $"HTTP/{_requestedHttpVersion}"
            };
            Console.WriteLine($"[Raven.Bench] Client validation: Requested {requestedForDisplay} (client-managed)");
        }
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
            using var http = new HttpClient(new HttpHelper.HttpVersionHandler(handler, _httpVersionInfo));

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

}
