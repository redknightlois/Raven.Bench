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
using System.Text.Json;
using System.Net;
using System.Net.Http;

namespace RavenBench.Transport;

/// <summary>
/// Transport implementation using the official RavenDB .NET client.
/// Provides high-level document operations through sessions for benchmarking.
/// </summary>
public sealed class RavenClientTransport : ITransport
{
    private readonly IDocumentStore _store;
    private readonly string _compressionMode;
    public string EffectiveCompressionMode => _compressionMode;
    public string EffectiveHttpVersion => "client-default";

    public RavenClientTransport(string url, string database, string compressionMode)
    {
        _compressionMode = compressionMode.ToLowerInvariant();
        
        _store = new DocumentStore
        {
            Urls = new[] { url },
            Database = database
        };

        ConfigureCompression();

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

    public async Task<TransportResult> ExecuteAsync(RavenBench.Workload.Operation op, CancellationToken ct)
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
                using (var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = true }))
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

    public async Task PutAsync(string id, string json)
    {
        // Use session to store raw JSON - simpler approach
        using var session = _store.OpenAsyncSession(new SessionOptions { NoTracking = true });
        var jsonObj = Newtonsoft.Json.JsonConvert.DeserializeObject(json);
        await session.StoreAsync(jsonObj, id).ConfigureAwait(false);
        await session.SaveChangesAsync().ConfigureAwait(false);
    }

    public async Task<ServerMetrics> GetServerMetricsAsync()
    {
        var baseUrl = _store.Urls[0].TrimEnd('/');
        return await RavenServerMetricsCollector.CollectAsync(baseUrl, _store.Database);
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
            using var http = new HttpClient();
            var url = _store.Urls[0].TrimEnd('/') + "/admin/license/status";
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
        catch
        {
            return "unknown";
        }
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

    public void Dispose()
    {
        _store.Dispose();
    }

    public async Task<int?> GetServerMaxCoresAsync()
    {
        try
        {
            // Use maintenance operation if exposed; fallback to HTTP endpoint via server URL.
            // We don’t depend on server admin permissions here; non-fatal if denied.
            using var http = new HttpClient();
            var url = _store.Urls[0].TrimEnd('/') + "/admin/license/status";
            using var resp = await http.GetAsync(url).ConfigureAwait(false);
            resp.EnsureSuccessStatusCode();
            var json = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
            var maxIdx = json.IndexOf("MaxCores\":");
            if (maxIdx > 0)
            {
                var remaining = json.Substring(maxIdx + 10);
                int i = 0; while (i < remaining.Length && !char.IsDigit(remaining[i])) i++;
                int j = i; while (j < remaining.Length && char.IsDigit(remaining[j])) j++;
                if (int.TryParse(remaining.Substring(i, j - i), out var cores))
                    return cores;
            }
        }
        catch { }
        return null;
    }

}
