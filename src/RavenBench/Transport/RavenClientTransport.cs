using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Sparrow.Json;
using RavenBench.Workload;
using RavenBench.Metrics;
using System.Text.Json;

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

    public RavenClientTransport(string url, string database, string compressionMode)
    {
        _compressionMode = compressionMode.ToLowerInvariant();
        _store = new DocumentStore
        {
            Urls = new[] { url },
            Database = database,
            Conventions =
            {
                // We keep defaults; RavenDB 7.1 client typically uses zstd
                // If identity requested, attempt to disable compression if available
            }
        }.Initialize();


        if (_compressionMode == "identity")
        {
            try
            {
                // Some 7.x versions expose a Conventions.UseCompression flag; if not, ignore.
                var prop = _store.Conventions.GetType().GetProperty("UseCompression");
                if (prop != null && prop.CanWrite)
                    prop.SetValue(_store.Conventions, false);
            }
            catch { /* best effort */ }
        }
    }

    public async Task<TransportResult> ExecuteAsync(Operation op, CancellationToken ct)
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
                var span = json.AsSpan(maxIdx + 10);
                int i = 0; while (i < span.Length && !char.IsDigit(span[i])) i++;
                int j = i; while (j < span.Length && char.IsDigit(span[j])) j++;
                if (int.TryParse(span[i..j], out var cores))
                    return cores;
            }
        }
        catch { }
        return null;
    }

}

