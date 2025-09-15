using RavenBench.Workload;
using RavenBench.Metrics;

namespace RavenBench.Transport;

public readonly struct TransportResult(long bytesOut, long bytesIn, string? errorDetails = null)
{
    public long BytesOut { get; } = bytesOut;
    public long BytesIn { get; } = bytesIn;
    public string? ErrorDetails { get; } = errorDetails;
    public bool IsSuccess => ErrorDetails == null;
}

public interface ITransport : IDisposable
{
    Task<TransportResult> ExecuteAsync(Operation op, CancellationToken ct);
    Task PutAsync(string id, string json);

    Task<int?> GetServerMaxCoresAsync();
    Task<ServerMetrics> GetServerMetricsAsync();
    Task<string> GetServerVersionAsync();
    Task<string> GetServerLicenseTypeAsync();
    Task ValidateClientAsync();
}

