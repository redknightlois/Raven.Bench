using RavenBench.Workload;
using RavenBench.Metrics;
using RavenBench.Metrics.Snmp;
using RavenBench.Diagnostics;
using RavenBench.Util;

namespace RavenBench.Transport;

public readonly struct TransportResult(long bytesOut, long bytesIn, string? errorDetails = null)
{
    public long BytesOut { get; } = bytesOut;
    public long BytesIn { get; } = bytesIn;
    public string? ErrorDetails { get; } = errorDetails;
    public bool IsSuccess => ErrorDetails == null;
}

public readonly struct CalibrationResult(double ttfbMs, double totalMs, long bytesDown, Version httpVersion, bool isSuccess = true, string? errorDetails = null)
{
    public double TtfbMs { get; } = ttfbMs;
    public double TotalMs { get; } = totalMs;
    public long BytesDown { get; } = bytesDown;
    public Version HttpVersion { get; } = httpVersion;
    public bool IsSuccess { get; } = isSuccess;
    public string? ErrorDetails { get; } = errorDetails;
}

public interface ITransport : IDisposable
{
    Task<TransportResult> ExecuteAsync(Operation op, CancellationToken ct);
    Task PutAsync(string id, string json);

    Task<int?> GetServerMaxCoresAsync();
    Task<ServerMetrics> GetServerMetricsAsync();

    /// <summary>
    /// Returns SNMP metrics as a structured sample.
    /// </summary>
    Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions);

    Task<string> GetServerVersionAsync();
    Task<string> GetServerLicenseTypeAsync();
    Task ValidateClientAsync();
    Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default);

    /// <summary>
    /// Returns the endpoints this transport wants to calibrate during startup.
    /// </summary>
    IReadOnlyList<(string name, string path)> GetCalibrationEndpoints();
}

