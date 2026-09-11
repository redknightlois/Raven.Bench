using RavenBench.Core.Workload;
using RavenBench.Core.Metrics;
using RavenBench.Core.Metrics.Snmp;
using RavenBench.Core.Diagnostics;
using RavenBench.Core;

namespace RavenBench.Core.Transport;

/// <summary>
/// Represents the result of a transport operation execution.
/// Includes byte counts, error details, and optional query-specific metadata.
/// </summary>
public readonly struct TransportResult(long bytesOut, long bytesIn, string? errorDetails = null, string? indexName = null, int? resultCount = null, bool? isStale = null)
{
    public long BytesOut { get; } = bytesOut;
    public long BytesIn { get; } = bytesIn;
    public string? ErrorDetails { get; } = errorDetails;
    public bool IsSuccess => ErrorDetails == null;

    /// <summary>
    /// True when the operation was aborted by external cancellation (end of run).
    /// Cancelled results must not be recorded as successes, errors, or latency samples.
    /// </summary>
    public bool Cancelled { get; init; }

    public static TransportResult CancelledResult { get; } = new TransportResult(0, 0) { Cancelled = true };

    /// <summary>
    /// Maps a failure raised out of a transport's ExecuteAsync to a result. External cancellation
    /// yields <see cref="CancelledResult"/>, which callers must not count as an error.
    /// </summary>
    internal static TransportResult FromException(Exception ex, CancellationToken ct) => ex switch
    {
        TaskCanceledException when ct.IsCancellationRequested => CancelledResult,
        TaskCanceledException => new TransportResult(0, 0, "Operation timed out"),
        HttpRequestException httpEx => new TransportResult(0, 0, $"HTTP {httpEx.Data["StatusCode"] ?? "Error"}: {httpEx.Message}"),
        _ => new TransportResult(0, 0, ex.Message)
    };

    /// <summary>
    /// Index name used by the query (populated for query operations).
    /// </summary>
    public string? IndexName { get; } = indexName;

    /// <summary>
    /// Number of results returned (populated for query operations).
    /// </summary>
    public int? ResultCount { get; } = resultCount;

    /// <summary>
    /// Whether the index was stale at query time (populated for query operations).
    /// </summary>
    public bool? IsStale { get; } = isStale;
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

/// <summary>
/// Everything a ycsb-style workload needs from a product: execute a typed operation, load a
/// document outside the measured path, prepare the target database or collection, count the
/// seeded documents, and identify the product and its version. A product with no notion of
/// SNMP, of a license or of RavenDB calibration implements this alone.
/// </summary>
public interface IYcsbTransport : IDisposable
{
    /// <summary>
    /// The product this transport drives (for example "RavenDB"), as the result row names the target.
    /// </summary>
    string ProductName { get; }

    /// <summary>
    /// True when reported byte counts are actual on-the-wire sizes. False when they are estimated
    /// (e.g. the client library hides the socket, or transparent decompression obscures wire size),
    /// in which case network-utilization figures derived from them must not be trusted.
    /// </summary>
    bool ReportsWireBytes { get; }

    Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct);

    /// <summary>
    /// Writes one document outside the measured path; used to fill the keyspace before a run.
    /// </summary>
    Task PutAsync<T>(string id, T document);

    Task EnsureDatabaseExistsAsync(string databaseName);
    Task<long> GetDocumentCountAsync(string idPrefix);
    Task<string> GetServerVersionAsync();
}

/// <summary>
/// The RavenDB capability set: the ycsb contract plus the server metrics, SNMP, license and
/// calibration surface that only RavenDB exposes.
/// </summary>
public interface ITransport : IYcsbTransport
{
    Task<int?> GetServerMaxCoresAsync();
    Task<ServerMetrics> GetServerMetricsAsync();

    /// <summary>
    /// Returns SNMP metrics as a structured sample.
    /// When databaseName is provided, queries database-specific OIDs; otherwise queries server-wide OIDs.
    /// </summary>
    Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null);

    Task<string> GetServerLicenseTypeAsync();
    Task ValidateClientAsync();
    Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default);

    /// <summary>
    /// Returns the endpoints this transport wants to calibrate during startup.
    /// </summary>
    IReadOnlyList<(string name, string path)> GetCalibrationEndpoints();
}

