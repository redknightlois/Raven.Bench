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
public readonly struct TransportResult(long bytesOut, long bytesIn, string? errorDetails = null, string? indexName = null, int? resultCount = null, bool? isStale = null, double? queryDurationMs = null)
{
    public long BytesOut { get; } = bytesOut;
    public long BytesIn { get; } = bytesIn;
    public string? ErrorDetails { get; } = errorDetails;
    public bool IsSuccess => ErrorDetails == null;

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

    /// <summary>
    /// Query execution duration in milliseconds as reported by RavenDB (populated for query operations).
    /// This is the server-side query execution time, not the full round-trip time.
    /// </summary>
    public double? QueryDurationMs { get; } = queryDurationMs;
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
    Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct);
    Task PutAsync<T>(string id, T document);
   Task EnsureDatabaseExistsAsync(string databaseName);
   Task<long> GetDocumentCountAsync(string idPrefix);

   Task<int?> GetServerMaxCoresAsync();
    Task<ServerMetrics> GetServerMetricsAsync();

    /// <summary>
    /// Returns SNMP metrics as a structured sample.
    /// When databaseName is provided, queries database-specific OIDs; otherwise queries server-wide OIDs.
    /// </summary>
    Task<SnmpSample> GetSnmpMetricsAsync(SnmpOptions snmpOptions, string? databaseName = null);

    Task<string> GetServerVersionAsync();
    Task<string> GetServerLicenseTypeAsync();
    Task ValidateClientAsync();
    Task<CalibrationResult> ExecuteCalibrationRequestAsync(string endpoint, CancellationToken ct = default);

    /// <summary>
    /// Samples actual document IDs from the database for warmup.
    /// Returns a list of document IDs that exist in the database with the given prefix.
    /// </summary>
    /// <param name="idPrefix">ID prefix to sample from (e.g., 'bench/').</param>
    /// <param name="count">Number of IDs to sample.</param>
    /// <param name="seed">Random seed for reproducible sampling.</param>
    Task<List<string>> SampleDocumentIdsAsync(string idPrefix, int count, int seed);

    /// <summary>
    /// Returns the endpoints this transport wants to calibrate during startup.
    /// </summary>
    IReadOnlyList<(string name, string path)> GetCalibrationEndpoints();
}
