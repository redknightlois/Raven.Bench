using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RavenBench.Util;

namespace RavenBench.Diagnostics;

/// <summary>
/// Holds the per-endpoint calibration data collected before the benchmark warmup runs.
/// </summary>
public sealed class StartupCalibration
{
    public required IReadOnlyList<EndpointCalibration> Endpoints { get; init; }
}

/// <summary>
/// Per-endpoint timing snapshot used to normalize observed latencies.
/// </summary>
public sealed class EndpointCalibration
{
    public required string Name { get; init; }
    public required string Url { get; init; }
    public required double TtfbMs { get; init; }
    public required double ObservedMs { get; init; }
    public required long BytesDown { get; init; }
    public required string HttpVersion { get; init; }
}



/// <summary>
/// Helper for calibrating HTTP endpoints using transport-specific execution.
/// Uses the transport's own calibration method to ensure accurate timing measurements.
/// </summary>
public static class EndpointCalibrator
{
    private const int DefaultAttempts = 32;
    private const double TargetDelayMeanMs = 150.0;
    private const double TargetDelayStdDevMs = 25.0;

    /// <summary>
    /// Executes calibration for the specified endpoints using a simple unified algorithm.
    /// Follows the common runner pattern: ask transport what endpoints it needs, then execute a simple algorithm.
    /// </summary>
    public static async Task<IReadOnlyList<EndpointCalibration>> CalibrateEndpointsAsync(
        Transport.ITransport transport,
        IReadOnlyList<(string name, string path)> endpoints,
        CancellationToken ct = default)
    {
        return await CalibrateEndpointsAsync(transport, endpoints, null, ct);
    }

    /// <summary>
    /// Executes calibration for the specified endpoints using a simple unified algorithm.
    /// Follows the common runner pattern: ask transport what endpoints it needs, then execute a simple algorithm.
    /// </summary>
    public static async Task<IReadOnlyList<EndpointCalibration>> CalibrateEndpointsAsync(
        Transport.ITransport transport,
        IReadOnlyList<(string name, string path)> endpoints,
        Action<double>? progressCallback,
        CancellationToken ct = default)
    {
        var results = new List<EndpointCalibration>();
        var totalOperations = endpoints.Count * DefaultAttempts;
        var completedOperations = 0;

        for (int endpointIndex = 0; endpointIndex < endpoints.Count; endpointIndex++)
        {
            var (name, path) = endpoints[endpointIndex];
            ct.ThrowIfCancellationRequested();
            var samples = new List<Transport.CalibrationResult>(DefaultAttempts);

            for (int i = 0; i < DefaultAttempts; i++)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
                    var sample = await transport.ExecuteCalibrationRequestAsync(path, ct).ConfigureAwait(false);
                    if (sample.IsSuccess)
                        samples.Add(sample);
                }
                catch
                {
                    // Ignore individual failures so a flaky endpoint does not break startup entirely
                }

                completedOperations++;
                progressCallback?.Invoke((double)completedOperations / totalOperations * 100.0);

                if (i + 1 < DefaultAttempts)
                {
                    await Task.Delay(GenerateRandomDelayMs(), ct).ConfigureAwait(false);
                }
            }

            if (samples.Count == 0)
                continue;

            // Calculate 5th percentile for TTFB
            samples.Sort(static (a, b) => a.TtfbMs.CompareTo(b.TtfbMs));
            var idx = Math.Clamp((int)Math.Floor(samples.Count * 0.05), 0, samples.Count - 1);
            var ttfbMs = samples[idx].TtfbMs;

            // Calculate 5th percentile for total time
            samples.Sort(static (a, b) => a.TotalMs.CompareTo(b.TotalMs));
            var ttlbSample = samples[idx];
            var ttlbMs = ttlbSample.TotalMs;
            var bytesDown = ttlbSample.BytesDown;
            var httpVersion = HttpHelper.FormatHttpVersion(ttlbSample.HttpVersion);

            results.Add(new EndpointCalibration
            {
                Name = name,
                Url = path,
                TtfbMs = ttfbMs,
                ObservedMs = ttlbMs,
                BytesDown = bytesDown,
                HttpVersion = httpVersion
            });
        }

        return results;
    }

    /// <summary>
    /// Generates a normally distributed delay in milliseconds around the target mean.
    /// We use random delays to:
    /// 1. Avoid creating predictable traffic patterns that might bias measurements
    /// 2. Better simulate real-world network timing variations
    /// 3. Ensure the calibration takes roughly 5 seconds total while varying individual delays
    /// 4. Prevent potential resonance effects with server-side periodic processes
    /// </summary>
    private static int GenerateRandomDelayMs()
    {
        // Box-Muller transform for normal distribution
        var u1 = Random.Shared.NextDouble();
        var u2 = Random.Shared.NextDouble();
        var z = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
        var normalValue = TargetDelayMeanMs + z * TargetDelayStdDevMs;

        // Clamp to reasonable bounds (20ms to 200ms)
        return Math.Clamp((int)Math.Round(normalValue), 20, 200);
    }
}



