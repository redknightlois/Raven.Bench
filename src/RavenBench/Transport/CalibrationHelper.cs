using System;
using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace RavenBench.Transport;

/// <summary>
/// Shared calibration logic for measuring HTTP request performance across different transport implementations.
/// </summary>
public static class CalibrationHelper
{
    /// <summary>
    /// Executes a calibration request and measures timing metrics.
    /// </summary>
    /// <param name="sendRequestAsync">Function that sends the HTTP request and returns the response</param>
    /// <param name="ct">Cancellation token</param>
    /// <param name="fallbackHttpVersion">HTTP version to use in error responses (should match requested version from command-line)</param>
    /// <returns>Calibration result with timing and transfer metrics</returns>
    public static async Task<CalibrationResult> ExecuteCalibrationAsync(
        Func<CancellationToken, Task<HttpResponseMessage>> sendRequestAsync,
        CancellationToken ct = default,
        Version? fallbackHttpVersion = null)
    {
        // Use provided fallback version that should respect command-line HTTP version requirements
        // If none provided, default to HTTP/1.1 as last resort
        var httpVersionFallback = fallbackHttpVersion ?? HttpVersion.Version11;

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            var sw = Stopwatch.StartNew();
            using var resp = await sendRequestAsync(cts.Token).ConfigureAwait(false);

            if (resp.IsSuccessStatusCode == false)
            {
                return new CalibrationResult(0, 0, 0, httpVersionFallback, false, $"HTTP {(int)resp.StatusCode}");
            }

            var ttfbMs = sw.Elapsed.TotalMilliseconds;

            long bytesIn = await MeasureResponseSizeAsync(resp, ct).ConfigureAwait(false);

            var totalMs = sw.Elapsed.TotalMilliseconds;
            return new CalibrationResult(ttfbMs, totalMs, bytesIn, resp.Version);
        }
        catch (TaskCanceledException)
        {
            if (ct.IsCancellationRequested)
                return new CalibrationResult(0, 0, 0, httpVersionFallback, false, "Cancelled");
            return new CalibrationResult(0, 0, 0, httpVersionFallback, false, "Timeout");
        }
        catch (Exception ex)
        {
            return new CalibrationResult(0, 0, 0, httpVersionFallback, false, ex.Message);
        }
    }

    /// <summary>
    /// Measures response content size, handling both Content-Length header and stream reading.
    /// </summary>
    private static async Task<long> MeasureResponseSizeAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.Content.Headers.ContentLength.HasValue)
        {
            return response.Content.Headers.ContentLength.Value;
        }

        await using var stream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        var buffer = new byte[64 * 1024];

        int totalRead = 0;
        int bytesRead;
        while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct).ConfigureAwait(false)) > 0)
        {
            totalRead += bytesRead;
        }
        return totalRead;
    }
}