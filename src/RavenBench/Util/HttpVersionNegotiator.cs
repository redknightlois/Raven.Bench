using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace RavenBench.Util;

/// <summary>
/// Probes server capabilities to determine the best HTTP version to use for the benchmark.
/// Negotiates HTTP version before transport creation to ensure compatibility.
/// </summary>
public static class HttpVersionNegotiator
{
    /// <summary>
    /// Negotiates the HTTP version to use by probing the server with the requested version.
    /// Returns the actual version that should be used for the benchmark.
    /// </summary>
    /// <param name="serverUrl">RavenDB server URL</param>
    /// <param name="requestedVersion">Requested HTTP version from command line (e.g., "auto", "2", "3", "1.1")</param>
    /// <param name="strictMode">Whether to enforce strict version matching</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>The negotiated HTTP version to use</returns>
    public static async Task<Version> NegotiateVersionAsync(
        string serverUrl,
        string requestedVersion,
        bool strictMode = false,
        CancellationToken ct = default)
    {
        var normalizedRequested = HttpHelper.NormalizeHttpVersion(requestedVersion);

        // For "auto" mode, probe server capabilities to find best version
        if (normalizedRequested == "auto")
        {
            return await ProbeForBestVersionAsync(serverUrl, ct).ConfigureAwait(false);
        }

        // For explicit versions, test if requested version works
        var testResult = await TestHttpVersionAsync(serverUrl, normalizedRequested, ct).ConfigureAwait(false);

        if (testResult.IsSuccess)
        {
            return testResult.NegotiatedVersion;
        }

        if (strictMode)
        {
            var displayVersion = normalizedRequested switch
            {
                "2" => "HTTP/2",
                "3" => "HTTP/3",
                "1.1" => "HTTP/1.1",
                "1.0" => "HTTP/1.0",
                _ => $"HTTP/{normalizedRequested}"
            };
            throw new InvalidOperationException(
                $"HTTP version negotiation failed: Server does not support {displayVersion}. " +
                $"Error: {testResult.ErrorMessage}. Use --http-version=auto or configure server to support {displayVersion}.");
        }

        // Non-strict mode: fallback to auto negotiation if requested version fails
        Console.WriteLine($"[Raven.Bench] Warning: Requested HTTP/{normalizedRequested} failed ({testResult.ErrorMessage}), falling back to auto-negotiation");
        return await ProbeForBestVersionAsync(serverUrl, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Probes server for the best available HTTP version, trying newer versions first.
    /// </summary>
    private static async Task<Version> ProbeForBestVersionAsync(string serverUrl, CancellationToken ct)
    {
        // Try versions in order of preference: HTTP/3 -> HTTP/2 -> HTTP/1.1
        var versionsToTry = new[] { "3", "2", "1.1" };

        foreach (var version in versionsToTry)
        {
            var result = await TestHttpVersionAsync(serverUrl, version, ct).ConfigureAwait(false);
            if (result.IsSuccess)
            {
                Console.WriteLine($"[Raven.Bench] HTTP version negotiated: HTTP/{HttpHelper.FormatHttpVersion(result.NegotiatedVersion)}");
                return result.NegotiatedVersion;
            }
        }

        // Fallback to HTTP/1.1 if all else fails
        Console.WriteLine("[Raven.Bench] Warning: All HTTP version probes failed, falling back to HTTP/1.1");
        return HttpVersion.Version11;
    }

    /// <summary>
    /// Tests if a specific HTTP version works with the server.
    /// </summary>
    private static async Task<NegotiationResult> TestHttpVersionAsync(
        string serverUrl,
        string httpVersion,
        CancellationToken ct)
    {
        try
        {
            var versionInfo = HttpHelper.GetRequestVersionInfo(httpVersion);
            var handler = HttpHelper.HttpVersionHandler.CreateConfiguredHandler();

            using var httpClient = new HttpClient(new HttpHelper.HttpVersionHandler(handler, versionInfo))
            {
                Timeout = TimeSpan.FromSeconds(10)
            };

            // Test with a lightweight endpoint
            var testUrl = $"{serverUrl.TrimEnd('/')}/build/version";
            using var request = new HttpRequestMessage(HttpMethod.Get, testUrl)
            {
                Version = versionInfo.version,
                VersionPolicy = versionInfo.policy
            };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cts.Token).ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
            {
                return new NegotiationResult(false, versionInfo.version, $"HTTP {(int)response.StatusCode}: {response.ReasonPhrase}");
            }

            return new NegotiationResult(true, response.Version, null);
        }
        catch (TaskCanceledException) when (ct.IsCancellationRequested)
        {
            return new NegotiationResult(false, HttpVersion.Version11, "Cancelled");
        }
        catch (TaskCanceledException)
        {
            return new NegotiationResult(false, HttpVersion.Version11, "Timeout");
        }
        catch (Exception ex)
        {
            return new NegotiationResult(false, HttpVersion.Version11, ex.Message);
        }
    }

    /// <summary>
    /// Result of HTTP version negotiation attempt.
    /// </summary>
    private readonly record struct NegotiationResult(bool IsSuccess, Version NegotiatedVersion, string? ErrorMessage);
}