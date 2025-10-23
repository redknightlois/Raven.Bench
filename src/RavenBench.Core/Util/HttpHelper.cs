using System.Net;
using System.Net.Http;

namespace RavenBench.Core;

/// <summary>
/// Shared utilities for HTTP version handling across transport implementations.
/// Provides consistent HTTP version normalization, formatting, and configuration.
/// </summary>
public static class HttpHelper
{
    /// <summary>
    /// Formats a .NET Version object to a standard HTTP version string.
    /// </summary>
    public static string FormatHttpVersion(Version version) => (version.Major, version.Minor) switch
    {
        (1, 0) => "1.0",
        (1, 1) => "1.1",
        (2, 0) => "2",
        (3, 0) => "3",
        _ => version.ToString()
    };

    /// <summary>
    /// Parses a standard HTTP version string back to a .NET Version object.
    /// </summary>
    public static Version ParseHttpVersion(string httpVersionString) => httpVersionString switch
    {
        "1.0" => HttpVersion.Version10,
        "1.1" => HttpVersion.Version11,
        "2" => HttpVersion.Version20,
        "3" => HttpVersion.Version30,
        _ => HttpVersion.Version11 // Fallback to HTTP/1.1
    };

    /// <summary>
    /// Normalizes various HTTP version string formats to a standard format.
    /// Combines patterns from both RawHttpTransport and RavenClientTransport.
    /// </summary>
    public static string NormalizeHttpVersion(string httpVersion) => httpVersion.ToLowerInvariant() switch
    {
        // HTTP/1.x variants
        "http1" or "http/1" or "1" => "1.1",
        "http1.1" or "http/1.1" or "1.1" => "1.1",
        "http1.0" or "http/1.0" or "1.0" => "1.0",

        // HTTP/2 variants
        "http2" or "http/2" or "2" or "2.0" => "2",

        // HTTP/3 variants
        "http3" or "http/3" or "3" or "3.0" => "3",

        // Special values
        "auto" => "auto",

        // Pass through other values
        _ => httpVersion.ToLowerInvariant()
    };

    /// <summary>
    /// Gets the appropriate HTTP version and policy for a given requested version string.
    /// </summary>
    public static (Version version, HttpVersionPolicy policy) GetRequestVersionInfo(string requestedHttpVersion) => requestedHttpVersion switch
    {
        "2" => (HttpVersion.Version20, HttpVersionPolicy.RequestVersionExact),
        "3" => (HttpVersion.Version30, HttpVersionPolicy.RequestVersionExact),
        "auto" => (HttpVersion.Version30, HttpVersionPolicy.RequestVersionOrLower),
        "1.1" => (HttpVersion.Version11, HttpVersionPolicy.RequestVersionExact),
        "1.0" => (HttpVersion.Version10, HttpVersionPolicy.RequestVersionExact),
        _ => (HttpVersion.Version11, HttpVersionPolicy.RequestVersionExact)
    };

    /// <summary>
    /// Custom HttpMessageHandler that sets HTTP version and policy on each request for HTTP/2 h2c support.
    /// Can be used by both RawHttpTransport and RavenClientTransport.
    /// </summary>
    public class HttpVersionHandler : DelegatingHandler
    {
        private readonly (Version version, HttpVersionPolicy policy) _versionInfo;

        public HttpVersionHandler(HttpMessageHandler innerHandler, (Version version, HttpVersionPolicy policy) versionInfo)
            : base(innerHandler)
        {
            _versionInfo = versionInfo;
        }

        public static SocketsHttpHandler CreateConfiguredHandler()
        {
            var handler = new SocketsHttpHandler
            {
                // Raise per-stream receive window (range: 65_535 .. 16_777_216 by default)
                InitialHttp2StreamWindowSize = 16 * 1024 * 1024,

                // Let handler open >1 HTTP/2 connection if stream limits are reached
                EnableMultipleHttp2Connections = true,

                PooledConnectionLifetime = TimeSpan.FromMinutes(10),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(1),
                MaxConnectionsPerServer = int.MaxValue,
                UseCookies = false,
                AllowAutoRedirect = false,
                KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(30),

                // Cross-platform settings (replaces ServicePointManager which only works on Windows)
                ConnectTimeout = TimeSpan.FromSeconds(30),
                ResponseDrainTimeout = TimeSpan.FromSeconds(10),
                Expect100ContinueTimeout = TimeSpan.Zero // Disable Expect 100-Continue
            };

            return handler;
        }


        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // Set HTTP version and policy for proper HTTP/2 h2c support
            request.Version = _versionInfo.version;
            request.VersionPolicy = _versionInfo.policy;
            return base.SendAsync(request, cancellationToken);
        }
    }
}