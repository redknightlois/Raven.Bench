using System.Net;
using System.Net.Http;
using Raven.Client.Documents;

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

    /// <summary>
    /// Configures a DocumentStore with the specified HTTP version settings.
    /// Ensures administrative operations use the same HTTP protocol as the benchmarking transport.
    /// </summary>
    /// <param name="store">The DocumentStore to configure.</param>
    /// <param name="httpVersion">The HTTP version to use (e.g., "2" for HTTP/2, "1.1" for HTTP/1.1).</param>
    /// <param name="policy">The HTTP version policy (defaults to RequestVersionExact for strict protocol enforcement).</param>
    public static void ConfigureHttpVersion(DocumentStore store, Version httpVersion, HttpVersionPolicy policy = HttpVersionPolicy.RequestVersionExact)
    {
        // Configure HTTP client with proper connection pooling for all versions
        store.Conventions.CreateHttpClient = (handler) =>
        {
            ConfigureHandlerForHighConcurrency(handler);

            var client = httpVersion.Equals(HttpVersion.Version11) || httpVersion.Equals(HttpVersion.Version10)
                ? new HttpClient(handler)
                : new HttpClient(new HttpVersionHandler(handler, (httpVersion, policy)));

            client.Timeout = Timeout.InfiniteTimeSpan;
            return client;
        };
    }

    private static void ConfigureHandlerForHighConcurrency(HttpMessageHandler handler)
    {
        // RavenDB client typically passes an HttpClientHandler here.
        // For HTTP/1.1, low MaxConnectionsPerServer can cap throughput even on fast servers.
        var current = handler;
        while (true)
        {
            if (current is HttpClientHandler httpClientHandler)
            {
                httpClientHandler.MaxConnectionsPerServer = int.MaxValue;
                return;
            }

            if (current is SocketsHttpHandler socketsHttpHandler)
            {
                socketsHttpHandler.MaxConnectionsPerServer = int.MaxValue;
                return;
            }

            if (current is DelegatingHandler delegatingHandler && delegatingHandler.InnerHandler != null)
            {
                current = delegatingHandler.InnerHandler;
                continue;
            }

            return;
        }
    }

    /// <summary>
    /// Configures a DocumentStore with the specified HTTP version string.
    /// Ensures administrative operations use the same HTTP protocol as the benchmarking transport.
    /// </summary>
    /// <param name="store">The DocumentStore to configure.</param>
    /// <param name="httpVersionString">The HTTP version string (e.g., "2", "1.1", "auto").</param>
    public static void ConfigureHttpVersion(DocumentStore store, string httpVersionString)
    {
        if (string.IsNullOrEmpty(httpVersionString))
            return;

        var normalized = NormalizeHttpVersion(httpVersionString);
        var (version, policy) = GetRequestVersionInfo(normalized);
        ConfigureHttpVersion(store, version, policy);
    }
}
