using System.Globalization;

namespace RavenBench.Core.Metrics;

/// <summary>A node_exporter scrape that failed or lacked a series the harness reads.</summary>
public sealed class NodeExporterException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>
/// The node_exporter series one scrape carries: per-CPU, per-mode seconds from
/// node_cpu_seconds_total, the two memory gauges, and the bytes written to disk summed over every
/// device node_exporter reports, null when the host exposes no disk series.
/// </summary>
public sealed record NodeExporterSample(
    IReadOnlyDictionary<string, double> CpuSeconds,
    double MemTotalBytes,
    double MemAvailableBytes,
    double? DiskWrittenBytes = null)
{
    public const string CpuSeries = "node_cpu_seconds_total";
    public const string MemTotalSeries = "node_memory_MemTotal_bytes";
    public const string MemAvailableSeries = "node_memory_MemAvailable_bytes";
    public const string DiskWrittenSeries = "node_disk_written_bytes_total";

    /// <summary>
    /// Parses the Prometheus text exposition format. Keys of <see cref="CpuSeries"/> are the raw
    /// label set, so each cpu and mode pair is one counter. Throws when a series is missing.
    /// </summary>
    public static NodeExporterSample Parse(string text)
    {
        var cpu = new Dictionary<string, double>(StringComparer.Ordinal);
        double? memTotal = null, memAvailable = null, diskWritten = null;

        foreach (var rawLine in text.Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line[0] == '#')
                continue;

            var braceOpen = line.IndexOf('{');
            var braceClose = braceOpen < 0 ? -1 : line.IndexOf('}', braceOpen);
            var nameEnd = braceOpen >= 0 ? braceOpen : line.IndexOf(' ');
            if (nameEnd <= 0)
                continue;
            var name = line[..nameEnd];
            var labels = braceOpen >= 0 && braceClose > braceOpen ? line[(braceOpen + 1)..braceClose] : "";
            var rest = (braceClose >= 0 ? line[(braceClose + 1)..] : line[nameEnd..]).Trim();
            // A sample may carry a trailing timestamp; the value is the first field.
            var valueText = rest.Split(' ', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();

            if (name != CpuSeries && name != MemTotalSeries && name != MemAvailableSeries && name != DiskWrittenSeries)
                continue;
            if (valueText == null || double.TryParse(valueText, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) == false)
                throw new NodeExporterException($"node_exporter: series {name} has an unreadable value '{valueText}'.");

            switch (name)
            {
                case CpuSeries: cpu[labels] = value; break;
                case MemTotalSeries: memTotal = value; break;
                case MemAvailableSeries: memAvailable = value; break;
                case DiskWrittenSeries: diskWritten = (diskWritten ?? 0) + value; break;
            }
        }

        if (cpu.Count == 0)
            throw new NodeExporterException($"node_exporter: series {CpuSeries} missing.");
        if (cpu.Keys.Any(IsIdle) == false)
            throw new NodeExporterException($"node_exporter: series {CpuSeries} has no mode=\"idle\" sample.");
        if (memTotal == null)
            throw new NodeExporterException($"node_exporter: series {MemTotalSeries} missing.");
        if (memAvailable == null)
            throw new NodeExporterException($"node_exporter: series {MemAvailableSeries} missing.");

        return new NodeExporterSample(cpu, memTotal.Value, memAvailable.Value, diskWritten);
    }

    internal static bool IsIdle(string labels) => labels.Contains("mode=\"idle\"", StringComparison.Ordinal);

    /// <summary>Used memory in MiB: MemTotal minus MemAvailable.</summary>
    public long UsedMemoryMB => (long)((MemTotalBytes - MemAvailableBytes) / (1024 * 1024));

    /// <summary>
    /// The host-wide non-idle share of CPU time between two scrapes, over all CPUs, 0 to 100.
    /// Every mode except idle counts as busy, iowait and steal included. Throws when a counter went backwards or no CPU time elapsed, so a reading is never zero by default.
    /// </summary>
    public static double CpuPercent(NodeExporterSample start, NodeExporterSample end)
    {
        double total = 0, idle = 0;
        foreach (var (labels, endValue) in end.CpuSeconds)
        {
            if (start.CpuSeconds.TryGetValue(labels, out var startValue) == false)
                throw new NodeExporterException($"node_exporter: series {CpuSeries}{{{labels}}} is absent from the start scrape.");
            var delta = endValue - startValue;
            if (delta < 0)
                throw new NodeExporterException($"node_exporter: counter {CpuSeries}{{{labels}}} reset between scrapes.");
            total += delta;
            if (IsIdle(labels))
                idle += delta;
        }

        if (total <= 0)
            throw new NodeExporterException($"node_exporter: no CPU time elapsed in {CpuSeries} between scrapes.");
        return (total - idle) / total * 100.0;
    }
}

/// <summary>Host-wide server CPU and memory over one measurement window, or the reason the window has none.</summary>
public sealed record NodeExporterWindow(double? CpuPercent, long? MemoryMB, string? Unavailable);

/// <summary>Scrapes an operator-supplied node_exporter metrics endpoint.</summary>
public sealed class NodeExporterClient : IDisposable
{
    public const string SourceName = "node_exporter";

    private readonly HttpClient _http;

    public Uri Endpoint { get; }

    public NodeExporterClient(Uri endpoint, HttpMessageHandler? handler = null)
    {
        Endpoint = endpoint;
        _http = handler == null ? new HttpClient() : new HttpClient(handler);
        _http.Timeout = TimeSpan.FromSeconds(10);
    }

    public async Task<NodeExporterSample> ScrapeAsync(CancellationToken cancellationToken = default)
    {
        string text;
        try
        {
            using var response = await _http.GetAsync(Endpoint, cancellationToken).ConfigureAwait(false);
            if (response.IsSuccessStatusCode == false)
                throw new NodeExporterException($"node_exporter: {Endpoint} answered HTTP {(int)response.StatusCode}.");
            text = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            throw new NodeExporterException($"node_exporter: {Endpoint} did not answer: {ex.Message}", ex);
        }
        return NodeExporterSample.Parse(text);
    }

    /// <summary>
    /// Closes a window opened by <paramref name="startScrape"/> with a second scrape. A failed scrape gives a window
    /// that states the reason, so a mid-run fault never cuts the measurement short.
    /// </summary>
    public async Task<NodeExporterWindow> EndWindowAsync(Task<NodeExporterSample> startScrape, CancellationToken cancellationToken = default)
    {
        try
        {
            var start = await startScrape.ConfigureAwait(false);
            var end = await ScrapeAsync(cancellationToken).ConfigureAwait(false);
            return new NodeExporterWindow(NodeExporterSample.CpuPercent(start, end), end.UsedMemoryMB, null);
        }
        catch (NodeExporterException ex)
        {
            Console.Error.WriteLine($"[Raven.Bench] WARNING: {ex.Message}");
            return new NodeExporterWindow(null, null, ex.Message);
        }
    }

    /// <summary>
    /// Opens the configured endpoint and scrapes it once, so an endpoint the operator asked for that
    /// does not answer fails the run before any load. Null when no endpoint is configured.
    /// </summary>
    public static async Task<NodeExporterClient?> ConnectAsync(Uri? endpoint)
    {
        if (endpoint == null)
            return null;
        var client = new NodeExporterClient(endpoint);
        try
        {
            await client.ScrapeAsync().ConfigureAwait(false);
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public void Dispose() => _http.Dispose();
}
