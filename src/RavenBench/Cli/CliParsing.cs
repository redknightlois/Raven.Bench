using System.Globalization;
using RavenBench.Util;
using System;

namespace RavenBench.Cli;

#nullable disable
internal static class CliParsing
{
    public static RunOptions ToRunOptions(this RunSettings s)
    {
        var (concurrencyStart, concurrencyEnd, concurrencyFactor) = ParseConcurrency(s.Concurrency);
        var (kneeThroughputDelta, kneeP95Delta) = ParseKneeRule(s.KneeRule);

        return new RunOptions
        {
            Url = RequiredString(s.Url!, "--url"),
            Database = RequiredString(s.Database!, "--database"),
            Reads = ParseNullableWeight(s.Reads),
            Writes = ParseNullableWeight(s.Writes),
            Updates = ParseNullableWeight(s.Updates),
            DocumentSizeBytes = ParseSize(s.DocSize),
            ConcurrencyStart = concurrencyStart,
            ConcurrencyEnd = concurrencyEnd,
            ConcurrencyFactor = concurrencyFactor,
            Warmup = ParseDuration(s.Warmup),
            Duration = ParseDuration(s.Duration),
            MaxErrorRate = ParsePercent(s.MaxErrors),
            KneeThroughputDelta = kneeThroughputDelta,
            KneeP95Delta = kneeP95Delta,
            ThreadPoolWorkers = s.TpWorkers,
            ThreadPoolIOCP = s.TpIOCP,
            Distribution = s.Distribution,
            Transport = s.Transport,
            Compression = s.Compression,
            Mode = s.Mode,
            OutJson = s.OutJson,
            OutCsv = s.OutCsv,
            Seed = s.Seed,
            Preload = s.Preload,
            RawEndpoint = s.RawEndpoint,
            Notes = s.Notes,
            ExpectedCores = s.ExpectedCores,
            NetworkLimitedMode = s.NetworkLimitedMode,
            LinkMbps = s.LinkMbps,
            HttpVersion = s.HttpVersion,
            StrictHttpVersion = s.StrictHttpVersion,
            Verbose = s.Verbose,
            SnmpEnabled = s.SnmpEnabled,
            SnmpPort = s.SnmpEnabled ? s.SnmpPort : 161,
            SnmpPollInterval = s.SnmpEnabled ? ParseDuration(s.SnmpInterval ?? "5s") : TimeSpan.FromSeconds(5),
            LatencyDisplay = ParseLatencyDisplayType(s.Latencies)
        };
    }

    private static string RequiredString(string value, string paramName)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new ArgumentException($"{paramName} is required");
        }
        return value;
    }

    private static double ParseNullableWeight(string s) => s is not null ? ParseWeight(s) : 0.0;

    public static (int, int, double) ParseConcurrency(string s)
    {
        var parts = s.Split("..", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length != 2) throw new ArgumentException("Invalid --concurrency format");
        var start = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var right = parts[1];
        var x = right.IndexOf('x');
        var end = x >= 0 ? int.Parse(right.Substring(0, x), CultureInfo.InvariantCulture) : int.Parse(right, CultureInfo.InvariantCulture);
        var factor = x >= 0 ? double.Parse(right.Substring(x + 1), CultureInfo.InvariantCulture) : 2.0;
        return (start, end, factor);
    }

    public static (double, double) ParseKneeRule(string s)
    {
        var parts = s.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        double dthr = 0.05, dp95 = 0.20;
        foreach (var p in parts)
        {
            if (p.StartsWith("dthr=")) dthr = ParsePercent(p.Substring(5));
            else if (p.StartsWith("dp95=")) dp95 = ParsePercent(p.Substring(5));
        }
        return (dthr, dp95);
    }

    public static int ParseSize(string s)
    {
        s = s.Trim().ToUpperInvariant();
        if (s.EndsWith("KB")) return int.Parse(s.AsSpan(0, s.Length - 2)) * 1024;
        if (s.EndsWith("MB")) return int.Parse(s.AsSpan(0, s.Length - 2)) * 1024 * 1024;
        if (s.EndsWith("B")) return int.Parse(s.AsSpan(0, s.Length - 1));
        return int.Parse(s);
    }

    public static TimeSpan ParseDuration(string s)
    {
        s = s.Trim().ToLowerInvariant();
        if (s.EndsWith("ms")) return TimeSpan.FromMilliseconds(double.Parse(s[..^2], CultureInfo.InvariantCulture));
        if (s.EndsWith("s")) return TimeSpan.FromSeconds(double.Parse(s[..^1], CultureInfo.InvariantCulture));
        if (s.EndsWith("m")) return TimeSpan.FromMinutes(double.Parse(s[..^1], CultureInfo.InvariantCulture));
        return TimeSpan.FromSeconds(double.Parse(s, CultureInfo.InvariantCulture));
    }

    public static double ParsePercent(string s)
    {
        s = s.Trim();
        if (s.EndsWith("%"))
            return double.Parse(s.AsSpan(0, s.Length - 1), CultureInfo.InvariantCulture) / 100.0;
        return double.Parse(s, CultureInfo.InvariantCulture);
    }

    public static double ParseWeight(string s)
    {
        s = s.Trim();
        if (s.EndsWith("%"))
            return double.Parse(s.AsSpan(0, s.Length - 1), CultureInfo.InvariantCulture);
        return double.Parse(s, CultureInfo.InvariantCulture);
    }

    private static LatencyDisplayType ParseLatencyDisplayType(string latencies)
    {
        return latencies.ToLowerInvariant() switch
        {
            "normalized" => LatencyDisplayType.Normalized,
            "raw" => LatencyDisplayType.Raw,
            "both" => LatencyDisplayType.Both,
            _ => throw new ArgumentException($"Invalid latency display type: {latencies}. Valid options: normalized, raw, both")
        };
    }

    private static string ExtractHostFromUrl(string url)
    {
        if (Uri.TryCreate(url, UriKind.Absolute, out Uri uri))
            return uri.Host;
        return url; // fallback
    }
}
