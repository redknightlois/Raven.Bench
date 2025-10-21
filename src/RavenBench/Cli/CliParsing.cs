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

        // Database is optional when using dataset profiles (will be auto-generated)
        var database = string.IsNullOrEmpty(s.DatasetProfile) == false || string.IsNullOrEmpty(s.Dataset) == false
            ? (s.Database ?? "temp-placeholder") // Will be overridden by dataset logic
            : RequiredString(s.Database!, "--database");

        return new RunOptions
        {
            Url = RequiredString(s.Url!, "--url"),
            Database = database,
            Reads = ParseNullableWeight(s.Reads),
            Writes = ParseNullableWeight(s.Writes),
            Updates = ParseNullableWeight(s.Updates),
            Profile = ParseProfile(s.Profile),
            QueryProfile = ParseQueryProfile(s.QueryProfile),
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
            Snmp = BuildSnmpOptions(s),
            LatencyDisplay = ParseLatencyDisplayType(s.Latencies),
            BulkBatchSize = s.BulkBatchSize,
            BulkDepth = s.BulkDepth,
            Dataset = s.Dataset,
            DatasetProfile = s.DatasetProfile,
            DatasetSize = s.DatasetSize,
            DatasetSkipIfExists = s.DatasetSkipIfExists,
            DatasetCacheDir = s.DatasetCacheDir
        };
    }

    private static SnmpOptions BuildSnmpOptions(RunSettings s)
    {
        if (!s.SnmpEnabled)
            return SnmpOptions.Disabled;

        return new SnmpOptions
        {
            Enabled = true,
            Port = s.SnmpPort,
            PollInterval = ParseDuration(s.SnmpInterval ?? "250ms"),
            Profile = ParseSnmpProfile(s.SnmpProfile),
            Timeout = ParseDuration(s.SnmpTimeout)
        };
    }

    private static WorkloadProfile ParseProfile(string profile)
    {
        if (string.IsNullOrWhiteSpace(profile))
            throw new ArgumentException("--profile is required. Valid options: mixed, writes, reads, query-by-id, query-users-by-name");

        return profile.Trim().ToLowerInvariant() switch
        {
            "mixed" => WorkloadProfile.Mixed,
            "writes" or "write" => WorkloadProfile.Writes,
            "reads" or "read" => WorkloadProfile.Reads,
            "query-by-id" or "querybyid" => WorkloadProfile.QueryById,
            "bulk-writes" or "bulkwrites" => WorkloadProfile.BulkWrites,
            "stackoverflow-reads" or "so-reads" => WorkloadProfile.StackOverflowReads,
            "stackoverflow-queries" or "so-queries" => WorkloadProfile.StackOverflowQueries,
            "query-users-by-name" or "queryusersbyname" => WorkloadProfile.QueryUsersByName,
            _ => throw new ArgumentException($"Invalid profile: {profile}. Valid options: mixed, writes, reads, query-by-id, bulk-writes, stackoverflow-reads, stackoverflow-queries, query-users-by-name")
        };
    }

    private static Util.QueryProfile ParseQueryProfile(string? queryProfile)
    {
        // Default to Equality for backward compatibility
        if (string.IsNullOrWhiteSpace(queryProfile))
            return Util.QueryProfile.Equality;

        return queryProfile.Trim().ToLowerInvariant() switch
        {
            "equality" or "eq" => Util.QueryProfile.Equality,
            "range" => Util.QueryProfile.Range,
            "text-prefix" or "textprefix" or "prefix" => Util.QueryProfile.TextPrefix,
            "text-search" or "textsearch" or "search" => Util.QueryProfile.TextSearch,
            "text-search-rare" or "textsearchrare" or "search-rare" => Util.QueryProfile.TextSearchRare,
            "text-search-common" or "textsearchcommon" or "search-common" => Util.QueryProfile.TextSearchCommon,
            "text-search-mixed" or "textsearchmixed" or "search-mixed" => Util.QueryProfile.TextSearchMixed,
            _ => throw new ArgumentException($"Invalid query profile: {queryProfile}. Valid options: equality, range, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed")
        };
    }

    private static SnmpProfile ParseSnmpProfile(string profile)
    {
        return profile.ToLowerInvariant() switch
        {
            "minimal" => SnmpProfile.Minimal,
            "extended" => SnmpProfile.Extended,
            _ => throw new ArgumentException($"Invalid SNMP profile: {profile}. Valid options: minimal, extended")
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

    private static double? ParseNullableWeight(string s) => string.IsNullOrWhiteSpace(s) ? null : ParseWeight(s);

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
