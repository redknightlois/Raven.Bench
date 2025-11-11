using System.Globalization;
using RavenBench.Core;
using System;
using System.IO;

namespace RavenBench.Cli;

#nullable disable
internal static class CliParsing
{
    public static RunOptions ToRunOptions(this ClosedSettings s)
    {
        var stepString = s.Step ?? s.Concurrency ?? "8..512x2";
        var stepPlan = ParseStepPlan(stepString);
        return BuildRunOptions(s, LoadShape.Closed, stepPlan, modeOverride: "closed");
    }

    public static RunOptions ToRunOptions(this RateSettings s)
    {
        var stepPlan = ParseStepPlan(s.Step ?? "200..20000x1.5");
        return BuildRunOptions(s, LoadShape.Rate, stepPlan, rateWorkers: s.RateWorkers, modeOverride: "rate");
    }

    private static RunOptions BuildRunOptions(
        BaseRunSettings settings,
        LoadShape shape,
        StepPlan stepPlan,
        int? rateWorkers = null,
        string? modeOverride = null)
    {
        var (kneeThroughputDelta, kneeP95Delta) = ParseKneeRule(settings.KneeRule);

        var database = string.IsNullOrEmpty(settings.DatasetProfile) == false || string.IsNullOrEmpty(settings.Dataset) == false
            ? (settings.Database ?? "temp-placeholder")
            : RequiredString(settings.Database!, "--database");

        var mode = modeOverride ?? shape.ToString().ToLowerInvariant();

        return new RunOptions
        {
            Url = RequiredString(settings.Url!, "--url"),
            Database = database,
            Reads = ParseNullableWeight(settings.Reads),
            Writes = ParseNullableWeight(settings.Writes),
            Updates = ParseNullableWeight(settings.Updates),
            Profile = ParseProfile(settings.Profile),
            QueryProfile = ParseQueryProfile(settings.QueryProfile),
            DocumentSizeBytes = ParseSize(settings.DocSize),
            Step = stepPlan.Normalize(),
            Shape = shape,
            RateWorkers = rateWorkers,
            Warmup = ParseDuration(settings.Warmup),
            Duration = ParseDuration(settings.Duration),
            MaxErrorRate = ParsePercent(settings.MaxErrors),
            KneeThroughputDelta = kneeThroughputDelta,
            KneeP95Delta = kneeP95Delta,
            ThreadPoolWorkers = settings.TpWorkers,
            ThreadPoolIOCP = settings.TpIOCP,
            Distribution = settings.Distribution,
            Transport = settings.Transport,
            Compression = settings.Compression,
            Mode = mode,
            OutJson = settings.OutJson,
            OutCsv = settings.OutCsv,
            Seed = settings.Seed,
            Preload = settings.Preload,
            RawEndpoint = settings.RawEndpoint,
            Notes = settings.Notes,
            ExpectedCores = settings.ExpectedCores,
            NetworkLimitedMode = settings.NetworkLimitedMode,
            LinkMbps = settings.LinkMbps,
            HttpVersion = settings.HttpVersion,
            StrictHttpVersion = settings.StrictHttpVersion,
            Verbose = settings.Verbose,
            Snmp = BuildSnmpOptions(settings),
            LatencyDisplay = ParseLatencyDisplayType(settings.Latencies),
            BulkBatchSize = settings.BulkBatchSize,
            BulkDepth = settings.BulkDepth,
            Dataset = settings.Dataset,
            DatasetProfile = settings.DatasetProfile,
            DatasetSize = settings.DatasetSize,
            DatasetSkipIfExists = settings.DatasetSkipIfExists,
            DatasetCacheDir = settings.DatasetCacheDir,
            OutputDir = settings.OutputDir,
            LatencyHistogramsDir = null,
            LatencyHistogramsFormat = ParseHistogramExportFormat(settings.HistogramsFormat)
        };
    }


    private static SnmpOptions BuildSnmpOptions(BaseRunSettings s)
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

    private static QueryProfile ParseQueryProfile(string? queryProfile)
    {
        // Default to Equality for backward compatibility
        if (string.IsNullOrWhiteSpace(queryProfile))
            return QueryProfile.Equality;

        return queryProfile.Trim().ToLowerInvariant() switch
        {
            "equality" or "eq" => QueryProfile.Equality,
            "range" => QueryProfile.Range,
            "text-prefix" or "textprefix" or "prefix" => QueryProfile.TextPrefix,
            "text-search" or "textsearch" or "search" => QueryProfile.TextSearch,
            "text-search-rare" or "textsearchrare" or "search-rare" => QueryProfile.TextSearchRare,
            "text-search-common" or "textsearchcommon" or "search-common" => QueryProfile.TextSearchCommon,
            "text-search-mixed" or "textsearchmixed" or "search-mixed" => QueryProfile.TextSearchMixed,
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

    public static StepPlan ParseStepPlan(string s)
    {
        var parts = s.Split("..", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length != 2) throw new ArgumentException("Invalid step plan format");
        var start = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var right = parts[1];
        var x = right.IndexOf('x');
        var end = x >= 0 ? int.Parse(right.Substring(0, x), CultureInfo.InvariantCulture) : int.Parse(right, CultureInfo.InvariantCulture);
        var factor = x >= 0 ? double.Parse(right.Substring(x + 1), CultureInfo.InvariantCulture) : 2.0;
        return new StepPlan(start, end, factor);
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
        s = s.Trim();

        // Try to parse as a full TimeSpan string (e.g., "00:16:21.7371071" or "1.02:30:00")
        // Only attempt if the string contains a colon to avoid ambiguity with plain numbers
        // (which TimeSpan.TryParse would interpret as days)
        if (s.Contains(':') && TimeSpan.TryParse(s, CultureInfo.InvariantCulture, out var timeSpan))
            return timeSpan;

        // Fall back to custom format parsing
        s = s.ToLowerInvariant();
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

    private static HistogramExportFormat ParseHistogramExportFormat(string format)
    {
        return format.ToLowerInvariant() switch
        {
            "hlog" => HistogramExportFormat.Hlog,
            "csv" => HistogramExportFormat.Csv,
            "both" => HistogramExportFormat.Both,
            _ => throw new ArgumentException($"Invalid histogram export format: {format}. Valid options: hlog, csv, both")
        };
    }

    public static RunOptions ApplyOutputOptions(RunOptions opts)
    {
        if (string.IsNullOrEmpty(opts.OutputDir) == false)
        {
            var prefix = opts.OutputDir!;
            return opts with
            {
                OutJson = $"{prefix}.json",
                OutCsv = $"{prefix}.csv",
                LatencyHistogramsDir = prefix
            };
        }

        if (string.IsNullOrEmpty(opts.LatencyHistogramsDir) &&
            string.IsNullOrEmpty(opts.OutCsv) == false)
        {
            var outputPath = opts.OutCsv!;
            var outputDir = Path.GetDirectoryName(outputPath) ?? ".";
            var outputName = Path.GetFileNameWithoutExtension(outputPath);
            var histogramPrefix = Path.Combine(outputDir, outputName);

            return opts with { LatencyHistogramsDir = histogramPrefix };
        }

        return opts;
    }

    private static string ExtractHostFromUrl(string url)
    {
        if (Uri.TryCreate(url, UriKind.Absolute, out Uri uri))
            return uri.Host;
        return url; // fallback
    }
}
