using System.Globalization;
using RavenBench.Core;
using RavenBench.Core.Workload;
using System;
using System.IO;

namespace RavenBench.Cli;

#nullable disable
internal static class CliParsing
{
    public static RunOptions ToRunOptions(this ClosedSettings s)
    {
        var stepPlan = ParseStepPlan(s.Step ?? s.Concurrency);
        return BuildRunOptions(s, LoadShape.Closed, stepPlan);
    }

    public static RunOptions ToRunOptions(this RateSettings s)
    {
        var stepPlan = ParseStepPlan(s.Step ?? "200..20000x1.5");
        return BuildRunOptions(s, LoadShape.Rate, stepPlan, rateWorkers: s.RateWorkers);
    }

    private static RunOptions BuildRunOptions(
        BaseRunSettings settings,
        LoadShape shape,
        StepPlan stepPlan,
        int? rateWorkers = null)
    {
        var database = string.IsNullOrEmpty(settings.DatasetProfile) == false || string.IsNullOrEmpty(settings.Dataset) == false
            ? (settings.Database ?? "temp-placeholder")
            : RequiredString(settings.Database!, "--database");

        return new RunOptions
        {
            Url = RequiredString(settings.Url!, "--url"),
            Database = database,
            Reads = ParseNullableWeight(settings.Reads),
            Writes = ParseNullableWeight(settings.Writes),
            Updates = ParseNullableWeight(settings.Updates),
            VectorTopK = settings.VectorTopK,
            VectorQuantization = ParseVectorQuantization(settings.VectorQuantization),
            VectorExactSearch = settings.VectorExactSearch,
            VectorMinSimilarity = settings.VectorMinSimilarity,
            VectorDimension = settings.VectorDimension,
            VectorEdges = settings.VectorEdges,
            VectorCandidates = settings.VectorCandidates,
            VectorSearchEf = settings.VectorSearchEf,
            VectorRecallKs = ParseRecallKs(settings.VectorRecallKs, settings.VectorTopK),
            VectorRecallEfSweep = ParseEfSweep(settings.VectorRecallEfSweep),
            Profile = ParseProfile(settings.Profile),
            QueryProfile = ParseQueryProfile(settings.QueryProfile),
            DocumentSizeBytes = ParseSize(settings.DocSize),
            Step = stepPlan.Normalize(),
            Shape = shape,
            RateWorkers = rateWorkers,
            Warmup = ParseDuration(settings.Warmup),
            Duration = ParseDuration(settings.Duration),
            MaxErrorRate = ParsePercent(settings.MaxErrors),
            ThreadPoolWorkers = settings.TpWorkers,
            ThreadPoolIOCP = settings.TpIOCP,
            Distribution = ParseDistribution(settings.Distribution),
            Transport = ParseTransport(settings.Transport),
            Compression = ParseCompression(settings.Compression),
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
            DatasetSkipIfExists = (settings.DatasetSkipIfExists ?? true) && settings.ForceDatasetImport == false,
            DatasetCacheDir = settings.DatasetCacheDir,
            DatasetSource = settings.DatasetSource,
            OutputDir = settings.OutputDir,
            LatencyHistogramsDir = null,
            LatencyHistogramsFormat = ParseHistogramExportFormat(settings.HistogramsFormat),
            SearchEngine = ParseSearchEngine(settings.SearchEngine)
        };
    }


    private static SnmpOptions BuildSnmpOptions(BaseRunSettings s)
    {
        if (s.SnmpEnabled == false)
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
            throw new ArgumentException("--profile is required. Valid options: mixed, writes, reads, query-by-id, query-users-by-name, vector-search, vector-search-exact");

        return profile.Trim().ToLowerInvariant() switch
        {
            "mixed" => WorkloadProfile.Mixed,
            "writes" or "write" => WorkloadProfile.Writes,
            "reads" or "read" => WorkloadProfile.Reads,
            "query-by-id" or "querybyid" => WorkloadProfile.QueryById,
            "bulk-writes" or "bulkwrites" => WorkloadProfile.BulkWrites,
            "stackoverflow-random-reads" or "so-random-reads" => WorkloadProfile.StackOverflowRandomReads,
            "stackoverflow-text-search" or "so-text-search" => WorkloadProfile.StackOverflowTextSearch,
            "query-users-by-name" or "queryusersbyname" => WorkloadProfile.QueryUsersByName,
            "vector-search" or "vectorsearch" => WorkloadProfile.VectorSearch,
            "vector-search-exact" or "vectorsearchexact" => WorkloadProfile.VectorSearchExact,
            _ => throw new ArgumentException($"Invalid profile: {profile}. Valid options: mixed, writes, reads, query-by-id, bulk-writes, stackoverflow-random-reads, stackoverflow-text-search, query-users-by-name, vector-search, vector-search-exact")
        };
    }

    private static QueryProfile ParseQueryProfile(string queryProfile)
    {
        // Default to VoronEquality (direct document lookup via id())
        if (string.IsNullOrWhiteSpace(queryProfile))
            return QueryProfile.VoronEquality;

        return queryProfile.Trim().ToLowerInvariant() switch
        {
            "voron-equality" or "voron" => QueryProfile.VoronEquality,
            "index-equality" or "index" => QueryProfile.IndexEquality,
            "range" => QueryProfile.Range,
            "text-prefix" or "textprefix" or "prefix" => QueryProfile.TextPrefix,
            "text-search" or "textsearch" or "search" => QueryProfile.TextSearch,
            "text-search-rare" or "textsearchrare" or "search-rare" => QueryProfile.TextSearchRare,
            "text-search-common" or "textsearchcommon" or "search-common" => QueryProfile.TextSearchCommon,
            "text-search-mixed" or "textsearchmixed" or "search-mixed" => QueryProfile.TextSearchMixed,
            _ => throw new ArgumentException($"Invalid query profile: {queryProfile}. Valid options: voron-equality, index-equality, range, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed")
        };
    }

    private static TransportKind ParseTransport(string transport)
    {
        return transport.Trim().ToLowerInvariant() switch
        {
            "raw" => TransportKind.Raw,
            "client" => TransportKind.Client,
            _ => throw new ArgumentException($"Invalid transport: {transport}. Valid options: raw, client")
        };
    }

    private static KeyDistributionKind ParseDistribution(string distribution)
    {
        return distribution.Trim().ToLowerInvariant() switch
        {
            "uniform" => KeyDistributionKind.Uniform,
            "zipfian" => KeyDistributionKind.Zipfian,
            "latest" => KeyDistributionKind.Latest,
            _ => throw new ArgumentException($"Invalid distribution: {distribution}. Valid options: uniform, zipfian, latest")
        };
    }

    private static CompressionMode ParseCompression(string compression)
    {
        return compression.Trim().ToLowerInvariant() switch
        {
            "identity" => CompressionMode.Identity,
            "gzip" => CompressionMode.Gzip,
            "zstd" => CompressionMode.Zstd,
            "br" or "brotli" => CompressionMode.Brotli,
            "deflate" => CompressionMode.Deflate,
            _ => throw new ArgumentException($"Invalid compression: {compression}. Valid options: identity, gzip, zstd, br, deflate")
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
        if (factor <= 1.0)
            throw new ArgumentException($"Invalid step plan '{s}': factor must be greater than 1, got {factor}");
        return new StepPlan(start, end, factor);
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

        // Full TimeSpan strings require a ':' — TimeSpan.TryParse would interpret a plain number as days
        if (s.Contains(':') && TimeSpan.TryParse(s, CultureInfo.InvariantCulture, out var timeSpan))
            return timeSpan;

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

        var value = double.Parse(s, CultureInfo.InvariantCulture);
        if (value <= 1.0)
            return value;
        if (value > 100.0)
            throw new ArgumentException($"Invalid percentage: {s}. Use a fraction (0.05), a percent (5%), or a bare number up to 100.");
        return value / 100.0;
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

    // Public wrappers for the recall-only command
    public static int[] ParseRecallKsRaw(string recallKs) => ParseRecallKs(recallKs, int.MaxValue);
    public static int[] ParseEfSweepRaw(string efSweep) => ParseEfSweep(efSweep);

    private static int[] ParseRecallKs(string recallKs, int vectorTopK)
    {
        if (string.IsNullOrWhiteSpace(recallKs))
            return null;

        var parts = recallKs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var ks = new int[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            ks[i] = int.Parse(parts[i], CultureInfo.InvariantCulture);
            if (ks[i] <= 0)
                throw new ArgumentException($"--vector-recall-ks values must be positive, got {ks[i]}");
            if (ks[i] > vectorTopK)
                throw new ArgumentException($"--vector-recall-ks value {ks[i]} exceeds --vector-topk {vectorTopK}");
        }

        Array.Sort(ks);
        return ks;
    }

    private static int[] ParseEfSweep(string efSweep)
    {
        if (string.IsNullOrWhiteSpace(efSweep))
            return null;

        var parts = efSweep.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var values = new int[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            values[i] = int.Parse(parts[i], CultureInfo.InvariantCulture);
            if (values[i] <= 0)
                throw new ArgumentException($"--vector-recall-ef-sweep values must be positive, got {values[i]}");
        }

        Array.Sort(values);
        return values;
    }

    private static VectorQuantization ParseVectorQuantization(string quantization)
    {
        return quantization.Trim().ToLowerInvariant() switch
        {
            "none" => VectorQuantization.None,
            "int8" => VectorQuantization.Int8,
            "int4" => VectorQuantization.Int4,
            "int3" => VectorQuantization.Int3,
            "int2" => VectorQuantization.Int2,
            "binary" => VectorQuantization.Binary,
            _ => throw new ArgumentException($"Invalid vector quantization: {quantization}. Valid options: none, int8, int4, int3, int2, binary")
        };
    }

    private static IndexingEngine ParseSearchEngine(string value)
    {
        return value.Trim().ToLowerInvariant() switch
        {
            "lucene" => IndexingEngine.Lucene,
            "corax" => IndexingEngine.Corax,
            _ => throw new ArgumentException($"Invalid search engine: {value}. Valid options: corax, lucene")
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
}
