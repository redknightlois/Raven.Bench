using System.Collections.Concurrent;
using System.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Diagnostics;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using Spectre.Console;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using RavenBench.Dataset;


namespace RavenBench;

/// <summary>
/// Simple error deduplication for verbose logging to prevent spam.
/// </summary>
internal static class VerboseErrorTracker
{
    private static readonly ConcurrentDictionary<string, int> ErrorCounts = new();

    public static void LogError(string errorMessage, bool verbose)
    {
        if (verbose == false || string.IsNullOrEmpty(errorMessage))
            return;

        // Just count the error, don't print it immediately
        ErrorCounts.AddOrUpdate(errorMessage, 1, (key, oldValue) => oldValue + 1);
    }

    public static void Reset()
    {
        ErrorCounts.Clear();
    }

    public static void PrintSummary()
    {
        if (ErrorCounts.IsEmpty)
            return;

        Console.WriteLine("[Raven.Bench] Verbose Error Summary:");
        var sortedErrors = ErrorCounts.OrderByDescending(kvp => kvp.Value).Take(10);
        foreach (var (error, count) in sortedErrors)
        {
            Console.WriteLine($"[Raven.Bench]   {count}× {error}");
        }

        var totalErrors = ErrorCounts.Values.Sum();
        var errorTypes = ErrorCounts.Count;
        if (errorTypes > 10)
        {
            var moreTypes = errorTypes - 10;
            Console.WriteLine($"[Raven.Bench]   ... and {moreTypes} more error type{(moreTypes == 1 ? "" : "s")} (total: {totalErrors} errors)");
        }
        else if (totalErrors > 0)
        {
            Console.WriteLine($"[Raven.Bench]   Total: {totalErrors} error{(totalErrors == 1 ? "" : "s")} across {errorTypes} type{(errorTypes == 1 ? "" : "s")}");
        }
    }
}

public class BenchmarkRunner(RunOptions opts)
{
    private readonly Random _rng = new(opts.Seed);

    public async Task<BenchmarkRun> RunAsync()
    {
        // Reset error tracking for this benchmark run
        VerboseErrorTracker.Reset();

        // Validate search engine compatibility with profile
        if (WorkloadProfiles.SupportsEngine(opts.Profile, opts.SearchEngine) == false)
        {
            var supported = string.Join(", ", WorkloadProfiles.GetSupportedEngines(opts.Profile).Select(e => e.ToString().ToLowerInvariant()));
            throw new InvalidOperationException(
                $"Profile '{opts.Profile}' does not support {opts.SearchEngine} indexing engine. " +
                $"Supported engines: {supported}.");
        }

        // Set ThreadPool minimum threads based on command-line parameters
        var workers = opts.ThreadPoolWorkers ?? 8192;
        var iocp = opts.ThreadPoolIOCP ?? 8192;
        
        Console.WriteLine($"[Raven.Bench] Setting ThreadPool: workers={workers}, iocp={iocp}");
        ThreadPool.SetMinThreads(workers, iocp);

        // Negotiate HTTP version before creating transport
        Console.WriteLine("[Raven.Bench] Negotiating HTTP version...");
        var negotiatedHttpVersion = await HttpVersionNegotiator.NegotiateVersionAsync(
            opts.Url,
            opts.HttpVersion,
            opts.StrictHttpVersion);
        Console.WriteLine($"[Raven.Bench] Using HTTP/{HttpHelper.FormatHttpVersion(negotiatedHttpVersion)}");

        // Import dataset if specified - this may override the database name
        string? datasetDatabase = null;
        bool datasetWasImported = false;
        if (string.IsNullOrEmpty(opts.Dataset) == false)
        {
            if (opts.Dataset.StartsWith("clinicalwords", StringComparison.OrdinalIgnoreCase))
            {
                var (database, imported) = await ImportClinicalWordsDatasetAsync(opts, negotiatedHttpVersion);
                datasetDatabase = database;
                datasetWasImported = imported;
            }
            else
            {
                datasetDatabase = await ImportDatasetAsync(opts);
                datasetWasImported = true;
            }

            if (datasetDatabase != opts.Database)
            {
                Console.WriteLine($"[Raven.Bench] Using dataset-specific database: '{datasetDatabase}'");
            }
        }

        // Use dataset database if different from opts.Database
        var effectiveDatabase = datasetDatabase ?? opts.Database;

        using var transport = BuildTransport(opts, negotiatedHttpVersion, effectiveDatabase);

        // Ensure database exists before preload or benchmark
        Console.WriteLine($"[Raven.Bench] Ensuring database '{effectiveDatabase}' exists...");
        await transport.EnsureDatabaseExistsAsync(effectiveDatabase);

        // Wait for indexes to be non-stale ONLY if we actually imported
        if (datasetWasImported)
        {
            await WaitForNonStaleIndexesAsync(opts.Url, effectiveDatabase, negotiatedHttpVersion);
        }

        // Create static indexes for StackOverflow/Users workloads (replaces auto-indexes)
        // This must happen before metadata discovery so we can set the index names
        StackOverflowDatasetProvider.StaticIndexNames? staticIndexNames = null;
        var needsStaticIndexes = opts.Profile == WorkloadProfile.StackOverflowRandomReads ||
                                  opts.Profile == WorkloadProfile.StackOverflowTextSearch ||
                                  opts.Profile == WorkloadProfile.QueryUsersByName;
        if (needsStaticIndexes && opts.Dataset?.Equals("stackoverflow", StringComparison.OrdinalIgnoreCase) == true)
        {
            var stackOverflowProvider = new StackOverflowDatasetProvider();
            staticIndexNames = await stackOverflowProvider.CreateStaticIndexesAsync(
                opts.Url,
                effectiveDatabase,
                opts.SearchEngine,
                negotiatedHttpVersion);
        }

        // Discover workload metadata for StackOverflow profiles (after dataset import and index wait)
        StackOverflowWorkloadMetadata? stackOverflowMetadata = null;
        if (opts.Profile == WorkloadProfile.StackOverflowRandomReads || opts.Profile == WorkloadProfile.StackOverflowTextSearch)
        {
            stackOverflowMetadata = await StackOverflowWorkloadHelper.DiscoverOrLoadMetadataAsync(
                opts.Url,
                effectiveDatabase,
                opts.Seed);

            if (stackOverflowMetadata == null)
            {
                throw new InvalidOperationException("StackOverflow metadata not available. Ensure dataset is imported and indexes are not stale.");
            }

            // Set static index names on metadata for workloads to use
            if (staticIndexNames != null)
            {
                stackOverflowMetadata.TitleIndexName = staticIndexNames.QuestionsTitleIndex;
                stackOverflowMetadata.TitleSearchIndexName = staticIndexNames.QuestionsTitleSearchIndex;
            }
        }

        // Discover workload metadata for QueryUsersByName profile (after dataset import and index wait)
        StackOverflowUsersWorkloadMetadata? usersMetadata = null;
        if (opts.Profile == WorkloadProfile.QueryUsersByName)
        {
            usersMetadata = await StackOverflowUsersWorkloadHelper.DiscoverOrLoadMetadataAsync(
                opts.Url,
                effectiveDatabase,
                opts.Seed);

            if (usersMetadata == null)
            {
                throw new InvalidOperationException("Users metadata not available. Ensure StackOverflow dataset is imported and indexes are not stale.");
            }

            // Set static index names on metadata for workloads to use
            if (staticIndexNames != null)
            {
                usersMetadata.DisplayNameIndexName = staticIndexNames.UsersDisplayNameIndex;
                usersMetadata.ReputationIndexName = staticIndexNames.UsersReputationIndex;
            }
        }

        // Discover workload metadata for vector search profiles
        VectorWorkloadMetadata? vectorMetadata = null;
        if (IsVectorSearchProfile(opts.Profile))
        {
            vectorMetadata = await LoadVectorMetadataAsync(opts, effectiveDatabase);
            if (vectorMetadata == null)
            {
                throw new InvalidOperationException("Vector metadata not available. Ensure vector dataset is imported or specify --dataset-cache-dir with query vectors.");
            }
        }

        var workload = BuildWorkload(opts, stackOverflowMetadata, usersMetadata, vectorMetadata);

        // Only preload for profiles that actually use bench/ prefix documents
        if (opts.Preload > 0 && ProfileRequiresPreload(opts.Profile))
            await PreloadAsync(transport, opts, opts.Preload, _rng, opts.DocumentSizeBytes);
        else if (opts.Preload > 0)
            Console.WriteLine($"[Raven.Bench] Skipping preload - profile '{opts.Profile}' uses imported dataset");

        var steps = new List<StepResult>();
        var histogramArtifacts = new List<HistogramArtifact>();

        var stepPlan = opts.Step.Normalize();
        var currentValue = stepPlan.Start;
        var endValue = stepPlan.End;

        var cpuTracker = new ProcessCpuTracker();
        using var serverTracker = new ServerMetricsTracker(transport, opts);
        var maxNetUtil = 0.0;
        StartupCalibration? startupCalibration = null;
        string clientCompression = transport switch
        {
            RavenClientTransport rc => rc.EffectiveCompressionMode,
            RawHttpTransport raw => raw.EffectiveCompressionMode,
            _ => "unknown"
        };
        // Use the negotiated HTTP version directly
        string httpVersion = HttpHelper.FormatHttpVersion(negotiatedHttpVersion);

        await ValidateClientAsync(transport);
        await ValidateServerSanityAsync(transport);
        await ValidateSnmpAsync(transport);

        try
        {
            Console.WriteLine("[Raven.Bench] Running startup calibration...");

            // Show progress bar during calibration
            startupCalibration = await AnsiConsole.Progress()
                .StartAsync(async ctx =>
                {
                    var task = ctx.AddTask("[green]Startup calibration[/]");
                    task.MaxValue = 100;

                    var endpoints = transport.GetCalibrationEndpoints();
                    var (endpointData, diagnostics) = await EndpointCalibrator.CalibrateEndpointsWithDiagnosticsAsync(transport, endpoints,
                        progress => task.Value = progress).ConfigureAwait(false);
                    return new StartupCalibration { Endpoints = endpointData, Diagnostics = diagnostics };
                }).ConfigureAwait(false);
            
            if (startupCalibration.Endpoints.Count > 0)
            {
                Console.WriteLine("[Raven.Bench] Startup calibration completed:");
                foreach (var endpoint in startupCalibration.Endpoints)
                {
                    Console.WriteLine($"[Raven.Bench]   {endpoint.Name}: TTFB={endpoint.TtfbMs:F2} ms, Total={endpoint.ObservedMs:F2} ms, HTTP/{endpoint.HttpVersion}");
                }
            }
            else
            {
                Console.WriteLine("[Raven.Bench] ERROR: Startup calibration failed - no successful measurements obtained");

                if (startupCalibration.Diagnostics != null)
                {
                    var diag = startupCalibration.Diagnostics;
                    Console.WriteLine($"[Raven.Bench]   Server: {opts.Url}");
                    Console.WriteLine($"[Raven.Bench]   Database: {opts.Database}");
                    Console.WriteLine($"[Raven.Bench]   Total attempts: {diag.TotalAttempts} ({diag.SuccessfulAttempts} succeeded, {diag.FailedAttempts} failed)");
                    Console.WriteLine($"[Raven.Bench]   Endpoints tested: {diag.TotalEndpoints}");

                    foreach (var endpoint in diag.EndpointDetails)
                    {
                        Console.WriteLine($"[Raven.Bench]   {endpoint.Name} ({endpoint.Path}): {endpoint.SuccessCount}/{endpoint.AttemptCount} successful");
                        if (endpoint.FailureCount > 0)
                        {
                            var errorGroups = endpoint.FailureReasons
                                .GroupBy(r => r)
                                .OrderByDescending(g => g.Count())
                                .Take(3)
                                .ToList();

                            foreach (var errorGroup in errorGroups)
                            {
                                var errorMessage = errorGroup.Key;
                                var countSuffix = errorGroup.Count() == 1 ? "" : $" (×{errorGroup.Count()})";

                                // Add helpful context for common errors
                                if (errorMessage.Contains("invalid request URI") || errorMessage.Contains("BaseAddress"))
                                {
                                    Console.WriteLine($"[Raven.Bench]     - URL construction error: {errorMessage}{countSuffix}");
                                    Console.WriteLine($"[Raven.Bench]       → Check if server URL is correct: {opts.Url}");
                                }
                                else if (errorMessage.Contains("404") || errorMessage.Contains("Not Found"))
                                {
                                    Console.WriteLine($"[Raven.Bench]     - Endpoint not found: {errorMessage}{countSuffix}");
                                    Console.WriteLine($"[Raven.Bench]       → Server may be older version or different RavenDB edition");
                                }
                                else if (errorMessage.Contains("Connection") || errorMessage.Contains("connect"))
                                {
                                    Console.WriteLine($"[Raven.Bench]     - Connection failed: {errorMessage}{countSuffix}");
                                    Console.WriteLine($"[Raven.Bench]       → Check if server is running at {opts.Url}");
                                }
                                else
                                {
                                    Console.WriteLine($"[Raven.Bench]     - {errorMessage}{countSuffix}");
                                }
                            }

                            var totalShown = errorGroups.Sum(g => g.Count());
                            if (endpoint.FailureReasons.Count > totalShown)
                            {
                                Console.WriteLine($"[Raven.Bench]     - ... and {endpoint.FailureReasons.Count - totalShown} more errors");
                            }
                        }
                    }
                }

                Console.WriteLine("[Raven.Bench] NOTE: Benchmark will continue but latency baselines will not be available");
                startupCalibration = null;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Raven.Bench] ERROR: Startup calibration failed: {ex.Message}");
            startupCalibration = null;
        }

        // Create core context for executor
        // Create benchmark executor with all dependencies
        var executor = new BenchmarkExecutor(
            opts,
            transport,
            workload,
            cpuTracker,
            serverTracker,
            opts.Snmp.Enabled ? opts.Snmp : null);

	        double? observedServiceTimeSeconds = null;
	        int? previousAutoRateWorkers = null;

        while (currentValue <= endValue)
        {
            // Calculate baseline latency for coordinated omission correction
            var baselineLatencyMicros = startupCalibration?.Endpoints.Count > 0
                ? (long)(startupCalibration.Endpoints.Min(e => e.ObservedMs) * 1000) // Convert ms to µs
                : 0L;

	            // Calculate worker fan-out for rate mode using either the override or baseline-derived heuristic
	            var rateWorkerCount = opts.Shape == LoadShape.Rate
	                ? ResolveRateWorkerCount(opts, (int)currentValue, baselineLatencyMicros, observedServiceTimeSeconds)
	                : 0;

	            if (opts.Shape == LoadShape.Rate && opts.RateWorkers.HasValue == false && previousAutoRateWorkers.HasValue)
	            {
	                // Avoid sudden worker explosions from transient tail-latency spikes.
	                // Doubling the target RPS should not require >2x workers in one step under normal conditions.
	                rateWorkerCount = Math.Min(rateWorkerCount, previousAutoRateWorkers.Value * 2);
	            }

            // Create appropriate load generator based on load shape
            ILoadGenerator loadGenerator = opts.Shape switch
            {
                LoadShape.Rate => new RateLoadGenerator(transport, workload, (int)currentValue, rateWorkerCount, _rng),
                LoadShape.Closed => new ClosedLoopLoadGenerator(transport, workload, (int)currentValue, _rng),
                _ => new ClosedLoopLoadGenerator(transport, workload, (int)currentValue, _rng)
            };

            LogStepStart(opts.Shape, steps.Count + 1, (int)currentValue, rateWorkerCount, opts);

            // Execute step using the executor
	            var (latencyRecorder, stepResult) = await executor.ExecuteStepAsync(loadGenerator, steps.Count, (int)currentValue, CancellationToken.None, baselineLatencyMicros);

            // Take histogram snapshot for this step
	            var snapshot = latencyRecorder.Snapshot();

	            if (opts.Shape == LoadShape.Rate && opts.RateWorkers.HasValue == false)
	            {
	                // In rate mode, baseline RTT can be much lower than the end-to-end request service time.
	                // Use a high percentile from the *measured* latency distribution to size workers for the next step.
	                // This avoids under-driving the client when per-op CPU/serialization dominates the RTT.
	                var p99ServiceTimeSeconds = Math.Max(1e-6, snapshot.GetPercentile(99) / 1_000_000.0);
	                var throughputImpliedServiceTimeSeconds = stepResult.Throughput > 0
	                    ? stepResult.Concurrency / stepResult.Throughput
	                    : 0;
	                var estimatedServiceTimeSeconds = Math.Max(p99ServiceTimeSeconds, throughputImpliedServiceTimeSeconds);
	                observedServiceTimeSeconds = observedServiceTimeSeconds.HasValue
	                    ? Math.Max(observedServiceTimeSeconds.Value, estimatedServiceTimeSeconds)
	                    : estimatedServiceTimeSeconds;

	                previousAutoRateWorkers = stepResult.Concurrency;
	            }

            // Calculate percentiles from snapshot
            int[] percentiles = { 50, 75, 90, 95, 99, 999 };
            var rawValues = new double[6];
            for (int i = 0; i < percentiles.Length; i++)
            {
                rawValues[i] = snapshot.GetPercentile(percentiles[i]) / 1000.0;
            }

            // Calculate high-percentile tail metrics (p99.9, p99.99, max)
            // These capture extreme outliers that standard percentiles may miss
            var p9999 = snapshot.GetPercentile(99.99) / 1000.0;  // p99.99 in milliseconds
            var pMax = snapshot.MaxMicros / 1000.0;  // Maximum latency in milliseconds

            var rawPercentiles = new Percentiles(rawValues[0], rawValues[1], rawValues[2], rawValues[3], rawValues[4], rawValues[5]);

            // Apply RTT-based normalization using baseline latency from calibration
            Percentiles normalizedPercentiles;
            if (startupCalibration?.Endpoints.Count > 0)
            {
                // Use minimum observed latency from calibration as baseline RTT
                var baselineRttMs = startupCalibration.Endpoints.Min(e => e.ObservedMs);
                var normalizedValues = new double[6];
                for (int i = 0; i < rawValues.Length; i++)
                {
                    // Subtract baseline RTT to get normalized latency (additional latency due to load)
                    normalizedValues[i] = Math.Max(0, rawValues[i] - baselineRttMs);
                }
                normalizedPercentiles = new Percentiles(normalizedValues[0], normalizedValues[1], normalizedValues[2], normalizedValues[3], normalizedValues[4], normalizedValues[5]);
            }
            else
            {
                // Fallback when calibration is unavailable
                normalizedPercentiles = rawPercentiles;
            }
            
            // Update step result with percentile data
            stepResult.Raw = rawPercentiles;
            stepResult.Normalized = normalizedPercentiles;
            stepResult.P9999 = p9999;
            stepResult.PMax = pMax;
            stepResult.CorrectedCount = snapshot.TotalCount;

            // Apply same baseline normalization to tail metrics as we do for percentiles
            // This ensures normalized view shows additional latency due to load consistently
            if (startupCalibration?.Endpoints.Count > 0)
            {
                var baselineRttMs = startupCalibration.Endpoints.Min(e => e.ObservedMs);
                stepResult.NormalizedP9999 = Math.Max(0, p9999 - baselineRttMs);
                stepResult.NormalizedPMax = Math.Max(0, pMax - baselineRttMs);
            }
            else
            {
                // No calibration available - normalized = raw
                stepResult.NormalizedP9999 = p9999;
                stepResult.NormalizedPMax = pMax;
            }

            // Always build histogram data for JSON output
            var artifact = BuildHistogramArtifact(snapshot, stepResult.Concurrency, opts.LatencyHistogramsDir, opts.LatencyHistogramsFormat);
            if (artifact != null)
            {
                histogramArtifacts.Add(artifact);
            }

            steps.Add(stepResult);
            LogStepResult(steps.Count, stepResult);
            maxNetUtil = Math.Max(maxNetUtil, stepResult.NetworkUtilization);

            // knee stop if error rate exceeds bound significantly
            if (stepResult.ErrorRate > Math.Max(opts.MaxErrorRate, 0.05))
            {
                Console.WriteLine("[Raven.Bench] High error rate; stopping ramp.");
                break;
            }

            // Early stop for rate-based benchmarks when server cannot keep up
            if (opts.Shape == LoadShape.Rate && stepResult.TargetThroughput.HasValue)
            {
                var target = stepResult.TargetThroughput.Value;
                var actual = stepResult.Throughput;
                var deltaPct = (actual - target) / target * 100.0;

                // Stop if throughput is significantly below target (server saturated)
                if (deltaPct < -30.0)
                {
                    Console.WriteLine($"[Raven.Bench] Throughput is {Math.Abs(deltaPct):F1}% below target ({actual:F0} vs {target:F0}). Server appears saturated; stopping ramp.");
                    break;
                }

                // Stop if throughput is degrading compared to previous step (performance regression)
                if (steps.Count >= 2)
                {
                    var prevStep = steps[steps.Count - 2];
                    var throughputDrop = (stepResult.Throughput - prevStep.Throughput) / prevStep.Throughput * 100.0;

                    // If throughput drops by >30% between steps, server is overloaded
                    if (throughputDrop < -30.0)
                    {
                        Console.WriteLine($"[Raven.Bench] Throughput degraded by {Math.Abs(throughputDrop):F1}% from previous step ({stepResult.Throughput:F0} vs {prevStep.Throughput:F0}). Server appears overloaded; stopping ramp.");
                        break;
                    }
                }
            }

            // Stop if latencies indicate extreme server degradation (applies to all load shapes)
            // p99.9 > 30 seconds suggests the server is severely overloaded and continuing is pointless
            if (stepResult.P9999 > 30_000.0) // 30 seconds in milliseconds
            {
                Console.WriteLine($"[Raven.Bench] Extreme latencies detected (p99.9={stepResult.P9999:F0}ms). Server severely degraded; stopping ramp.");
                break;
            }

            currentValue = stepPlan.Next(currentValue);
        }

        // Print verbose error summary if there were any errors
        if (opts.Verbose)
        {
            VerboseErrorTracker.PrintSummary();
        }

        // Get SNMP metrics history before disposing the tracker
        var serverMetricsHistory = serverTracker.GetHistory();

        return new BenchmarkRun
        {
            Steps = steps,
            MaxNetworkUtilization = maxNetUtil,
            ClientCompression = clientCompression,
            EffectiveHttpVersion = httpVersion,
            StartupCalibration = startupCalibration,
            ServerMetricsHistory = serverMetricsHistory.Count > 0 ? serverMetricsHistory : null,
            HistogramArtifacts = histogramArtifacts.Count > 0 ? histogramArtifacts : null
        };
    }

    /// <summary>
    /// Build histogram data for JSON. Always creates the full percentile distribution.
    /// Optionally writes hlog/csv files if outputPrefix is specified.
    /// </summary>
    private static HistogramArtifact? BuildHistogramArtifact(
        HistogramSnapshot snapshot,
        int concurrency,
        string? outputPrefix,
        HistogramExportFormat format)
    {
        var histogram = snapshot.GetHistogram();
        if (histogram == null || histogram.TotalCount == 0)
            return null;

        string? hlogPath = null;
        string? csvPath = null;

        // Only export files if output prefix is specified
        if (string.IsNullOrEmpty(outputPrefix) == false)
        {
            // Ensure output directory exists (in case prefix includes a directory path)
            var outputDir = Path.GetDirectoryName(outputPrefix);
            if (string.IsNullOrEmpty(outputDir) == false)
            {
                Directory.CreateDirectory(outputDir);
            }

            // Optional: write hlog file if requested
            if (format == HistogramExportFormat.Hlog || format == HistogramExportFormat.Both)
            {
                hlogPath = $"{outputPrefix}-step-c{concurrency:D4}.hlog";

                try
                {
                    using var fs = File.Create(hlogPath);
                    using var writer = new StreamWriter(fs);

                    writer.WriteLine("# HdrHistogram Percentile Distribution");
                    writer.WriteLine($"# Concurrency: {concurrency}");
                    writer.WriteLine($"# TotalCount: {histogram.TotalCount}");
                    writer.WriteLine($"# MaxValueMicros: {snapshot.MaxMicros}");
                    writer.WriteLine("# Percentile,LatencyMicros,LatencyMs");

                    foreach (var p in HistogramArtifact.StandardPercentiles)
                    {
                        var valueMicros = histogram.GetValueAtPercentile(p);
                        var valueMs = valueMicros / 1000.0;
                        writer.WriteLine($"{p:F3},{valueMicros},{valueMs:F3}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Raven.Bench] Warning: Failed to export hlog for concurrency {concurrency}: {ex.Message}");
                    hlogPath = null;
                }
            }

            // Optional: write CSV file if requested
            if (format == HistogramExportFormat.Csv || format == HistogramExportFormat.Both)
            {
                csvPath = $"{outputPrefix}-step-c{concurrency:D4}.csv";

                try
                {
                    using var fs = File.Create(csvPath);
                    using var writer = new StreamWriter(fs);

                    writer.WriteLine("Percentile,LatencyMicros,LatencyMs");

                    // Simpler CSV - just the key percentiles most people care about
                    var csvPercentiles = new[] { 0.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9, 99.99, 99.999, 100.0 };
                    foreach (var p in csvPercentiles)
                    {
                        var valueMicros = histogram.GetValueAtPercentile(p);
                        var valueMs = valueMicros / 1000.0;
                        writer.WriteLine($"{p:F3},{valueMicros},{valueMs:F3}");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Raven.Bench] Warning: Failed to export CSV for concurrency {concurrency}: {ex.Message}");
                    csvPath = null;
                }
            }
        }  // End of file export block

        // Build the full percentile distribution for JSON
        var percentiles = HistogramArtifact.StandardPercentiles.ToArray();
        var latencyInMicroseconds = new long[percentiles.Length];
        var latencyInMilliseconds = new double[percentiles.Length];

        for (int i = 0; i < percentiles.Length; i++)
        {
            var valueMicros = histogram.GetValueAtPercentile(percentiles[i]);
            latencyInMicroseconds[i] = valueMicros;
            latencyInMilliseconds[i] = Math.Round(valueMicros / 1000.0, 4);
        }

        // Extract histogram bin data for reconstructing the distribution
        // RecordedValues() returns (value, count) pairs for all non-zero bins
        var binEdgesList = new List<long>();
        var binCountsList = new List<long>();

        foreach (var bucket in histogram.RecordedValues())
        {
            binEdgesList.Add(bucket.ValueIteratedTo);
            binCountsList.Add(bucket.CountAddedInThisIterationStep);
        }

        return new HistogramArtifact
        {
            Concurrency = concurrency,
            TotalCount = histogram.TotalCount,
            MaxValueInMicroseconds = snapshot.MaxMicros,
            Percentiles = percentiles,
            LatencyInMicroseconds = latencyInMicroseconds,
            LatencyInMilliseconds = latencyInMilliseconds,
            BinEdges = binEdgesList.ToArray(),
            BinCounts = binCountsList.ToArray(),
            HlogPath = hlogPath,
            CsvPath = csvPath
        };
    }

    private static void LogStepStart(LoadShape shape, int stepNumber, int currentValue, int rateWorkerCount, RunOptions opts)
    {
        var warmup = FormatDuration(opts.Warmup);
        var duration = FormatDuration(opts.Duration);

        if (shape == LoadShape.Rate)
        {
            var workerSuffix = opts.RateWorkers.HasValue ? string.Empty : " (auto)";
            Console.WriteLine($"[Raven.Bench] Step {stepNumber}: target {currentValue} RPS (workers={rateWorkerCount}{workerSuffix}, warmup={warmup}, duration={duration})");
        }
        else
        {
            Console.WriteLine($"[Raven.Bench] Step {stepNumber}: concurrency {currentValue} (warmup={warmup}, duration={duration})");
        }
    }

    private static string FormatDuration(TimeSpan duration)
    {
        if (duration <= TimeSpan.Zero)
            return "0s";

        if (duration.TotalSeconds >= 10)
            return $"{duration.TotalSeconds:F0}s";

        if (duration.TotalSeconds >= 1)
            return $"{duration.TotalSeconds:F1}s";

        return $"{duration.TotalMilliseconds:F0}ms";
    }

    private static void LogStepResult(int stepNumber, StepResult step)
    {
        if (step.TargetThroughput.HasValue && step.TargetThroughput > 0)
        {
            var target = step.TargetThroughput.Value;
            var actual = step.Throughput;
            var deltaPct = (actual - target) / target * 100.0;
            var deltaFormatted = double.IsFinite(deltaPct) ? $"{deltaPct:+0.0;-0.0;0}%" : "n/a";
            var rollingInfo = step.RollingRate is { HasSamples: true } rate
                ? $" | rolling median {rate.Median:F0} (min {rate.Min:F0}, max {rate.Max:F0}, samples={rate.SampleCount})"
                : string.Empty;
            Console.WriteLine($"[Raven.Bench] Step {stepNumber} result: {actual:F0} ops/s (target {target:F0}, delta {deltaFormatted}){rollingInfo}");

            if (Math.Abs(deltaPct) > 10.0)
            {
                Console.WriteLine("[Raven.Bench]   note: measured rate deviates >10% from target; check server-side meters (they may count extra system requests) or adjust --rate-workers.");
            }
        }
        else
        {
            Console.WriteLine($"[Raven.Bench] Step {stepNumber} result: concurrency {step.Concurrency}, throughput {step.Throughput:F0}/s");
        }
    }

    internal static int ResolveRateWorkerCount(RunOptions opts, int targetRps, long baselineLatencyMicros)
    {
        return ResolveRateWorkerCount(opts, targetRps, baselineLatencyMicros, observedServiceTimeSeconds: null);
    }

    internal static int ResolveRateWorkerCount(RunOptions opts, int targetRps, long baselineLatencyMicros, double? observedServiceTimeSeconds)
    {
        if (opts == null)
            throw new ArgumentNullException(nameof(opts));

        if (targetRps <= 0)
            return Math.Max(1, opts.RateWorkers ?? 32);

        if (opts.RateWorkers.HasValue)
            return Math.Max(1, opts.RateWorkers.Value);

        const double fallbackBaselineSeconds = 0.002; // Assume 2 ms RTT when calibration is unavailable
        var baselineSeconds = baselineLatencyMicros > 0
            ? Math.Max(baselineLatencyMicros / 1_000_000.0, 1e-6)
            : fallbackBaselineSeconds;

        // Little's Law: concurrency ~= throughput * latency. Add 1.5x headroom to absorb jitter.
        var effectiveSeconds = observedServiceTimeSeconds.HasValue && observedServiceTimeSeconds.Value > 0
            ? Math.Max(observedServiceTimeSeconds.Value, baselineSeconds)
            : baselineSeconds;

        var estimatedConcurrency = targetRps * effectiveSeconds;
        var plannedWorkers = (int)Math.Ceiling(Math.Max(estimatedConcurrency * 1.5, 1));

        const int minWorkers = 32;
        const int maxWorkers = 16384;

        if (plannedWorkers < minWorkers)
            return minWorkers;

        if (plannedWorkers > maxWorkers)
            return maxWorkers;

        return plannedWorkers;
    }



    internal static IWorkload BuildWorkload(RunOptions opts, StackOverflowWorkloadMetadata? stackOverflowMetadata, StackOverflowUsersWorkloadMetadata? usersMetadata, VectorWorkloadMetadata? vectorMetadata)
    {
        if (opts.Profile == WorkloadProfile.Unspecified)
            throw new InvalidOperationException("Workload profile is required. Specify --profile mixed|writes|reads|query-by-id|query-users-by-name.");

        if (opts.Profile != WorkloadProfile.Mixed)
        {
            if (opts.Reads.HasValue || opts.Writes.HasValue || opts.Updates.HasValue)
            {
                throw new InvalidOperationException("--reads/--writes/--updates are only supported with --profile mixed.");
            }
        }

        IKeyDistribution CreateDistribution()
        {
            return opts.Distribution.ToLowerInvariant() switch
            {
                "uniform" => new UniformDistribution(),
                "zipfian" => new ZipfianDistribution(),
                "latest" => new LatestDistribution(),
                _ => throw new NotImplementedException($"Distribution '{opts.Distribution}' is not implemented. Supported distributions: uniform, zipfian, latest")
            };
        }

        return opts.Profile switch
        {
            WorkloadProfile.Mixed => BuildMixedWorkload(opts, CreateDistribution()),
            WorkloadProfile.Writes => new WriteWorkload(opts.DocumentSizeBytes, startingKey: opts.Preload),
            WorkloadProfile.Reads => BuildReadWorkload(opts, CreateDistribution()),
            WorkloadProfile.QueryById => BuildQueryWorkload(opts, CreateDistribution()),
            WorkloadProfile.BulkWrites => new BulkWriteWorkload(opts.DocumentSizeBytes, opts.BulkBatchSize, startingKey: opts.Preload),
            WorkloadProfile.StackOverflowRandomReads => new StackOverflowReadWorkload(stackOverflowMetadata!),
            WorkloadProfile.StackOverflowTextSearch => BuildStackOverflowQueryWorkload(opts, stackOverflowMetadata!),
            WorkloadProfile.QueryUsersByName => BuildUsersQueryWorkload(opts, usersMetadata!),
            WorkloadProfile.VectorSearch => BuildVectorSearchWorkload(opts, vectorMetadata!),
            WorkloadProfile.VectorSearchExact => BuildVectorSearchWorkload(opts, vectorMetadata!, exactSearch: true),
            _ => throw new NotSupportedException($"Unsupported profile: {opts.Profile}")
        };
    }

    private static IWorkload BuildMixedWorkload(RunOptions opts, IKeyDistribution distribution)
    {
        if (opts.Preload <= 0)
            throw new InvalidOperationException("Mixed profile requires preloaded documents. Use --preload to seed data.");

        // Default: 75% reads, 25% updates (no writes - operate on existing data)
        var reads = opts.Reads ?? 75.0;
        var writes = opts.Writes ?? 0.0;
        var updates = opts.Updates ?? 25.0;
        var mix = WorkloadMix.FromWeights(reads, writes, updates);
        return new MixedProfileWorkload(mix, distribution, opts.DocumentSizeBytes, initialKeyspace: opts.Preload);
    }

    private static IWorkload BuildReadWorkload(RunOptions opts, IKeyDistribution distribution)
    {
        if (opts.Preload <= 0)
            throw new InvalidOperationException("Read profile requires --preload to seed the keyspace before the run.");
        return new ReadWorkload(distribution, opts.Preload);
    }

    private static IWorkload BuildQueryWorkload(RunOptions opts, IKeyDistribution distribution)
    {
        if (opts.Preload <= 0)
            throw new InvalidOperationException("Query profile requires --preload to seed the keyspace before the run.");
        return new QueryWorkload(distribution, opts.Preload);
    }

    private static IWorkload BuildStackOverflowQueryWorkload(RunOptions opts, StackOverflowWorkloadMetadata metadata)
    {
        return opts.QueryProfile switch
        {
            QueryProfile.VoronEquality => new StackOverflowQueryWorkload(metadata, useVoronPath: true), // direct Voron lookup via id()
            QueryProfile.IndexEquality => new StackOverflowQueryWorkload(metadata, useVoronPath: false), // index-based lookup
            QueryProfile.TextPrefix => new QuestionsByTitlePrefixWorkload(metadata),
            QueryProfile.TextSearch => new QuestionsByTitleSearchWorkload(metadata, 0.3), // 30% rare, 70% common
            QueryProfile.TextSearchRare => new QuestionsByTitleSearchWorkload(metadata, 1.0), // 100% rare
            QueryProfile.TextSearchCommon => new QuestionsByTitleSearchWorkload(metadata, 0.0), // 100% common
            QueryProfile.TextSearchMixed => new QuestionsByTitleSearchWorkload(metadata, 0.5), // 50% rare, 50% common
            _ => throw new NotSupportedException($"Query profile '{opts.QueryProfile}' is not supported for StackOverflow queries. Supported profiles: voron-equality, index-equality, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed")
        };
    }

    private static IWorkload BuildUsersQueryWorkload(RunOptions opts, StackOverflowUsersWorkloadMetadata metadata)
    {
        return opts.QueryProfile switch
        {
            QueryProfile.VoronEquality or QueryProfile.IndexEquality => new StackOverflowUsersByNameQueryWorkload(metadata),
            QueryProfile.Range => new StackOverflowUsersRangeQueryWorkload(metadata),
            _ => throw new NotSupportedException($"Query profile '{opts.QueryProfile}' is not supported for Users queries. Supported profiles: voron-equality, index-equality, range")
        };
    }

    private static bool IsVectorSearchProfile(WorkloadProfile profile)
    {
        return profile == WorkloadProfile.VectorSearch ||
               profile == WorkloadProfile.VectorSearchExact;
    }

    /// <summary>
    /// Determines if a profile requires preloaded bench/ documents.
    /// Dataset-based profiles (StackOverflow, Users, Vector) use their own imported data.
    /// </summary>
    private static bool ProfileRequiresPreload(WorkloadProfile profile)
    {
        return profile switch
        {
            WorkloadProfile.Mixed => true,
            WorkloadProfile.Reads => true,
            WorkloadProfile.QueryById => true,
            // All other profiles either generate data on-the-fly or use imported datasets
            _ => false
        };
    }

    private static async Task<VectorWorkloadMetadata?> LoadVectorMetadataAsync(RunOptions opts, string database)
    {
        // For vector workloads, we need to load query vectors from the dataset
        var datasetCacheDir = opts.DatasetCacheDir ?? Path.Combine(Path.GetTempPath(), "raven-bench-datasets");
        
        // Determine dataset name from profile or explicit --dataset option
        var datasetName = opts.Dataset ?? GetDefaultDatasetForProfile(opts.Profile);
        
        if (string.IsNullOrEmpty(datasetName))
        {
            throw new InvalidOperationException("Vector search profiles require --dataset option. Supported: clinicalwords100d, clinicalwords300d, clinicalwords600d");
        }

        // For clinicalwords datasets, generate query vectors directly from the provider
        if (datasetName.StartsWith("clinicalwords", StringComparison.OrdinalIgnoreCase))
        {
            int dimensions = 100;
            if (datasetName.Contains("300d")) dimensions = 300;
            else if (datasetName.Contains("600d")) dimensions = 600;
            
            var provider = new Dataset.ClinicalWordsDatasetProvider(dimensions);
            return await provider.GenerateQueryVectorsAsync(count: 1000);
        }

        // Construct path to query vectors file for other datasets
        var queryFilePath = Path.Combine(datasetCacheDir, GetQueryFileName(datasetName));
        
        if (File.Exists(queryFilePath) == false)
        {
            Console.WriteLine($"[Raven.Bench] Query vectors file not found: {queryFilePath}");
            Console.WriteLine($"[Raven.Bench] Please download dataset using --dataset {datasetName} or manually place query vectors in cache directory.");
            return null;
        }

        // Fallback or error for unknown datasets
        throw new NotSupportedException($"Dataset '{datasetName}' is not supported for vector search queries.");
    }

    private static string? GetDefaultDatasetForProfile(WorkloadProfile profile)
    {
        // Dataset is now specified via --dataset parameter, not inferred from profile
        return null;
    }

    private static string GetQueryFileName(string datasetName)
    {
        var name = datasetName.ToLowerInvariant();
        if (name.StartsWith("clinicalwords"))
            return $"{name}_queries.json";
        throw new NotSupportedException($"Unknown dataset: {datasetName}");
    }

    private static IWorkload BuildVectorSearchWorkload(
        RunOptions opts, 
        VectorWorkloadMetadata metadata, 
        bool exactSearch = false,
        VectorQuantization? quantization = null)
    {
        var effectiveQuantization = quantization ?? opts.VectorQuantization;
        var effectiveExactSearch = exactSearch || opts.VectorExactSearch;
        
        return new VectorSearchWorkload(
            metadata,
            topK: opts.VectorTopK,
            minimumSimilarity: opts.VectorMinSimilarity,
            useExactSearch: effectiveExactSearch,
            quantization: effectiveQuantization);
    }

    private static ITransport BuildTransport(RunOptions opts, Version negotiatedHttpVersion, string? databaseOverride = null)
    {
        var database = databaseOverride ?? opts.Database;

        if (opts.Transport == "raw")
        {
            Console.WriteLine($"[Raven.Bench] Transport: Raw HTTP with {opts.Compression} compression");
            return new RawHttpTransport(opts.Url, database, opts.Compression, negotiatedHttpVersion, opts.RawEndpoint);
        }

        if (opts.Transport == "client")
        {
            Console.WriteLine($"[Raven.Bench] Transport: RavenDB Client with {opts.Compression} compression");
            return new RavenClientTransport(opts.Url, database, opts.Compression, negotiatedHttpVersion);
        }

        Console.WriteLine("[Raven.Bench] Transport: Raw HTTP with identity compression (default)");
        return new RawHttpTransport(opts.Url, database, "identity", negotiatedHttpVersion);
    }

    private static async Task PreloadAsync(ITransport transport, RunOptions opts, int count, Random rng, int docSize)
    {
        // Check if documents already exist
        var existingCount = await transport.GetDocumentCountAsync("bench/");

        if (existingCount >= count)
        {
            Console.WriteLine($"[Raven.Bench] Database already has {existingCount} documents (>= {count} requested). Skipping preload.");
            return;
        }

        Console.WriteLine($"[Raven.Bench] Preloading {count} documents...");

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = 32
        };

        await Parallel.ForEachAsync(
            Enumerable.Range(1, count),
            options,
            async (i, ct) =>
            {
                try
                {
                    var id = IdFor(i);
                    await transport.PutAsync(id, PayloadGenerator.Generate(docSize, rng));
                }
                catch
                {
                    // ignore individual preload failures
                }
            });

        Console.WriteLine("[Raven.Bench] Preload complete.");
    }

    private static async Task WaitForNonStaleIndexesAsync(string serverUrl, string databaseName, Version httpVersion)
    {
        Console.WriteLine("[Raven.Bench] Waiting for indexes to become non-stale...");

        using var store = new DocumentStore
        {
            Urls = new[] { serverUrl },
            Database = databaseName
        };
        HttpHelper.ConfigureHttpVersion(store, httpVersion, HttpVersionPolicy.RequestVersionExact);
        store.Initialize();

        var maxWait = TimeSpan.FromMinutes(10);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        while (sw.Elapsed < maxWait)
        {
            var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
            var staleIndexes = stats.Indexes.Where(i => i.IsStale).ToList();

            if (staleIndexes.Count == 0)
            {
                Console.WriteLine($"[Raven.Bench] All indexes are non-stale (waited {sw.Elapsed.TotalSeconds:F1}s)");
                return;
            }

            Console.WriteLine($"[Raven.Bench] {staleIndexes.Count} stale index(es), waiting... ({sw.Elapsed.TotalSeconds:F0}s elapsed)");
            await Task.Delay(2000);
        }

        Console.WriteLine($"[Raven.Bench] WARNING: Indexes still stale after {maxWait.TotalMinutes} minutes");
    }

    private static async Task<string> ImportDatasetAsync(RunOptions opts)
    {
        Console.WriteLine($"[Raven.Bench] Dataset import requested: {opts.Dataset}");

        var datasetManager = new Dataset.DatasetManager(opts.DatasetCacheDir);

        // Determine database name and dataset size based on profile or custom size
        string targetDatabase;
        int datasetSize;

        if (string.IsNullOrEmpty(opts.DatasetProfile) == false)
        {
            // Use predefined profile
            var profile = Enum.Parse<Dataset.DatasetProfile>(opts.DatasetProfile, ignoreCase: true);
            targetDatabase = Dataset.KnownDatasets.GetDatabaseName(profile);
            datasetSize = Dataset.KnownDatasets.GetDatasetSize(profile);
            Console.WriteLine($"[Raven.Bench] Using dataset profile '{opts.DatasetProfile}': {targetDatabase} (~{(datasetSize == 0 ? 50 : datasetSize + 2)}GB)");
        }
        else if (opts.DatasetSize > 0 || opts.DatasetSize == 0)
        {
            // Use custom size - generate database name based on size
            targetDatabase = Dataset.KnownDatasets.GetDatabaseNameForSize(opts.DatasetSize);
            datasetSize = opts.DatasetSize;
            Console.WriteLine($"[Raven.Bench] Using custom dataset size: {targetDatabase} (~{(datasetSize == 0 ? 50 : datasetSize + 2)}GB)");
        }
        else
        {
            // Fallback to user-specified database name
            targetDatabase = opts.Database;
            datasetSize = 0;
        }

        // Check if dataset already exists
        if (opts.DatasetSkipIfExists)
        {
            var exists = await datasetManager.IsStackOverflowDatasetImportedAsync(opts.Url, targetDatabase, opts.HttpVersion, expectedMinDocuments: 10000);
            if (exists)
            {
                Console.WriteLine($"[Raven.Bench] Dataset appears to already exist in database '{targetDatabase}'. Skipping import.");
                Console.WriteLine($"[Raven.Bench] Use --dataset-skip-if-exists=false to force re-import.");
                return targetDatabase; // Return database name even if skipping import
            }
        }

        // Get dataset info
        Dataset.DatasetInfo? dataset;
        if (datasetSize > 0)
        {
            // Partial dataset
            Console.WriteLine($"[Raven.Bench] Importing partial dataset with {datasetSize} post dump files to '{targetDatabase}'");
            dataset = Dataset.KnownDatasets.StackOverflowPartial(datasetSize);
        }
        else
        {
            // Full dataset
            Console.WriteLine($"[Raven.Bench] Importing full dataset to '{targetDatabase}'");
            dataset = Dataset.KnownDatasets.GetByName(opts.Dataset!);
        }

        if (dataset == null)
        {
            throw new ArgumentException($"Unknown dataset: {opts.Dataset}. Supported: stackoverflow, clinicalwords100d, clinicalwords300d, clinicalwords600d");
        }

        // Download and import to the target database
        await datasetManager.ImportDatasetAsync(dataset, opts.Url, targetDatabase, opts.HttpVersion);

        // Return the target database name so the runner can use it
        return targetDatabase;
    }

    private static async Task<(string database, bool imported)> ImportClinicalWordsDatasetAsync(RunOptions opts, Version httpVersion)
    {
        // Parse dimensions from dataset name (e.g., "clinicalwords100d" -> 100)
        var datasetName = opts.Dataset!.ToLowerInvariant();
        int dimensions = 100; // default
        if (datasetName.Contains("300d")) dimensions = 300;
        else if (datasetName.Contains("600d")) dimensions = 600;

        var provider = new Dataset.ClinicalWordsDatasetProvider(dimensions);
        var targetDatabase = provider.GetDatabaseName();

        Console.WriteLine($"[Raven.Bench] ClinicalWords{dimensions}D dataset -> '{targetDatabase}'");

        // Check if data already imported to database
        if (opts.DatasetSkipIfExists)
        {
            Console.WriteLine($"[Raven.Bench] Checking if data already imported...");
            var exists = await provider.IsDatasetImportedAsync(opts.Url, targetDatabase, expectedMinDocuments: Dataset.ClinicalWordsDatasetProvider.MinExpectedDocuments, httpVersion: httpVersion);
            if (exists)
            {
                Console.WriteLine($"[Raven.Bench] ClinicalWords{dimensions}D already imported. Ready to use.");
                return (targetDatabase, imported: false);
            }
            Console.WriteLine($"[Raven.Bench] Data not found or incomplete, will import.");
        }

        // Import words as documents with embeddings
        Console.WriteLine($"[Raven.Bench] Importing clinical word embeddings to RavenDB (engine: {opts.SearchEngine})...");
        var exactSearch = opts.Profile == WorkloadProfile.VectorSearchExact || opts.VectorExactSearch;
        await provider.ImportWordsAsync(opts.Url, targetDatabase, opts.VectorQuantization, exactSearch, httpVersion: httpVersion, searchEngine: opts.SearchEngine);

        Console.WriteLine($"[Raven.Bench] ClinicalWords{dimensions}D import complete.");

        return (targetDatabase, imported: true);
    }

    private static string IdFor(int i) => $"bench/{i:D8}";

    /// <summary>
    /// Runs a closed-loop benchmark step with simplified parameter passing via context objects.
    /// </summary>
    internal async Task<(StepResult result, LatencyRecorder hist)> RunClosedLoopAsync(BenchmarkContext context, StepParameters step)
    {
        using var cts = new CancellationTokenSource(step.Duration);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        long success = 0, errors = 0, bytesOut = 0, bytesIn = 0;

        // Query metadata tracking (thread-safe)
        long queryOps = 0;
        var indexUsage = new ConcurrentDictionary<string, long>();
        long totalResults = 0;
        int minResultCount = int.MaxValue;
        int maxResultCount = int.MinValue;
        long staleQueries = 0;

        var hist = new LatencyRecorder(recordLatencies: step.Record);

        context.CpuTracker.Reset();
        context.CpuTracker.Start();
        
        // Start server metrics tracking
        context.ServerTracker.Start();
        
        // Create concurrent worker tasks for closed-loop benchmark execution
        var tasks = new Task[step.Concurrency];
        for (int i = 0; i < step.Concurrency; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                await started.Task.ConfigureAwait(false);
                var rnd = new Random(context.Rng.Next());

                // Coordinated omission correction: use baseline latency as expected interval
                // When a request takes longer than baseline, HDRHistogram backfills synthetic samples
                var baseline = step.BaselineLatencyMicros;

                while (cts.IsCancellationRequested == false)
                {
                    var op = context.Workload.NextOperation(rnd);
                    var t0 = Stopwatch.GetTimestamp();
                    try
                    {
                        var res = await context.Transport.ExecuteAsync(op, cts.Token).ConfigureAwait(false);
                        var us = ElapsedMicros(t0);
                        if (step.Record)
                        {
                            // Apply coordinated omission correction if baseline is available
                            if (baseline > 0)
                                hist.RecordWithExpectedInterval(us, baseline);
                            else
                                hist.Record(us);
                        }

                        if (res.IsSuccess)
                        {
                            Interlocked.Increment(ref success);
                            Interlocked.Add(ref bytesOut, res.BytesOut);
                            Interlocked.Add(ref bytesIn, res.BytesIn);

                            // Capture query metadata if available
                            if (res.IndexName != null || res.ResultCount.HasValue || res.IsStale.HasValue)
                            {
                                Interlocked.Increment(ref queryOps);

                                if (res.IndexName != null)
                                {
                                    indexUsage.AddOrUpdate(res.IndexName, 1, (_, count) => count + 1);
                                }

                                if (res.ResultCount.HasValue)
                                {
                                    Interlocked.Add(ref totalResults, res.ResultCount.Value);

                                    // Update min/max using lock-free compare-and-swap pattern
                                    int currentMin, currentMax;
                                    do
                                    {
                                        currentMin = minResultCount;
                                    } while (res.ResultCount.Value < currentMin &&
                                             Interlocked.CompareExchange(ref minResultCount, res.ResultCount.Value, currentMin) != currentMin);

                                    do
                                    {
                                        currentMax = maxResultCount;
                                    } while (res.ResultCount.Value > currentMax &&
                                             Interlocked.CompareExchange(ref maxResultCount, res.ResultCount.Value, currentMax) != currentMax);
                                }

                                if (res.IsStale == true)
                                {
                                    Interlocked.Increment(ref staleQueries);
                                }
                            }
                        }
                        else
                        {
                            Interlocked.Increment(ref errors);
                            VerboseErrorTracker.LogError(res.ErrorDetails ?? string.Empty, opts.Verbose);
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        // We do not increment errors because we are asking for it. 
                    }
                    catch (Exception)
                    {
                        Interlocked.Increment(ref errors);
                    }
                }
            });
        }

        started.SetResult();
        await Task.WhenAll(tasks).ConfigureAwait(false);

        context.CpuTracker.Stop();
        var cpu = context.CpuTracker.AverageCpu;
        
        // Stop server metrics tracking and get final metrics
        context.ServerTracker.Stop();
        var serverMetrics = context.ServerTracker.Current;

        var ops = success + errors;
        var thr = ops / step.Duration.TotalSeconds;
        var errRate = ops > 0 ? (double)errors / ops : 0.0;
        var netBps = (bytesOut + bytesIn) / step.Duration.TotalSeconds * 8.0; // bits per second
        var linkBps = opts.LinkMbps * 1_000_000.0;
        var netUtil = linkBps > 0 ? netBps / linkBps : 0.0;

        // Prepare query metadata for StepResult if any query operations were tracked
        IReadOnlyDictionary<string, long>? finalIndexUsage = null;
        List<IndexUsageSummary>? topIndexes = null;
        int? finalMinResultCount = null;
        int? finalMaxResultCount = null;
        double? finalAvgResultCount = null;

        if (queryOps > 0)
        {
            finalIndexUsage = new Dictionary<string, long>(indexUsage);

            // Build top-N index summary (top 5 indexes by usage)
            topIndexes = indexUsage
                .OrderByDescending(kvp => kvp.Value)
                .Take(5)
                .Select(kvp => new IndexUsageSummary
                {
                    IndexName = kvp.Key,
                    UsageCount = kvp.Value,
                    UsagePercent = queryOps > 0 ? (double)kvp.Value / queryOps * 100.0 : null
                })
                .ToList();

            if (minResultCount != int.MaxValue)
                finalMinResultCount = minResultCount;
            if (maxResultCount != int.MinValue)
                finalMaxResultCount = maxResultCount;
            if (queryOps > 0)
                finalAvgResultCount = (double)totalResults / queryOps;
        }

        var stepResult = new StepResult
        {
            Concurrency = step.Concurrency,
            Throughput = thr,
            ErrorRate = errRate,
            BytesOut = bytesOut,
            BytesIn = bytesIn,

            // SampleCount tracks actual operations observed (before coordinated omission correction)
            // This is the total number of operations (success + errors) that completed and were recorded
            // in the histogram. Must match histogram's raw sample count before CO backfilling.
            SampleCount = ops,

            ClientCpu = cpu, // 0..1
            NetworkUtilization = netUtil,
            ServerCpu = serverMetrics.CpuUsagePercent,
            ServerMemoryMB = serverMetrics.MemoryUsageMB,
            ServerRequestsPerSec = serverMetrics.RequestsPerSecond,
            ServerIoReadOps = serverMetrics.SnmpIoReadOpsPerSec.HasValue ? (long)serverMetrics.SnmpIoReadOpsPerSec.Value : null,
            ServerIoWriteOps = serverMetrics.SnmpIoWriteOpsPerSec.HasValue ? (long)serverMetrics.SnmpIoWriteOpsPerSec.Value : null,
            ServerIoReadKb = serverMetrics.ReadThroughputKb,
            ServerIoWriteKb = serverMetrics.WriteThroughputKb,

            // SNMP gauge metrics
            MachineCpu = serverMetrics.MachineCpu,
            ProcessCpu = serverMetrics.ProcessCpu,
            ManagedMemoryMb = serverMetrics.ManagedMemoryMb,
            UnmanagedMemoryMb = serverMetrics.UnmanagedMemoryMb,
            DirtyMemoryMb = serverMetrics.DirtyMemoryMb,
            Load1Min = serverMetrics.Load1Min,
            Load5Min = serverMetrics.Load5Min,
            Load15Min = serverMetrics.Load15Min,

            // SNMP rate metrics
            SnmpIoReadOpsPerSec = serverMetrics.SnmpIoReadOpsPerSec,
            SnmpIoReadBytesPerSec = serverMetrics.SnmpIoReadBytesPerSec,
            SnmpIoWriteBytesPerSec = serverMetrics.SnmpIoWriteBytesPerSec,
            ServerSnmpRequestsPerSec = serverMetrics.ServerSnmpRequestsPerSec,
            SnmpErrorsPerSec = serverMetrics.SnmpErrorsPerSec,

            // Query metadata (populated for query workload profiles)
            QueryOperations = queryOps > 0 ? queryOps : null,
            IndexUsage = finalIndexUsage,
            TopIndexes = topIndexes,
            MinResultCount = finalMinResultCount,
            MaxResultCount = finalMaxResultCount,
            AvgResultCount = finalAvgResultCount,
            TotalResults = queryOps > 0 ? totalResults : null,
            StaleQueryCount = staleQueries > 0 ? staleQueries : null,
            QueryProfile = opts.QueryProfile != QueryProfile.VoronEquality ? opts.QueryProfile : null, // Only include if not default
        };
        
        return (stepResult, hist);
    }

    private static long ElapsedMicros(long t0)
    {
        var ticks = Stopwatch.GetTimestamp() - t0;
        return (long)(ticks * 1_000_000.0 / Stopwatch.Frequency);
    }

    /// <summary>
    /// Validates client can connect to the server and rejects invalid clients.
    /// This is a hard validation that will terminate the benchmark if the client is not valid.
    /// </summary>
    private async Task ValidateClientAsync(ITransport transport)
    {
        try
        {
            if (transport is RawHttpTransport rawTransport)
            {
                await rawTransport.ValidateClientAsync(opts.StrictHttpVersion);
            }
            else
            {
                await transport.ValidateClientAsync();
            }
            Console.WriteLine("[Raven.Bench] Client validation successful");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed. Benchmark cannot proceed with invalid client: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Validates server configuration matches expectations to catch environment issues early.
    /// Checks server core limits to ensure benchmark runs in expected conditions.
    /// </summary>
    private async Task ValidateServerSanityAsync(ITransport transport)
    {
        try
        { 
            // Display RavenDB server version and license type
            var serverVersion = await transport.GetServerVersionAsync();
            var licenseType = await transport.GetServerLicenseTypeAsync();
            var maxCores = await transport.GetServerMaxCoresAsync();
            Console.WriteLine($"[Raven.Bench] RavenDB Server Version: {serverVersion}");
            Console.WriteLine($"[Raven.Bench] License Type: {licenseType}");
            Console.WriteLine($"[Raven.Bench] Max CPU Cores: {(maxCores?.ToString() ?? "unlimited")}");

            // Display effective HTTP version
            if (transport is RawHttpTransport rawTransport)
            {
                Console.WriteLine($"[Raven.Bench] HTTP Version: {rawTransport.EffectiveHttpVersion}");
            }
            
            if (opts.ExpectedCores.HasValue)
            {
                if (maxCores.HasValue && maxCores.Value != opts.ExpectedCores.Value)
                {
                    Console.WriteLine($"[Raven.Bench] Warning: Server core limit={maxCores} differs from expected={opts.ExpectedCores}");
                }
            }
        }
        catch (Exception ex)
        {
            // non-fatal - continue with benchmark
            Console.WriteLine($"[Raven.Bench] Warning: Server validation failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Validates SNMP connectivity if SNMP is enabled in options.
    /// Throws exception if SNMP is enabled but fails to retrieve metrics - SNMP must work when explicitly enabled.
    /// </summary>
    private async Task ValidateSnmpAsync(ITransport transport)
    {
        if (opts.Snmp.Enabled == false)
            return;

        Console.WriteLine($"[Raven.Bench] Validating SNMP connectivity (profile: {opts.Snmp.Profile})...");

        try
        {
            var snmpSample = await transport.GetSnmpMetricsAsync(opts.Snmp, opts.Database);

            if (snmpSample.IsEmpty == false)
            {
                Console.WriteLine("[Raven.Bench] SNMP validation successful:");
                if (snmpSample.MachineCpu.HasValue)
                    Console.WriteLine($"[Raven.Bench]   Machine CPU: {snmpSample.MachineCpu.Value}%");
                if (snmpSample.ProcessCpu.HasValue)
                    Console.WriteLine($"[Raven.Bench]   Process CPU: {snmpSample.ProcessCpu.Value}%");
                if (snmpSample.ManagedMemoryMb.HasValue)
                    Console.WriteLine($"[Raven.Bench]   Managed Memory: {snmpSample.ManagedMemoryMb.Value} MB");
                if (snmpSample.UnmanagedMemoryMb.HasValue)
                    Console.WriteLine($"[Raven.Bench]   Unmanaged Memory: {snmpSample.UnmanagedMemoryMb.Value} MB");
                if (snmpSample.IoWriteOpsPerSec.HasValue)
                    Console.WriteLine($"[Raven.Bench]   IO Write Ops/sec: {snmpSample.IoWriteOpsPerSec.Value}");
                if (snmpSample.IoReadOpsPerSec.HasValue)
                    Console.WriteLine($"[Raven.Bench]   IO Read Ops/sec: {snmpSample.IoReadOpsPerSec.Value}");
            }
            else
            {
                throw new InvalidOperationException(
                    "SNMP is enabled but no metrics were retrieved. Possible causes:\n" +
                    $"  - SNMP service not running on server\n" +
                    $"  - Firewall blocking SNMP port {opts.Snmp.Port}\n" +
                    $"  - Community string mismatch (RavenDB uses 'ravendb')\n" +
                    "  - Server SNMP not enabled (set Monitoring.Snmp.Enabled=true in server settings.json)\n" +
                    "\nBenchmark cannot proceed with SNMP enabled but unavailable.");
            }
        }
        catch (InvalidOperationException)
        {
            // Re-throw validation failures
            throw;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"SNMP validation failed: {ex.Message}\n" +
                "Benchmark cannot proceed with SNMP enabled but unavailable.", ex);
        }
    }
}
