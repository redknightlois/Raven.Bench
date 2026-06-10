using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Diagnostics;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using Spectre.Console;
using RavenBench.Dataset;

namespace RavenBench;

public class BenchmarkRunner(RunOptions opts)
{
    private readonly Random _rng = new(opts.Seed);

    public async Task<BenchmarkRun> RunAsync()
    {
        VerboseErrorTracker.Reset();
        LoadGeneratorExecution.ResetErrorTracking();
        LoadGeneratorExecution.OnFirstError = msg => Console.WriteLine($"[Raven.Bench] Error (first occurrence): {msg}");

        if (WorkloadProfiles.SupportsEngine(opts.Profile, opts.SearchEngine) == false)
        {
            var supported = string.Join(", ", WorkloadProfiles.GetSupportedEngines(opts.Profile).Select(e => e.ToString().ToLowerInvariant()));
            throw new InvalidOperationException(
                $"Profile '{opts.Profile}' does not support {opts.SearchEngine} indexing engine. " +
                $"Supported engines: {supported}.");
        }

        int workers = opts.ThreadPoolWorkers;
        int iocp = opts.ThreadPoolIOCP;

        Console.WriteLine($"[Raven.Bench] Setting ThreadPool: workers={workers}, iocp={iocp}");
        ThreadPool.SetMinThreads(workers, iocp);

        Console.WriteLine("[Raven.Bench] Negotiating HTTP version...");
        var negotiatedHttpVersion = await HttpVersionNegotiator.NegotiateVersionAsync(
            opts.Url,
            opts.HttpVersion,
            opts.StrictHttpVersion);
        Console.WriteLine($"[Raven.Bench] Using HTTP/{HttpHelper.FormatHttpVersion(negotiatedHttpVersion)}");

        // Dataset import may override the database name
        string? datasetDatabase = null;
        bool datasetWasImported = false;
        if (string.IsNullOrEmpty(opts.Dataset) == false)
        {
            if (opts.Dataset.StartsWith("clinicalwords", StringComparison.OrdinalIgnoreCase))
            {
                var (database, imported) = await DatasetImportCoordinator.ImportClinicalWordsDatasetAsync(opts, negotiatedHttpVersion);
                datasetDatabase = database;
                datasetWasImported = imported;
            }
            else if (opts.Dataset.StartsWith("sphere", StringComparison.OrdinalIgnoreCase))
            {
                var (database, imported) = await DatasetImportCoordinator.ImportSphereDatasetAsync(opts, negotiatedHttpVersion);
                datasetDatabase = database;
                datasetWasImported = imported;
            }
            else
            {
                datasetDatabase = await DatasetImportCoordinator.ImportDatasetAsync(opts);
                datasetWasImported = true;
            }

            if (datasetDatabase != opts.Database)
            {
                Console.WriteLine($"[Raven.Bench] Using dataset-specific database: '{datasetDatabase}'");
            }
        }

        var effectiveDatabase = datasetDatabase ?? opts.Database;

        using var transport = BuildTransport(opts, negotiatedHttpVersion, effectiveDatabase);

        Console.WriteLine($"[Raven.Bench] Ensuring database '{effectiveDatabase}' exists...");
        await transport.EnsureDatabaseExistsAsync(effectiveDatabase);

        if (datasetWasImported)
        {
            await DatasetImportCoordinator.WaitForNonStaleIndexesAsync(opts.Url, effectiveDatabase, negotiatedHttpVersion);
        }

        // Static indexes must exist before metadata discovery so index names can be set on the metadata
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

            if (staticIndexNames != null)
            {
                stackOverflowMetadata.TitleIndexName = staticIndexNames.QuestionsTitleIndex;
                stackOverflowMetadata.TitleSearchIndexName = staticIndexNames.QuestionsTitleSearchIndex;
            }
        }

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

            if (staticIndexNames != null)
            {
                usersMetadata.DisplayNameIndexName = staticIndexNames.UsersDisplayNameIndex;
                usersMetadata.ReputationIndexName = staticIndexNames.UsersReputationIndex;
            }
        }

        VectorWorkloadMetadata? vectorMetadata = null;
        if (WorkloadFactory.IsVectorSearchProfile(opts.Profile))
        {
            vectorMetadata = await DatasetImportCoordinator.LoadVectorMetadataAsync(opts);
            if (vectorMetadata == null)
            {
                throw new InvalidOperationException("Vector metadata not available. Ensure vector dataset is imported or specify --dataset-cache-dir with query vectors.");
            }
        }

        if (vectorMetadata?.IndexName != null)
        {
            await DatasetImportCoordinator.EnsureVectorIndexExistsAsync(transport, opts, vectorMetadata, effectiveDatabase);
        }

        var workload = WorkloadFactory.BuildWorkload(opts, stackOverflowMetadata, usersMetadata, vectorMetadata);

        if (opts.Preload > 0 && WorkloadFactory.ProfileRequiresPreload(opts.Profile))
            await PreloadAsync(transport, opts, opts.Preload, opts.DocumentSizeBytes);
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
        string httpVersion = HttpHelper.FormatHttpVersion(negotiatedHttpVersion);

        await ValidateClientAsync(transport);
        await ValidateServerSanityAsync(transport);
        await ValidateSnmpAsync(transport);

        try
        {
            Console.WriteLine("[Raven.Bench] Running startup calibration...");

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
                    Console.WriteLine($"[Raven.Bench]   Database: {effectiveDatabase}");
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

        var executor = new BenchmarkExecutor(opts, transport, workload, cpuTracker, serverTracker);

        double? observedServiceTimeSeconds = null;
        int? previousAutoRateWorkers = null;

        while (currentValue <= endValue)
        {
            // Baseline latency for coordinated omission correction, in µs
            var baselineLatencyMicros = startupCalibration?.Endpoints.Count > 0
                ? (long)(startupCalibration.Endpoints.Min(e => e.ObservedMs) * 1000)
                : 0L;

            var rateWorkerCount = opts.Shape == LoadShape.Rate
                ? RateWorkerPlanner.ResolveRateWorkerCount(opts, (int)currentValue, baselineLatencyMicros, observedServiceTimeSeconds)
                : 0;

            if (opts.Shape == LoadShape.Rate && opts.RateWorkers.HasValue == false && previousAutoRateWorkers.HasValue)
            {
                // Auto worker growth is capped at 2x per step to absorb transient tail-latency spikes.
                rateWorkerCount = Math.Min(rateWorkerCount, previousAutoRateWorkers.Value * 2);
            }

            ILoadGenerator loadGenerator = opts.Shape switch
            {
                LoadShape.Rate => new RateLoadGenerator(transport, workload, (int)currentValue, rateWorkerCount, _rng),
                LoadShape.Closed => new ClosedLoopLoadGenerator(transport, workload, (int)currentValue, _rng),
                _ => new ClosedLoopLoadGenerator(transport, workload, (int)currentValue, _rng)
            };

            LogStepStart(opts.Shape, steps.Count + 1, (int)currentValue, rateWorkerCount, opts);

            var (latencyRecorder, stepResult) = await executor.ExecuteStepAsync(loadGenerator, steps.Count, (int)currentValue, CancellationToken.None, baselineLatencyMicros);

            var snapshot = latencyRecorder.Snapshot();

            if (opts.Shape == LoadShape.Rate && opts.RateWorkers.HasValue == false)
            {
                // Workers for the next step are sized from measured service time, not baseline RTT,
                // which under-drives the client when per-op CPU/serialization dominates.
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

            int[] percentiles = { 50, 75, 90, 95, 99, 999 };
            var rawValues = new double[6];
            for (int i = 0; i < percentiles.Length; i++)
            {
                rawValues[i] = snapshot.GetPercentile(percentiles[i]) / 1000.0;
            }

            var p9999 = snapshot.GetPercentile(99.99) / 1000.0;
            var pMax = snapshot.MaxMicros / 1000.0;

            var rawPercentiles = new Percentiles(rawValues[0], rawValues[1], rawValues[2], rawValues[3], rawValues[4], rawValues[5]);

            // Normalized = raw minus baseline RTT (additional latency due to load); raw when calibration is unavailable
            Percentiles normalizedPercentiles;
            if (startupCalibration?.Endpoints.Count > 0)
            {
                var baselineRttMs = startupCalibration.Endpoints.Min(e => e.ObservedMs);
                var normalizedValues = new double[6];
                for (int i = 0; i < rawValues.Length; i++)
                {
                    normalizedValues[i] = Math.Max(0, rawValues[i] - baselineRttMs);
                }
                normalizedPercentiles = new Percentiles(normalizedValues[0], normalizedValues[1], normalizedValues[2], normalizedValues[3], normalizedValues[4], normalizedValues[5]);
            }
            else
            {
                normalizedPercentiles = rawPercentiles;
            }

            stepResult.Raw = rawPercentiles;
            stepResult.Normalized = normalizedPercentiles;
            stepResult.P9999 = p9999;
            stepResult.PMax = pMax;
            stepResult.CorrectedCount = snapshot.TotalCount;

            if (startupCalibration?.Endpoints.Count > 0)
            {
                var baselineRttMs = startupCalibration.Endpoints.Min(e => e.ObservedMs);
                stepResult.NormalizedP9999 = Math.Max(0, p9999 - baselineRttMs);
                stepResult.NormalizedPMax = Math.Max(0, pMax - baselineRttMs);
            }
            else
            {
                stepResult.NormalizedP9999 = p9999;
                stepResult.NormalizedPMax = pMax;
            }

            var artifact = HistogramExporter.BuildHistogramArtifact(snapshot, stepResult.Concurrency, opts.LatencyHistogramsDir, opts.LatencyHistogramsFormat);
            if (artifact != null)
            {
                histogramArtifacts.Add(artifact);
            }

            steps.Add(stepResult);
            LogStepResult(steps.Count, stepResult);
            maxNetUtil = Math.Max(maxNetUtil, stepResult.NetworkUtilization);

            if (stepResult.ErrorRate > Math.Max(opts.MaxErrorRate, 0.05))
            {
                Console.WriteLine("[Raven.Bench] High error rate; stopping ramp.");
                break;
            }

            if (opts.Shape == LoadShape.Rate && stepResult.TargetThroughput.HasValue)
            {
                var target = stepResult.TargetThroughput.Value;
                var actual = stepResult.Throughput;
                var deltaPct = (actual - target) / target * 100.0;

                if (deltaPct < -30.0)
                {
                    Console.WriteLine($"[Raven.Bench] Throughput is {Math.Abs(deltaPct):F1}% below target ({actual:F0} vs {target:F0}). Server appears saturated; stopping ramp.");
                    break;
                }

                if (steps.Count >= 2)
                {
                    var prevStep = steps[steps.Count - 2];
                    var throughputDrop = (stepResult.Throughput - prevStep.Throughput) / prevStep.Throughput * 100.0;

                    if (throughputDrop < -30.0)
                    {
                        Console.WriteLine($"[Raven.Bench] Throughput degraded by {Math.Abs(throughputDrop):F1}% from previous step ({stepResult.Throughput:F0} vs {prevStep.Throughput:F0}). Server appears overloaded; stopping ramp.");
                        break;
                    }
                }
            }

            if (stepResult.P9999 > 30_000.0)
            {
                Console.WriteLine($"[Raven.Bench] Extreme latencies detected (p99.9={stepResult.P9999:F0}ms). Server severely degraded; stopping ramp.");
                break;
            }

            currentValue = stepPlan.Next(currentValue);
        }

        if (opts.Verbose)
        {
            VerboseErrorTracker.PrintSummary();
        }

        var serverMetricsHistory = serverTracker.GetHistory();

        return new BenchmarkRun
        {
            Steps = steps,
            MaxNetworkUtilization = maxNetUtil,
            ClientCompression = clientCompression,
            EffectiveHttpVersion = httpVersion,
            StartupCalibration = startupCalibration,
            ServerMetricsHistory = serverMetricsHistory.Count > 0 ? serverMetricsHistory : null,
            HistogramArtifacts = histogramArtifacts.Count > 0 ? histogramArtifacts : null,
            VectorMetadata = vectorMetadata,
            EffectiveDatabase = effectiveDatabase
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

    private static ITransport BuildTransport(RunOptions opts, Version negotiatedHttpVersion, string? databaseOverride = null)
    {
        var database = databaseOverride ?? opts.Database;

        switch (opts.Transport)
        {
            case TransportKind.Raw:
                Console.WriteLine($"[Raven.Bench] Transport: Raw HTTP with {opts.Compression} compression");
                return new RawHttpTransport(opts.Url, database, opts.Compression, negotiatedHttpVersion, opts.RawEndpoint);
            case TransportKind.Client:
                Console.WriteLine($"[Raven.Bench] Transport: RavenDB Client with {opts.Compression} compression");
                return new RavenClientTransport(opts.Url, database, opts.Compression, negotiatedHttpVersion);
            default:
                throw new ArgumentOutOfRangeException(nameof(opts.Transport), opts.Transport, null);
        }
    }

    private static async Task PreloadAsync(ITransport transport, RunOptions opts, int count, int docSize)
    {
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

        var failures = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(1, count),
            options,
            async (i, ct) =>
            {
                try
                {
                    var id = BenchIds.IdFor(i);
                    var rng = new Random(opts.Seed + i);
                    await transport.PutAsync(id, PayloadGenerator.Generate(docSize, rng));
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failures);
                    VerboseErrorTracker.LogError(ex.Message, opts.Verbose);
                }
            });

        if (failures > 0)
            throw new InvalidOperationException($"Preload failed: {failures} of {count} document writes failed.");

        Console.WriteLine("[Raven.Bench] Preload complete.");
    }

    /// <summary>
    /// Validates client can connect to the server and rejects invalid clients.
    /// This is a hard validation that will terminate the benchmark if the client is not valid.
    /// </summary>
    private async Task ValidateClientAsync(ITransport transport)
    {
        try
        {
            await transport.ValidateClientAsync();
            Console.WriteLine("[Raven.Bench] Client validation successful");
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Client validation failed. Benchmark cannot proceed with invalid client: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// Validates server configuration matches expectations to catch environment issues early.
    /// </summary>
    private async Task ValidateServerSanityAsync(ITransport transport)
    {
        try
        {
            var serverVersion = await transport.GetServerVersionAsync();
            var licenseType = await transport.GetServerLicenseTypeAsync();
            var maxCores = await transport.GetServerMaxCoresAsync();
            Console.WriteLine($"[Raven.Bench] RavenDB Server Version: {serverVersion}");
            Console.WriteLine($"[Raven.Bench] License Type: {licenseType}");
            Console.WriteLine($"[Raven.Bench] Max CPU Cores: {(maxCores?.ToString() ?? "unlimited")}");

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
            // non-fatal
            Console.WriteLine($"[Raven.Bench] Warning: Server validation failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Validates SNMP connectivity if SNMP is enabled in options.
    /// SNMP must work when explicitly enabled; failure aborts the benchmark.
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
