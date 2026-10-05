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
            else if (Dataset.Vectors.VectorSets.FindPublished(opts.Dataset) is { } published)
            {
                var (database, imported) = await DatasetImportCoordinator.ImportPublishedSetAsync(opts, published, negotiatedHttpVersion);
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

        var effectiveDatabase = datasetDatabase ?? opts.Database
            ?? throw new InvalidOperationException("No database: pass --database, or --dataset to name one.");

        using var nodeExporter = await NodeExporterClient.ConnectAsync(opts.NodeExporterUrl);
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
            var extraIndexes = opts.QueryProfile switch
            {
                QueryProfile.Spatial => new[] { StackOverflowIndex.UsersBySpatial },
                QueryProfile.Suggestions => new[] { StackOverflowIndex.QuestionsByTitleSuggestions },
                QueryProfile.MoreLikeThis => new[] { StackOverflowIndex.QuestionsByTitleMoreLikeThis },
                QueryProfile.GroupBy => new[] { StackOverflowIndex.QuestionsByViewCountGrouped },
                QueryProfile.Stream => new[] { StackOverflowIndex.QuestionsByTags },
                _ => Array.Empty<StackOverflowIndex>()
            };

            var stackOverflowProvider = new StackOverflowDatasetProvider();
            staticIndexNames = await stackOverflowProvider.CreateStaticIndexesAsync(
                opts.Url,
                effectiveDatabase,
                opts.SearchEngine,
                negotiatedHttpVersion,
                extraIndexes);
        }

        var soDataset = string.IsNullOrEmpty(opts.Dataset) ? null : KnownDatasets.GetByName(opts.Dataset);
        var soMaxQuestionId = soDataset?.MaxQuestionId ?? 0;
        var soMaxUserId = soDataset?.MaxUserId ?? 0;

        StackOverflowWorkloadMetadata? stackOverflowMetadata = null;
        if (opts.Profile == WorkloadProfile.StackOverflowRandomReads || opts.Profile == WorkloadProfile.StackOverflowTextSearch)
        {
            stackOverflowMetadata = await StackOverflowWorkloadHelper.DiscoverOrLoadMetadataAsync(
                opts.Url,
                effectiveDatabase,
                opts.Seed,
                soMaxQuestionId,
                soMaxUserId);

            if (stackOverflowMetadata == null)
            {
                throw new InvalidOperationException("StackOverflow metadata not available. Ensure dataset is imported and indexes are not stale.");
            }

            if (staticIndexNames != null)
            {
                stackOverflowMetadata.TitleIndexName = staticIndexNames.QuestionsTitleIndex;
                stackOverflowMetadata.TitleSearchIndexName = staticIndexNames.QuestionsTitleSearchIndex;
                stackOverflowMetadata.TitleSuggestionsIndexName = staticIndexNames.QuestionsTitleSuggestionsIndex;
                stackOverflowMetadata.TitleMoreLikeThisIndexName = staticIndexNames.QuestionsTitleMoreLikeThisIndex;
                stackOverflowMetadata.ViewCountGroupedIndexName = staticIndexNames.QuestionsViewCountGroupedIndex;
                stackOverflowMetadata.TagsIndexName = staticIndexNames.QuestionsTagsIndex;
            }
        }

        StackOverflowUsersWorkloadMetadata? usersMetadata = null;
        if (opts.Profile == WorkloadProfile.QueryUsersByName)
        {
            usersMetadata = await StackOverflowUsersWorkloadHelper.DiscoverOrLoadMetadataAsync(
                opts.Url,
                effectiveDatabase,
                opts.Seed,
                soMaxUserId);

            if (usersMetadata == null)
            {
                throw new InvalidOperationException("Users metadata not available. Ensure StackOverflow dataset is imported and indexes are not stale.");
            }

            if (staticIndexNames != null)
            {
                usersMetadata.DisplayNameIndexName = staticIndexNames.UsersDisplayNameIndex;
                usersMetadata.ReputationIndexName = staticIndexNames.UsersReputationIndex;
                usersMetadata.SpatialIndexName = staticIndexNames.UsersSpatialIndex;
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

        if (opts.Profile == WorkloadProfile.Attachments && opts.AttachmentOp != AttachmentOperationKind.Put)
            await PreloadAttachmentsAsync(transport, opts);

        var steps = new List<StepResult>();
        var histogramArtifacts = new List<HistogramArtifact>();

        var cpuTracker = new ProcessCpuTracker();
        using var serverTracker = new ServerMetricsTracker(transport, opts with { Database = effectiveDatabase });
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
        await ValidateSnmpAsync(transport, effectiveDatabase);

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

        var executor = new BenchmarkExecutor(opts, transport, workload, cpuTracker, serverTracker, nodeExporter: nodeExporter);

        var rampResult = await RunRampAsync(opts, transport, executor, workload, startupCalibration, _rng);
        steps = rampResult.Steps;
        histogramArtifacts = rampResult.HistogramArtifacts;
        maxNetUtil = rampResult.MaxNetworkUtilization;

        var serverMetricsHistory = serverTracker.GetHistory();

        return new BenchmarkRun
        {
            Steps = steps,
            MaxNetworkUtilization = maxNetUtil,
            ClientCompression = clientCompression,
            EffectiveHttpVersion = httpVersion,
            TransportPath = transport.TransportPath,
            StartupCalibration = startupCalibration,
            ServerMetricsHistory = serverMetricsHistory.Count > 0 ? serverMetricsHistory : null,
            MeasurementWindows = serverTracker.GetWindows(),
            HistogramArtifacts = histogramArtifacts.Count > 0 ? histogramArtifacts : null,
            VectorMetadata = vectorMetadata,
            EffectiveDatabase = effectiveDatabase
        };
    }

    /// <summary>
    /// The result of driving a step plan through its full ramp: the load, C, A, B and
    /// insert-stream runs of a ycsb scenario each drive one call of this, at a fixed step plan
    /// (a single value or a ramp), so no second ramp loop exists anywhere in the codebase.
    /// </summary>
    internal readonly record struct RampResult(List<StepResult> Steps, double MaxNetworkUtilization, List<HistogramArtifact> HistogramArtifacts);

    internal static async Task<RampResult> RunRampAsync(
        RunOptions opts,
        IYcsbTransport transport,
        BenchmarkExecutor executor,
        IWorkload workload,
        StartupCalibration? startupCalibration,
        Random rng)
    {
        var steps = new List<StepResult>();
        var histogramArtifacts = new List<HistogramArtifact>();
        var stepPlan = opts.Step.Normalize();
        var currentValue = stepPlan.Start;
        var endValue = stepPlan.End;
        var maxNetUtil = 0.0;

        double? observedServiceTimeSeconds = null;
        int? previousAutoRateWorkers = null;

        while (currentValue <= endValue)
        {
            // Unloaded startup latency in µs, sizes the rate workers
            var baselineLatencyMicros = startupCalibration?.Endpoints.Count > 0
                ? (long)(startupCalibration.Endpoints.Min(e => e.ObservedMs) * 1000)
                : 0L;

            var rateWorkerCount = opts.Shape == LoadShape.Rate
                ? RateWorkerPlanner.ResolveRateWorkerCount(opts, (int)currentValue, baselineLatencyMicros, observedServiceTimeSeconds, previousAutoRateWorkers)
                : 0;

            ILoadGenerator loadGenerator = opts.Shape switch
            {
                LoadShape.Rate => new RateLoadGenerator(transport, workload, (int)currentValue, rateWorkerCount, rng, opts.PipelineDepth),
                LoadShape.Closed => new ClosedLoopLoadGenerator(transport, workload, (int)currentValue, rng, opts.PipelineDepth),
                _ => new ClosedLoopLoadGenerator(transport, workload, (int)currentValue, rng, opts.PipelineDepth)
            };

            LogStepStart(opts.Shape, steps.Count + 1, (int)currentValue, rateWorkerCount, opts);

            var (latencyRecorder, stepResult) = await executor.ExecuteStepAsync(loadGenerator, steps.Count, (int)currentValue, CancellationToken.None);

            var snapshot = latencyRecorder.Snapshot();

            if (opts.Shape == LoadShape.Rate && opts.RateWorkers.HasValue == false)
            {
                // The mean latency per operation sizes the next step.
                observedServiceTimeSeconds = snapshot.MeanMicros / 1_000_000.0;
                previousAutoRateWorkers = rateWorkerCount;
            }

            double[] percentiles = { 50, 75, 90, 95, 99, 99.9 };
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
            var latenessReason = SendLateness.MarkingFor(stepResult.SendLateness, rawPercentiles.P50);
            if (stepResult.InvalidReason == null && latenessReason != null)
            {
                stepResult.InvalidReason = latenessReason;
                Console.WriteLine(ClientSaturation.ConsoleLine(steps.Count + 1, stepResult.Concurrency, runName: null, latenessReason));
            }
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

            var artifact = HistogramExporter.BuildHistogramArtifact(snapshot, steps.Count, stepResult.Concurrency, opts.LatencyHistogramsDir, opts.LatencyHistogramsFormat);
            if (artifact != null)
            {
                histogramArtifacts.Add(artifact);
            }

            steps.Add(stepResult);
            LogStepResult(steps.Count, stepResult);
            maxNetUtil = Math.Max(maxNetUtil, stepResult.NetworkUtilization);

            // A bounded workload ends the ramp when it has produced its last operation.
            if (workload.IsExhausted)
                break;

            if (stepResult.ErrorRate > Math.Max(opts.MaxErrorRate, 0.05))
            {
                Console.WriteLine("[Raven.Bench] High error rate; stopping ramp.");
                break;
            }

            if (opts.Shape == LoadShape.Rate && stepResult.TargetThroughput.HasValue)
            {
                var target = stepResult.TargetThroughput.Value;
                var actual = SucceededOperationsPerSecond(stepResult);
                var deltaPct = (actual - target) / target * 100.0;

                if (deltaPct < -30.0)
                {
                    Console.WriteLine($"[Raven.Bench] Throughput is {Math.Abs(deltaPct):F1}% below target ({actual:F0} vs {target:F0} ops/s). Server appears saturated; stopping ramp.");
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

        return new RampResult(steps, maxNetUtil, histogramArtifacts);
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

    /// <summary>Succeeded operations per second over the measured window: the unit of a rate target.</summary>
    private static double SucceededOperationsPerSecond(StepResult step) =>
        step.MeasuredDuration is { TotalSeconds: > 0 } d ? step.SampleCount * (1 - step.ErrorRate) / d.TotalSeconds : 0;

    private static void LogStepResult(int stepNumber, StepResult step)
    {
        if (step.TargetThroughput.HasValue && step.TargetThroughput > 0)
        {
            var target = step.TargetThroughput.Value;
            var actual = SucceededOperationsPerSecond(step);
            var deltaPct = (actual - target) / target * 100.0;
            var deltaFormatted = double.IsFinite(deltaPct) ? $"{deltaPct:+0.0;-0.0;0}%" : "n/a";
            var rollingInfo = step.RollingRate is { HasSamples: true } rate
                ? $" | rolling median {rate.Median:F0} docs/s (min {rate.Min:F0}, max {rate.Max:F0}, samples={rate.SampleCount})"
                : string.Empty;
            Console.WriteLine($"[Raven.Bench] Step {stepNumber} result: {step.Throughput:F0} docs/s, {actual:F0} ops/s (target {target:F0} ops/s, delta {deltaFormatted}){rollingInfo}");
            if (step.SendLateness is { } late)
                Console.WriteLine($"[Raven.Bench]   send lateness ms: p50 {late.P50:F3}, p90 {late.P90:F3}, p99 {late.P99:F3}, p99.9 {late.P999:F3} (latency p50 {step.Raw.P50:F3})");

            if (Math.Abs(deltaPct) > 10.0)
            {
                Console.WriteLine("[Raven.Bench]   note: measured rate deviates >10% from target; check server-side meters (they may count extra system requests) or adjust --rate-workers.");
            }
        }
        else
        {
            Console.WriteLine($"[Raven.Bench] Step {stepNumber} result: concurrency {step.Concurrency}, throughput {step.Throughput:F0} docs/s");
        }
    }

    private static ITransport BuildTransport(RunOptions opts, Version negotiatedHttpVersion, string database)
    {
        switch (opts.Transport)
        {
            case TransportKind.Raw:
                var raw = new RawHttpTransport(opts.Url, database, opts.Compression, negotiatedHttpVersion, opts.RawEndpoint, opts.PipelineDepth);
                Console.WriteLine($"[Raven.Bench] Transport: Raw HTTP with {opts.Compression} compression, path {raw.TransportPath}, pipeline depth {raw.PipelineDepth}");
                return raw;
            case TransportKind.Client:
            case TransportKind.ClientEntity:
                var mapEntities = opts.Transport == TransportKind.ClientEntity;
                Console.WriteLine($"[Raven.Bench] Transport: RavenDB Client with {opts.Compression} compression, entities {(mapEntities ? "mapped" : "not mapped")}");
                return new RavenClientTransport(opts.Url, database, opts.Compression, negotiatedHttpVersion, mapEntities);
            default:
                throw new ArgumentOutOfRangeException(nameof(opts.Transport), opts.Transport, null);
        }
    }

    private static async Task PreloadAsync(IYcsbTransport transport, RunOptions opts, int count, int docSize)
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
                    await transport.PutAsync(id, PayloadGenerator.Generate(opts.Seed, id, docSize));
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failures);
                    VerboseErrorTracker.LogError(ex.Message);
                }
            });

        if (failures > 0)
            throw new InvalidOperationException($"Preload failed: {failures} of {count} document writes failed.");

        Console.WriteLine("[Raven.Bench] Preload complete.");
    }

    /// <summary>
    /// Get/Delete attachment workloads need attachments to exist on the preloaded documents.
    /// </summary>
    private static async Task PreloadAttachmentsAsync(ITransport transport, RunOptions opts)
    {
        Console.WriteLine($"[Raven.Bench] Creating attachments on {opts.Preload} preloaded documents...");

        var payload = new byte[opts.DocumentSizeBytes];
        new Random(opts.Seed).NextBytes(payload);

        var options = new ParallelOptions { MaxDegreeOfParallelism = 32 };
        var failures = 0;
        await Parallel.ForEachAsync(
            Enumerable.Range(1, opts.Preload),
            options,
            async (i, ct) =>
            {
                var docId = BenchIds.IdFor(i);
                var result = await transport.ExecuteAsync(new AttachmentOperation
                {
                    DocumentId = docId,
                    Name = AttachmentWorkload.NameFor(docId),
                    Kind = AttachmentOperationKind.Put,
                    Payload = payload
                }, ct);

                if (result.IsSuccess == false)
                {
                    Interlocked.Increment(ref failures);
                    VerboseErrorTracker.LogError(result.ErrorDetails ?? "attachment preload failed");
                }
            });

        if (failures > 0)
            throw new InvalidOperationException($"Attachment preload failed: {failures} of {opts.Preload} attachment writes failed.");

        Console.WriteLine("[Raven.Bench] Attachment preload complete.");
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
            Console.WriteLine($"[Raven.Bench] {transport.ProductName} Server Version: {serverVersion}");
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
    private async Task ValidateSnmpAsync(ITransport transport, string database)
    {
        if (opts.Snmp.Enabled == false)
            return;

        Console.WriteLine($"[Raven.Bench] Validating SNMP connectivity (profile: {opts.Snmp.Profile})...");

        try
        {
            var snmpSample = await transport.GetSnmpMetricsAsync(opts.Snmp, database);

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
