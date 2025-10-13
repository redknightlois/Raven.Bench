using System.Collections.Concurrent;
using System.Diagnostics;
using RavenBench.Metrics;
using RavenBench.Transport;
using RavenBench.Workload;
using RavenBench.Diagnostics;
using RavenBench.Util;
using RavenBench.Reporting;
using Spectre.Console;


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

        // Set ThreadPool minimum threads based on command-line parameters
        var workers = opts.ThreadPoolWorkers ?? 8192;
        var iocp = opts.ThreadPoolIOCP ?? 8192;
        
        Console.WriteLine($"[Raven.Bench] Setting ThreadPool: workers={workers}, iocp={iocp}");
        ThreadPool.SetMinThreads(workers, iocp);

        var workload = BuildWorkload(opts);

        // Negotiate HTTP version before creating transport
        Console.WriteLine("[Raven.Bench] Negotiating HTTP version...");
        var negotiatedHttpVersion = await HttpVersionNegotiator.NegotiateVersionAsync(
            opts.Url,
            opts.HttpVersion,
            opts.StrictHttpVersion);
        Console.WriteLine($"[Raven.Bench] Using HTTP/{HttpHelper.FormatHttpVersion(negotiatedHttpVersion)}");

        using var transport = BuildTransport(opts, negotiatedHttpVersion);

        // Ensure database exists before preload or benchmark
        Console.WriteLine($"[Raven.Bench] Ensuring database '{opts.Database}' exists...");
        await transport.EnsureDatabaseExistsAsync(opts.Database);

        if (opts.Preload > 0)
            await PreloadAsync(transport, opts, opts.Preload, _rng, opts.DocumentSizeBytes);

        var steps = new List<StepResult>();
        var concurrency = opts.ConcurrencyStart;

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
        
        // Create benchmark context to reduce parameter passing
        var context = new BenchmarkContext
        {
            Transport = transport,
            Workload = workload,
            CpuTracker = cpuTracker,
            ServerTracker = serverTracker,
            Rng = _rng,
            Options = opts
        };
        
        while (concurrency <= opts.ConcurrencyEnd)
        {
            var warmupResult = await WarmupWithProgress(context, new StepParameters
            {
                Concurrency = concurrency,
                Duration = opts.Warmup,
                Record = false
            });

            // Check if warmup failed with high error rate - terminate gracefully
            if (warmupResult.ErrorRate > 0.5)
            {
                Console.WriteLine($"[Raven.Bench] Warmup failed with {warmupResult.ErrorRate:P1} error rate (>{50:P0}).");
                var httpVersionInfo = negotiatedHttpVersion.Major == 1 ? " (common with HTTP/1 socket exhaustion)" : "";
                Console.WriteLine($"[Raven.Bench] Stopping benchmark - system appears unstable{httpVersionInfo}.");
                break;
            }

            var (s, hist) = await MeasureWithProgress(context, new StepParameters
            {
                Concurrency = concurrency,
                Duration = opts.Duration,
                Record = true
            });

            // Calculate percentiles
            int[] percentiles = { 50, 75, 90, 95, 99, 999 };
            var rawValues = new double[6];
            for (int i = 0; i < percentiles.Length; i++)
            {
                rawValues[i] = hist.GetPercentile(percentiles[i]) / 1000.0;
            }

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
            
            s.Raw = rawPercentiles;
            s.Normalized = normalizedPercentiles;

            steps.Add(s);
            maxNetUtil = Math.Max(maxNetUtil, s.NetworkUtilization);

            // knee stop if error rate exceeds bound significantly
            if (s.ErrorRate > Math.Max(opts.MaxErrorRate, 0.05))
            {
                Console.WriteLine("[Raven.Bench] High error rate; stopping ramp.");
                break;
            }

            concurrency = (int)Math.Max(concurrency * opts.ConcurrencyFactor, concurrency + 1);
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
            ServerMetricsHistory = serverMetricsHistory.Count > 0 ? serverMetricsHistory : null
        };
    }

    private async Task<StepResult> WarmupWithProgress(BenchmarkContext context, StepParameters step)
    {
        var (result, _) = await AnsiConsole.Progress()
            .AutoClear(true)
            .Columns(new ProgressColumn[]
            {
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new ElapsedTimeColumn(),
                new RemainingTimeColumn()
            })
            .StartAsync(async ctx =>
            {
                var httpVersionInfo = context.Transport is RawHttpTransport raw ? $" HTTP/{raw.EffectiveHttpVersion}" : "";
                var t = ctx.AddTask($"Warmup @ C={step.Concurrency}{httpVersionInfo}", maxValue: step.Duration.TotalSeconds);
                var run = RunClosedLoopAsync(context, step);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (run.IsCompleted == false)
                {
                    t.Value = Math.Min(step.Duration.TotalSeconds, sw.Elapsed.TotalSeconds);
                    await Task.Delay(200);
                }
                t.Value = t.MaxValue;
                return await run;
            });

        return result;
    }

    private async Task<(StepResult result, LatencyRecorder hist)> MeasureWithProgress(BenchmarkContext context, StepParameters step)
    {
        (StepResult, LatencyRecorder) res = default;
        await AnsiConsole.Progress()
            .AutoClear(true)
            .Columns(new ProgressColumn[]
            {
                new TaskDescriptionColumn(),
                new ProgressBarColumn(),
                new ElapsedTimeColumn(),
                new RemainingTimeColumn()
            })
            .StartAsync(async ctx =>
            {
                var httpVersionInfo = context.Transport is RawHttpTransport raw ? $" HTTP/{raw.EffectiveHttpVersion}" : "";
                var t = ctx.AddTask($"Measure @ C={step.Concurrency}{httpVersionInfo}", maxValue: step.Duration.TotalSeconds);
                var run = RunClosedLoopAsync(context, step);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (run.IsCompleted == false)
                {
                    t.Value = Math.Min(step.Duration.TotalSeconds, sw.Elapsed.TotalSeconds);
                    await Task.Delay(200);
                }
                t.Value = t.MaxValue;
                res = await run;
            });
        return res;
    }

    private static IWorkload BuildWorkload(RunOptions opts)
    {
        if (opts.Profile == WorkloadProfile.Unspecified)
            throw new InvalidOperationException("Workload profile is required. Specify --profile mixed|writes|reads|query-by-id.");

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

    private static ITransport BuildTransport(RunOptions opts, Version negotiatedHttpVersion)
    {
        if (opts.Transport == "raw")
        {
            Console.WriteLine($"[Raven.Bench] Transport: Raw HTTP with {opts.Compression} compression");
            return new RawHttpTransport(opts.Url, opts.Database, opts.Compression, negotiatedHttpVersion, opts.RawEndpoint);
        }

        if (opts.Transport == "client")
        {
            Console.WriteLine($"[Raven.Bench] Transport: RavenDB Client with {opts.Compression} compression");
            return new RavenClientTransport(opts.Url, opts.Database, opts.Compression, negotiatedHttpVersion);
        }

        Console.WriteLine("[Raven.Bench] Transport: Raw HTTP with identity compression (default)");
        return new RawHttpTransport(opts.Url, opts.Database, "identity", negotiatedHttpVersion);
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

    private static string IdFor(int i) => $"bench/{i:D8}";

    /// <summary>
    /// Runs a closed-loop benchmark step with simplified parameter passing via context objects.
    /// </summary>
    internal async Task<(StepResult result, LatencyRecorder hist)> RunClosedLoopAsync(BenchmarkContext context, StepParameters step)
    {
        using var cts = new CancellationTokenSource(step.Duration);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        long success = 0, errors = 0, bytesOut = 0, bytesIn = 0;

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
                while (cts.IsCancellationRequested == false)
                {
                    var op = context.Workload.NextOperation(rnd);
                    var t0 = Stopwatch.GetTimestamp();
                    try
                    {
                        var res = await context.Transport.ExecuteAsync(op, cts.Token).ConfigureAwait(false);
                        var us = ElapsedMicros(t0);
                        if (step.Record)
                            hist.Record(us);

                        if (res.IsSuccess)
                        {
                            Interlocked.Increment(ref success);
                            Interlocked.Add(ref bytesOut, res.BytesOut);
                            Interlocked.Add(ref bytesIn, res.BytesIn);
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

        var stepResult = new StepResult
        {
            Concurrency = step.Concurrency,
            Throughput = thr,
            ErrorRate = errRate,
            BytesOut = bytesOut,
            BytesIn = bytesIn,
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
            SnmpIoWriteOpsPerSec = serverMetrics.SnmpIoWriteOpsPerSec,
            SnmpIoReadBytesPerSec = serverMetrics.SnmpIoReadBytesPerSec,
            SnmpIoWriteBytesPerSec = serverMetrics.SnmpIoWriteBytesPerSec,
            ServerSnmpRequestsPerSec = serverMetrics.ServerSnmpRequestsPerSec,
            SnmpErrorsPerSec = serverMetrics.SnmpErrorsPerSec,
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

            if (!snmpSample.IsEmpty)
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
                    $"  - Community string mismatch (RavenDB uses '{SnmpOptions.Community}')\n" +
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
