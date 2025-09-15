using System.Diagnostics;
using RavenBench.Metrics;
using RavenBench.Transport;
using RavenBench.Workload;
using RavenBench.Util;
using RavenBench.Reporting;
using Spectre.Console;


namespace RavenBench;

public class BenchmarkRunner(RunOptions opts)
{
    private readonly Random _rng = new(opts.Seed);

    public async Task<BenchmarkRun> RunAsync()
    {
        // Set ThreadPool minimum threads based on command-line parameters
        var workers = opts.ThreadPoolWorkers ?? 8192;
        var iocp = opts.ThreadPoolIOCP ?? 8192;
        
        Console.WriteLine($"[Raven.Bench] Setting ThreadPool: workers={workers}, iocp={iocp}");
        ThreadPool.SetMinThreads(workers, iocp);

        var workload = BuildWorkload(opts);
        using var transport = BuildTransport(opts);

        if (opts.Preload > 0)
            await PreloadAsync(transport, opts, opts.Preload, _rng, opts.DocumentSizeBytes);

        var steps = new List<StepResult>();
        var concurrency = opts.ConcurrencyStart;

        var cpuTracker = new ProcessCpuTracker();
        using var serverTracker = new ServerMetricsTracker(transport);

        var maxNetUtil = 0.0;
        string clientCompression = transport switch
        {
            RavenClientTransport rc => rc.EffectiveCompressionMode,
            RawHttpTransport raw => raw.EffectiveCompressionMode,
            _ => "unknown"
        };
        string httpVersion = transport switch
        {
            RawHttpTransport raw => raw.EffectiveHttpVersion,
            _ => "client-default"
        };

        await ValidateClientAsync(transport);
        await ValidateServerSanityAsync(transport);
        
        // Create benchmark context to reduce parameter passing
        var context = new BenchmarkContext
        {
            Transport = transport,
            Workload = workload,
            CpuTracker = cpuTracker,
            ServerTracker = serverTracker,
            Rng = _rng
        };
        
        while (concurrency <= opts.ConcurrencyEnd)
        {
            await WarmupWithProgress(context, new StepParameters
            {
                Concurrency = concurrency,
                Duration = opts.Warmup,
                Record = false
            });

            var (s, hist) = await MeasureWithProgress(context, new StepParameters
            {
                Concurrency = concurrency,
                Duration = opts.Duration,
                Record = true
            });

            s.P50Ms = hist.GetPercentile(50) / 1000.0;
            s.P90Ms = hist.GetPercentile(90) / 1000.0;
            s.P95Ms = hist.GetPercentile(95) / 1000.0;
            s.P99Ms = hist.GetPercentile(99) / 1000.0;

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

        return new BenchmarkRun
        {
            Steps = steps,
            MaxNetworkUtilization = maxNetUtil,
            ClientCompression = clientCompression,
            EffectiveHttpVersion = httpVersion
        };
    }

    private async Task WarmupWithProgress(BenchmarkContext context, StepParameters step)
    {
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
                var t = ctx.AddTask($"Warmup @ C={step.Concurrency}{httpVersionInfo}", maxValue: step.Duration.TotalSeconds);
                var run = RunClosedLoopAsync(context, step);
                var sw = System.Diagnostics.Stopwatch.StartNew();
                while (!run.IsCompleted)
                {
                    t.Value = Math.Min(step.Duration.TotalSeconds, sw.Elapsed.TotalSeconds);
                    await Task.Delay(200);
                }
                t.Value = t.MaxValue;
                await run;
            });
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
                while (!run.IsCompleted)
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
        var r = opts.Reads ?? 75.0;
        var w = opts.Writes ?? 25.0;
        var u = opts.Updates ?? 0.0;
        var mix = WorkloadMix.FromWeights(r, w, u);
        
        IKeyDistribution dist = opts.Distribution.ToLowerInvariant() switch
        {
            "uniform" => new UniformDistribution(),
            "zipfian" => new ZipfianDistribution(),
            "latest" => new LatestDistribution(),
            _ => throw new NotImplementedException($"Distribution '{opts.Distribution}' is not implemented. Supported distributions: uniform, zipfian, latest")
        };

        return new MixedWorkload(mix, dist, opts.DocumentSizeBytes);
    }

    private static ITransport BuildTransport(RunOptions opts)
    {
        if (opts.Compression.StartsWith("raw:"))
        {
            var mode = opts.Compression.Split(':')[1];
            return new RawHttpTransport(opts.Url, opts.Database, mode, opts.HttpVersion, opts.RawEndpoint);
        }
        
        if (opts.Compression.StartsWith("client:"))
        {
            var mode = opts.Compression.Split(':')[1];
            return new RavenClientTransport(opts.Url, opts.Database, mode);
        }
        
        return new RawHttpTransport(opts.Url, opts.Database, "identity", opts.HttpVersion);
    }

    private static async Task PreloadAsync(ITransport transport, RunOptions opts, int count, Random rng, int docSize)
    {
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
        
        // REVIEW: all this information is from the client. Shoulnt we be able to hit the RavenDB own's endpoints to get data out of it?
        // You can access an instance at http://98.87.58.180:9082 in case you need it. 
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
                            if (opts.Verbose && res.ErrorDetails != null)
                            {
                                Console.WriteLine($"[Raven.Bench] Verbose Error: {res.ErrorDetails}");
                            }
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        // We do not increment errors because we are asking for it. 
                    }
                    catch (Exception e)
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
            ServerIoReadOps = serverMetrics.IoReadOperations.HasValue ? (long)serverMetrics.IoReadOperations.Value : null,
            ServerIoWriteOps = serverMetrics.IoWriteOperations.HasValue ? (long)serverMetrics.IoWriteOperations.Value : null,
            ServerIoReadKb = serverMetrics.ReadThroughputKb,
            ServerIoWriteKb = serverMetrics.WriteThroughputKb,
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
            Console.WriteLine($"[Raven.Bench] RavenDB Server Version: {serverVersion}");
            Console.WriteLine($"[Raven.Bench] License Type: {licenseType}");

            // Display effective HTTP version
            if (transport is RawHttpTransport rawTransport)
            {
                Console.WriteLine($"[Raven.Bench] HTTP Version: {rawTransport.EffectiveHttpVersion}");
            }
            
            if (opts.ExpectedCores.HasValue)
            {
                var cores = await transport.GetServerMaxCoresAsync();
                if (cores.HasValue && cores.Value != opts.ExpectedCores.Value)
                {
                    Console.WriteLine($"[Raven.Bench] Warning: Server core limit={cores} differs from expected={opts.ExpectedCores}");
                }
            }
        }
        catch (Exception ex)
        {
            // non-fatal - continue with benchmark
            Console.WriteLine($"[Raven.Bench] Warning: Server validation failed: {ex.Message}");
        }
    }
}
