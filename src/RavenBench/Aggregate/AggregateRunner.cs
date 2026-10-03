using System.Diagnostics;
using System.Net;
using System.Text.Json;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Aggregate;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Ycsb;

namespace RavenBench.Aggregate;

/// <summary>Issues one shape's grouped aggregate on every call.</summary>
public sealed class AggregateQueryWorkload(Func<GroupedAggregateOperation> create) : IWorkload
{
    public OperationBase NextOperation(Random rng) => create();
}

/// <summary>Forwards to the product transport and hands every grouped answer, with its receive time, to the freshness tracker.</summary>
public sealed class FreshnessRecordingTransport(IYcsbTransport inner, FreshnessTracker tracker) : IYcsbTransport
{
    public string ProductName => inner.ProductName;
    public bool ReportsWireBytes => inner.ReportsWireBytes;

    public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
    {
        var result = await inner.ExecuteAsync(op, ct).ConfigureAwait(false);
        var received = Stopwatch.GetTimestamp();
        if (op is GroupedAggregateOperation && result.IsSuccess && result.Cancelled == false && result.Groups is { } groups)
            tracker.Answered(groups, received);
        return result;
    }

    public Task PutAsync<T>(string id, T document) => inner.PutAsync(id, document);
    public Task EnsureDatabaseExistsAsync(string databaseName) => inner.EnsureDatabaseExistsAsync(databaseName);
    public Task<long> GetDocumentCountAsync(string idPrefix) => inner.GetDocumentCountAsync(idPrefix);
    public Task<string> GetServerVersionAsync() => inner.GetServerVersionAsync();

    // The inner transport belongs to the runner, which disposes it.
    public void Dispose()
    {
    }
}

public sealed record AggregateRunResult(string Run, BenchmarkSummary Summary);

/// <summary>
/// Runs build, count-by-category, sum-by-region, filtered-group and under-write against one target.
/// No query asks the server to wait for a non-stale answer; every answer records the product's stale flag.
/// </summary>
public sealed class AggregateRunner(AggregateScenario scenario, IReadOnlyDictionary<string, string> overrides, AggregateSettings settings)
{
    public const string RavendbTarget = YcsbRunner.RavendbTarget;
    public const string Ravendb7Target = YcsbRunner.Ravendb7Target;

    public static readonly IReadOnlyList<string> Targets = [RavendbTarget, Ravendb7Target, MongoYcsbTransport.MongoDbTarget, MongoYcsbTransport.MongoDbIndexedTarget];
    public static readonly IReadOnlyList<string> Runs = ["build", AggregateShapes.CountByCategory, AggregateShapes.SumByRegion, AggregateShapes.FilteredGroup, "under-write"];

    public const string QueryPolicy = "no-wait: each query reads the answer the product has now and records its stale flag";
    public const string WriteLatencyDefinition = "from sending an update to its acknowledgement; the bulk writers keep up to writers updates in flight, the probe one";

    public async Task<List<AggregateRunResult>> RunAsync(CancellationToken ct = default)
    {
        var targetName = Required(settings.Target, "--target").ToLowerInvariant();
        if (Targets.Contains(targetName) == false)
            throw new ArgumentException($"Target '{targetName}' is not an aggregate target; valid targets are {string.Join(", ", Targets)}.", "--target");
        var url = Required(settings.Url, "--url");
        var database = Required(settings.Database, "--database");
        var warmup = CliParsing.ParseDuration(scenario.Warmup);
        var duration = CliParsing.ParseDuration(scenario.Duration);
        var nonStaleTimeout = CliParsing.ParseDuration(scenario.NonStaleTimeout);
        var dataDirectory = scenario.ResolveDataDirectory(RepositoryRootLocator.Find());
        var dataSet = new AggregateDataSet(scenario.DataSpec());
        bool isRavenDb = targetName is RavendbTarget or Ravendb7Target;

        var (nodeExporter, nodeExporterFailure) = await ConnectNodeExporterAsync(settings.NodeExporterUrl);
        using var _ = nodeExporter;
        using IYcsbTransport transport = isRavenDb
            ? new RawHttpTransport(url, database, CompressionMode.Identity, HttpVersion.Version11)
            : new MongoYcsbTransport(url, database, targetName);
        bool createdDatabase = isRavenDb && await CreateRavenDatabaseAsync(url, database, ct);

        try
        {
            // build
            using var digest = new AggregateSetDigest(dataSet.Spec);
            var (buildStep, build) = await BuildAsync(transport, dataSet, digest, nodeExporter, nodeExporterFailure, nonStaleTimeout, ct);
            var summary = digest.Summary();
            RecordManifest(dataDirectory, dataSet.Spec, summary);
            var tracked = AggregateOrdering.Top(digest.CategoryCounts.Select(c => new AggregateGroup(c.Key, c.Value)), 1)[0];
            var filterCategory = digest.CategoryCounts
                .OrderBy(c => Math.Abs((double)c.Value / summary.Count - scenario.FilterSelectivity))
                .ThenBy(c => c.Key, AggregateOrdering.KeyComparer)
                .First().Key;
            Console.WriteLine($"[Aggregate] {summary.Count:N0} documents, checksum {summary.Checksum}, filtered-group category {filterCategory}");

            var productName = transport.ProductName;
            var serverVersion = await transport.GetServerVersionAsync();
            var container = targetName != RavendbTarget && Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Port > 0
                ? new DockerDatabaseContainerLocator().Locate(uri.Port)
                : null;
            var fingerprint = new MachineFingerprintCollector(new NativeMachineFingerprintSource()).Collect(RepositoryRootLocator.Find(), container);

            GroupedAggregateOperation Operation(string shape) => shape switch
            {
                AggregateShapes.CountByCategory => AggregateShapes.Create(shape, scenario.CountTopN),
                AggregateShapes.FilteredGroup => AggregateShapes.Create(shape, scenario.RegionTopN, filterCategory),
                _ => AggregateShapes.Create(shape, scenario.RegionTopN)
            };

            // count-by-category, sum-by-region, filtered-group: the closed loop finds the ceiling, the fixed-rate runner runs at it.
            var queryRuns = new List<(string Shape, BenchmarkRunner.RampResult Closed, BenchmarkRunner.RampResult Fixed, AggregateQueryInfo Info)>();
            foreach (var shape in AggregateShapes.All)
            {
                var workload = new AggregateQueryWorkload(() => Operation(shape));
                var closed = await RampAsync(transport, workload, url, database, LoadShape.Closed, scenario.Concurrency, null, warmup, duration, $"{shape}-closed", nodeExporter);
                var rate = Math.Max(1, (int)Math.Floor(closed.Steps[^1].Throughput));
                var fixedRate = await RampAsync(transport, workload, url, database, LoadShape.Rate, rate, scenario.Concurrency, warmup, duration, $"{shape}-rate", nodeExporter);
                var steps = closed.Steps.Concat(fixedRate.Steps).ToList();
                var op = Operation(shape);
                queryRuns.Add((shape, closed, fixedRate, new AggregateQueryInfo(shape, isRavenDb ? op.IndexName : null, op.TopN, Describe(op.Filter), QueryPolicy,
                    closed.Steps[^1].Throughput, rate, steps.Any(s => ClientSaturation.IsSaturated(s.ClientCpu)), Answers(steps))));
                Console.WriteLine($"[Aggregate] {shape}: closed loop {closed.Steps[^1].Throughput:F0} q/s, fixed rate {rate} q/s p99 {fixedRate.Steps[^1].Raw.P99:F2} ms");
            }

            // under-write: a quiet step and an under-write step at the same fixed query rate.
            var queryRate = (int)Math.Round(scenario.UnderWriteQueryRate);
            var countWorkload = new AggregateQueryWorkload(() => Operation(AggregateShapes.CountByCategory));
            var quiet = await RampAsync(transport, countWorkload, url, database, LoadShape.Rate, queryRate, scenario.Concurrency, warmup, duration, "under-write-quiet", nodeExporter);
            var tracker = new FreshnessTracker(tracked.Key, tracked.Value);
            var (underWrite, writer, probe) = await UnderWriteAsync(transport, dataSet, digest.CategoryCounts, tracker, countWorkload, queryRate, url, database, warmup, duration, nodeExporter, ct);
            var underWriteSteps = quiet.Steps.Concat(underWrite.Steps).ToList();
            var freshness = tracker.Complete();
            var underWriteInfo = new AggregateUnderWriteInfo(scenario.UnderWriteQueryRate, QueryPolicy, quiet.Steps[^1].Throughput, quiet.Steps[^1].Raw.P99,
                underWrite.Steps[^1].Throughput, underWrite.Steps[^1].Raw.P99, writer, WriteLatencyDefinition, freshness,
                underWriteSteps.Any(s => ClientSaturation.IsSaturated(s.ClientCpu)), Answers(underWriteSteps), probe);
            Console.WriteLine($"[Aggregate] under-write: bulk writers held {writer.HeldPerSecond:F0} of {writer.RequestedPerSecond:F0} updates/s{(writer.Shortfall is null ? "" : $" ({writer.Shortfall})")}; probe held {probe.HeldPerSecond:F0} of {probe.RequestedPerSecond:F0} updates/s; freshness observed {freshness.Observed}, unobserved {freshness.Unobserved}, p50 {freshness.Distribution.P50:F1} ms, p99 {freshness.Distribution.P99:F1} ms");

            var durability = isRavenDb
                ? new DurabilityParity { Setting = "durability", Value = "ravendb-default" }
                : new DurabilityParity { Setting = "writeConcern", Value = "j=true" };
            AggregateRunResult Result(string run, List<StepResult> steps, List<HistogramArtifact>? histograms, Func<AggregateRunInfo, AggregateRunInfo> fill) => new(run, new BenchmarkSummary
            {
                Options = new RunOptions { Url = url, Database = database, Seed = scenario.Seed, Warmup = warmup, Duration = duration },
                Steps = steps,
                Verdict = steps.Any(s => ClientSaturation.IsSaturated(s.ClientCpu)) ? "client-bound" : "measured",
                ClientCompression = "n/a",
                EffectiveHttpVersion = "n/a",
                HistogramArtifacts = histograms is { Count: > 0 } ? histograms : null,
                MachineFingerprint = fingerprint,
                Aggregate = fill(new AggregateRunInfo
                {
                    Run = run,
                    ResolvedScenario = scenario,
                    Overrides = overrides,
                    Target = targetName,
                    ProductName = productName,
                    ServerVersion = serverVersion,
                    ImageReference = container?.ImageReference,
                    ImageDigest = container?.ImageDigest,
                    Durability = durability,
                    DataSet = summary,
                    FilterCategory = filterCategory,
                    ServerColumns = ServerColumnAvailability.FromSteps(productName, steps)
                })
            });

            var results = new List<AggregateRunResult> { Result("build", [buildStep], null, i => i with { Build = build }) };
            foreach (var (shape, closed, fixedRate, info) in queryRuns)
                results.Add(Result(shape, closed.Steps.Concat(fixedRate.Steps).ToList(), closed.HistogramArtifacts.Concat(fixedRate.HistogramArtifacts).ToList(), i => i with { Query = info }));
            results.Add(Result("under-write", underWriteSteps, quiet.HistogramArtifacts.Concat(underWrite.HistogramArtifacts).ToList(), i => i with { UnderWrite = underWriteInfo }));
            return results;
        }
        finally
        {
            if (settings.KeepData == false)
                await CleanupAsync(transport, url, database, createdDatabase);
        }
    }

    private async Task<(StepResult Step, AggregateBuildInfo Info)> BuildAsync(IYcsbTransport transport, AggregateDataSet dataSet, AggregateSetDigest digest,
        NodeExporterClient? nodeExporter, string? nodeExporterFailure, TimeSpan nonStaleTimeout, CancellationToken ct)
    {
        var (start, startFailure) = await TryScrapeAsync(nodeExporter, ct);
        var cpu = new ProcessCpuTracker();
        cpu.Start();
        var clock = Stopwatch.StartNew();

        await AggregateParityCheck.LoadInBatchesAsync(transport, dataSet.Generate().Select(d =>
        {
            digest.Add(d);
            return d;
        }), ct);
        var loadSeconds = clock.Elapsed.TotalSeconds;

        IReadOnlyList<string> indexes;
        switch (transport)
        {
            case RawHttpTransport raw:
                await raw.EnsureAggregateIndexesAsync(ct);
                await raw.WaitForNonStaleAggregateIndexesAsync(nonStaleTimeout, ct);
                indexes = AggregateShapes.RavenDbIndexes().Select(i => i.GetProperty("Name").GetString()!).ToList();
                break;
            case MongoYcsbTransport mongo:
                await mongo.EnsureAggregateIndexesAsync(ct);
                indexes = await mongo.ListAggregateIndexNamesAsync(ct);
                break;
            default:
                throw new NotSupportedException($"{transport.GetType().Name} is not an aggregate transport.");
        }
        // Queryable means every shape answers once, and on RavenDB the wait above already saw every index non-stale.
        foreach (var shape in AggregateShapes.All)
        {
            var op = AggregateShapes.Create(shape, 1, AggregateDataSet.CategoryKey(1, dataSet.Spec.CategoryCardinality));
            var result = await transport.ExecuteAsync(op, ct);
            if (result.IsSuccess == false)
                throw new InvalidOperationException($"The {shape} query is not queryable after the build: {result.ErrorDetails}");
        }
        clock.Stop();
        cpu.Stop();

        var (end, endFailure) = await TryScrapeAsync(nodeExporter, ct);
        var unavailable = nodeExporterFailure ?? startFailure ?? endFailure;
        SourcedFigure serverCpu, disk;
        if (start is null || end is null)
        {
            serverCpu = SourcedFigure.Missing("percent", unavailable ?? "no server source configured: pass --node-exporter-url");
            disk = SourcedFigure.Missing("bytes", unavailable ?? "no server source configured: pass --node-exporter-url");
        }
        else
        {
            serverCpu = SourcedFigure.Read(NodeExporterSample.CpuPercent(start, end), "percent", NodeExporterClient.SourceName + " (host-wide)");
            disk = start.DiskWrittenBytes is { } before && end.DiskWrittenBytes is { } after
                ? SourcedFigure.Read(after - before, "bytes", $"{NodeExporterClient.SourceName} {NodeExporterSample.DiskWrittenSeries}, summed over devices (host-wide)")
                : SourcedFigure.Missing("bytes", $"{NodeExporterClient.SourceName} exposes no {NodeExporterSample.DiskWrittenSeries} series");
        }
        OnDiskSize onDisk;
        try
        {
            var sized = (IReportsStorageSize)transport;
            onDisk = OnDiskSize.Reported(sized.StorageSizeMetricName, await sized.GetStorageSizeBytesAsync());
        }
        catch (Exception ex) when (ex is InvalidOperationException or HttpRequestException)
        {
            onDisk = new OnDiskSize { Unavailable = $"{transport.ProductName} did not report its size: {ex.Message}" };
        }

        var step = new StepResult
        {
            Concurrency = 1,
            Throughput = digest.Summary().Count / loadSeconds,
            MeasuredDuration = clock.Elapsed,
            ClientCpu = cpu.AverageCpu,
            InvalidReason = ClientSaturation.MarkingFor(cpu.AverageCpu),
            ServerCpu = serverCpu.Value,
            ServerCpuSource = serverCpu.Source,
            ServerMetricsHostWide = serverCpu.Value.HasValue ? true : null,
            ServerMetricsUnavailable = serverCpu.Unavailable
        };
        return (step, new AggregateBuildInfo(clock.Elapsed.TotalSeconds, loadSeconds, clock.Elapsed.TotalSeconds - loadSeconds, indexes, serverCpu, disk, onDisk));
    }

    /// <summary>
    /// Runs the bulk writers at <c>writeRate</c> and the probe at the query rate, both for the whole
    /// under-write step. Freshness takes only the probe's acknowledgements: the probe is the only
    /// writer that moves a document into the tracked group, one update in flight, so the tracked count
    /// after the k-th acknowledged probe write is the baseline plus k.
    /// </summary>
    private async Task<(BenchmarkRunner.RampResult Ramp, HeldWriteRate Writer, HeldWriteRate Probe)> UnderWriteAsync(IYcsbTransport transport, AggregateDataSet dataSet,
        IReadOnlyDictionary<string, long> categoryCounts, FreshnessTracker tracker, IWorkload workload, int queryRate, string url, string database, TimeSpan warmup, TimeSpan duration,
        NodeExporterClient? nodeExporter, CancellationToken ct)
    {
        // Each writer owns about as many documents as its share of the step's updates.
        var documentsPerWriter = (long)Math.Ceiling(scenario.WriteRate * (warmup + duration).TotalSeconds / scenario.Writers);
        var bulkWriters = UnderWriteSplit.BulkWriters(dataSet.Generate(), categoryCounts, tracker.TrackedGroup, scenario.Writers, documentsPerWriter, SeedMixer.Derive(scenario.Seed, "under-write-bulk"));
        // A step at the query rate needs one probe document per tick; the step may run past its window, so the probe keeps every document.
        var probeSources = UnderWriteSplit.ProbeSources(dataSet.Generate(), tracker.TrackedGroup, (long)Math.Ceiling(queryRate * (warmup + duration).TotalSeconds));
        var probed = 0;
        var probeAmounts = new Random(SeedMixer.Derive(scenario.Seed, "under-write"));

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var writer = Task.Run(() => PacedWriter.RunAsync(scenario.WriteRate, scenario.Writers, async (w, token) =>
        {
            await UpdateAsync(transport, bulkWriters[w].Next(), token);
            return true;
        }, _ => { }, stop.Token), CancellationToken.None);
        var probe = Task.Run(() => PacedWriter.RunAsync(queryRate, 1, async (_, token) =>
        {
            if (probed == probeSources.Length)
                throw new InvalidOperationException($"No document outside the tracked group '{tracker.TrackedGroup}' is left for the probe; raise documentCount or shorten the step.");
            var op = new AggregateUpdateOperation { Id = probeSources[probed++].Id, Category = tracker.TrackedGroup, Amount = probeAmounts.NextInt64(1, AggregateDataSet.MaxAmount + 1) };
            await UpdateAsync(transport, op, token);
            return true;
        }, tracker.Acknowledged, stop.Token), CancellationToken.None);

        var ramp = await AlongsideAsync(RampAsync(new FreshnessRecordingTransport(transport, tracker), workload, url, database, LoadShape.Rate, queryRate, scenario.Concurrency, warmup, duration, "under-write", nodeExporter),
            stop, writer, probe);
        return (ramp, await writer, await probe);
    }

    /// <summary>
    /// Awaits <paramref name="foreground"/>, then cancels <paramref name="stop"/>. When the foreground throws, every
    /// background task is awaited before the foreground exception propagates; a background fault is observed but never replaces it.
    /// </summary>
    internal static async Task<T> AlongsideAsync<T>(Task<T> foreground, CancellationTokenSource stop, params Task[] background)
    {
        try
        {
            return await foreground;
        }
        catch
        {
            stop.Cancel();
            try
            {
                await Task.WhenAll(background);
            }
            catch (Exception)
            {
                // The foreground exception is the cause; a background fault after the stop is its consequence.
            }
            throw;
        }
        finally
        {
            stop.Cancel();
        }
    }

    private static async Task UpdateAsync(IYcsbTransport transport, AggregateUpdateOperation op, CancellationToken token)
    {
        var result = await transport.ExecuteAsync(op, token);
        if (result.Cancelled)
            throw new OperationCanceledException(token);
        if (result.IsSuccess == false)
            throw new InvalidOperationException($"The update of '{op.Id}' failed: {result.ErrorDetails}");
    }

    private static IReadOnlyList<AggregateStepAnswers> Answers(IReadOnlyList<StepResult> steps) => steps.Select((s, i) => AggregateStepAnswers.From(i, s)).ToList();

    private static string? Describe(AggregateFilter? filter) => filter switch
    {
        null => null,
        EqualityFilter eq => $"{eq.Field} = {eq.Value}",
        RangeFilter range => $"{range.Lower} <= {range.Field} < {range.Upper}",
        _ => filter.ToString()
    };

    /// <summary>
    /// Writes the emitted set's summary under the data directory. A manifest from an earlier run with
    /// the same spec but another checksum means the generator changed, and the run stops.
    /// </summary>
    private static void RecordManifest(string dataDirectory, AggregateDataSpec spec, AggregateDataSetSummary summary)
    {
        Directory.CreateDirectory(dataDirectory);
        var path = Path.Combine(dataDirectory, $"aggregate-{spec.Seed}-{spec.DocumentCount}-{spec.DocumentSizeBytes}-{spec.CategoryCardinality}-{spec.RegionCardinality}-{spec.Distribution}.json");
        if (File.Exists(path) && JsonSerializer.Deserialize<AggregateDataSetSummary>(File.ReadAllText(path)) is { } earlier && earlier != summary)
            throw new InvalidDataException($"The emitted set differs from the manifest '{path}': checksum {summary.Checksum}, recorded {earlier.Checksum}.");
        File.WriteAllText(path, JsonSerializer.Serialize(summary));
    }

    private static async Task<bool> CreateRavenDatabaseAsync(string url, string database, CancellationToken ct)
    {
        using var store = HttpHelper.Create(url, database, HttpVersion.Version11);
        if (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(database), ct) is not null)
            return false;
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)), ct);
        return true;
    }

    /// <summary>A busy server confirms a committed delete later than the client default wait.</summary>
    private static readonly TimeSpan DeleteConfirmationWait = TimeSpan.FromMinutes(2);

    private static async Task CleanupAsync(IYcsbTransport transport, string url, string database, bool createdDatabase)
    {
        if (transport is MongoYcsbTransport mongo)
        {
            await mongo.DropAggregateCollectionAsync(CancellationToken.None);
            return;
        }
        if (createdDatabase == false)
            return;
        using var store = HttpHelper.Create(url, database, HttpVersion.Version11);
        await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true, timeToWaitForConfirmation: DeleteConfirmationWait));
    }

    /// <summary>An endpoint that does not answer leaves the server figures unavailable with its reason, rather than failing the run.</summary>
    private static async Task<(NodeExporterClient? Client, string? Failure)> ConnectNodeExporterAsync(string? url)
    {
        var endpoint = CliParsing.ParseNodeExporterUrl(url);
        try
        {
            return (await NodeExporterClient.ConnectAsync(endpoint), null);
        }
        catch (NodeExporterException ex)
        {
            Console.Error.WriteLine($"[Aggregate] WARNING: {ex.Message}; the server figures are recorded as unavailable.");
            return (null, ex.Message);
        }
    }

    private static async Task<(NodeExporterSample? Sample, string? Failure)> TryScrapeAsync(NodeExporterClient? client, CancellationToken ct)
    {
        if (client is null)
            return (null, null);
        try
        {
            return (await client.ScrapeAsync(ct), null);
        }
        catch (NodeExporterException ex)
        {
            return (null, ex.Message);
        }
    }

    private async Task<BenchmarkRunner.RampResult> RampAsync(IYcsbTransport transport, IWorkload workload, string url, string database, LoadShape shape, int value,
        int? rateWorkers, TimeSpan warmup, TimeSpan duration, string name, NodeExporterClient? nodeExporter)
    {
        var opts = new RunOptions
        {
            Url = url,
            Database = database,
            Seed = SeedMixer.Derive(scenario.Seed, name),
            Warmup = warmup,
            Duration = duration,
            Shape = shape,
            Step = new StepPlan(value, value, 2.0),
            RateWorkers = rateWorkers,
            LatencyHistogramsDir = settings.OutputPrefix is null ? null : $"{settings.OutputPrefix}-{name}",
            LatencyHistogramsFormat = HistogramExportFormat.Both
        };
        var executor = new BenchmarkExecutor(opts, transport, workload, new ProcessCpuTracker(), serverTracker: null, name, nodeExporter);
        return await BenchmarkRunner.RunRampAsync(opts, transport, executor, workload, startupCalibration: null, new Random(opts.Seed));
    }

    private static string Required(string? value, string option) =>
        string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"{option} is required.", option) : value;
}
