using System.Collections.Concurrent;
using System.Diagnostics;
using RavenBench.Analysis;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Vector;
using RavenBench.Core.Workload;
using RavenBench.Dataset;
using RavenBench.Dataset.Vectors;
using RavenBench.Ycsb;

namespace RavenBench.VectorBench;

/// <summary>A vector search that remembers which query it carries.</summary>
public sealed class TrackedVectorSearch : VectorSearchOperation
{
    public required int QueryIndex { get; init; }
}

/// <summary>Draws the selected queries at one effort; every operation is a <see cref="TrackedVectorSearch"/>.</summary>
public sealed class VectorQueryWorkload(float[][] queries, IVectorTarget target, int k, SearchEffort effort) : IWorkload
{
    public OperationBase NextOperation(Random rng)
    {
        var i = rng.Next(queries.Length);
        return new TrackedVectorSearch
        {
            QueryIndex = i,
            QueryVector = queries[i],
            FieldName = target.FieldName,
            TopK = k,
            ExpectedIndex = target.ExpectedIndex,
            Effort = effort
        };
    }
}

/// <summary>
/// Forwards to the product transport and keeps the ids every tracked search returned, with the
/// acknowledged insert count read when the search is sent, not when the workload created it.
/// </summary>
public sealed class RecordingTransport(IYcsbTransport inner, Func<long> insertedBefore) : IYcsbTransport
{
    public ConcurrentQueue<(int Query, long InsertedBefore, IReadOnlyList<string> Ids)> Results { get; } = new();

    public string ProductName => inner.ProductName;
    public bool ReportsWireBytes => inner.ReportsWireBytes;

    public async Task<TransportResult> ExecuteAsync(OperationBase op, CancellationToken ct)
    {
        var before = insertedBefore();
        var result = await inner.ExecuteAsync(op, ct).ConfigureAwait(false);
        if (op is TrackedVectorSearch tracked && result.IsSuccess && result.Cancelled == false)
            Results.Enqueue((tracked.QueryIndex, before, result.NeighborIds ?? throw new InvalidOperationException($"{inner.ProductName} returned no neighbour ids.")));
        return result;
    }

    public Task PutAsync<T>(string id, T document) => inner.PutAsync(id, document);
    public Task EnsureDatabaseExistsAsync(string databaseName) => inner.EnsureDatabaseExistsAsync(databaseName);
    public Task<long> GetDocumentCountAsync(string idPrefix) => inner.GetDocumentCountAsync(idPrefix);
    public Task<string> GetServerVersionAsync() => inner.GetServerVersionAsync();

    // The inner transport belongs to the target, which disposes it.
    public void Dispose()
    {
    }
}

public sealed record VectorRunResult(string Run, BenchmarkSummary Summary);

/// <summary>
/// Runs the five vector runs against one target: load, recall, readers, filtered and under-insert.
/// Recall is always scored against the set's product-neutral truth, never a product's own search.
/// </summary>
/// <param name="setOverride">The set to run; null resolves the scenario's dataset name.</param>
public sealed class VectorRunner(VectorScenario scenario, IReadOnlyDictionary<string, string> overrides, VectorSettings settings, IVectorDataset? setOverride = null)
{
    public const string RavendbTarget = YcsbRunner.RavendbTarget;
    public const string Ravendb7Target = YcsbRunner.Ravendb7Target;

    public static readonly IReadOnlyList<string> Runs = ["load", "recall", "readers", "filtered", "under-insert"];

    public static IVectorDataset ResolveSet(string name)
    {
        if (VectorSets.FindPublished(name) is { } published)
            return published;
        if (name.StartsWith("sphere-", StringComparison.OrdinalIgnoreCase))
            return new SphereDatasetProvider(name["sphere-".Length..]);
        if (name.StartsWith("clinical-words-", StringComparison.OrdinalIgnoreCase) && int.TryParse(name["clinical-words-".Length..], out var dims))
            return new ClinicalWordsDatasetProvider(dims);
        throw new VectorScenarioException($"Scenario key 'Dataset' is '{name}'; valid sets are {string.Join(", ", VectorSets.Published.Select(s => s.Name))}, sphere-<profile> and clinical-words-<100|300|600>.");
    }

    /// <summary>Builds the target for the set's metric. A metric the product cannot serve is refused here, before any load.</summary>
    /// <param name="concurrency">The most requests the run holds in flight; a pgvector pool opens one connection per request.</param>
    public static IVectorTarget BuildTarget(string target, string url, string database, VectorMetric metric, int dimensions, int concurrency, VectorScenario scenario)
    {
        if (string.Equals(target, RavendbTarget, StringComparison.OrdinalIgnoreCase) || string.Equals(target, Ravendb7Target, StringComparison.OrdinalIgnoreCase))
            return new RavenDbVectorTarget(url, database, metric);
        if (string.Equals(target, PgVectorTransport.Target, StringComparison.OrdinalIgnoreCase))
            return new PgVectorTarget(new PgVectorTransport(url, database, concurrency, metric, dimensions));
        if (string.Equals(target, ElasticsearchVectorTransport.Target, StringComparison.OrdinalIgnoreCase))
            return new ElasticsearchVectorTarget(new ElasticsearchVectorTransport(url, database, metric, dimensions, scenario.ElasticsearchIndexKind));
        throw new VectorScenarioException($"Target '{target}' is not a vector target; valid targets are '{RavendbTarget}', '{Ravendb7Target}', '{PgVectorTransport.Target}' and '{ElasticsearchVectorTransport.Target}'.");
    }

    public async Task<List<VectorRunResult>> RunAsync(CancellationToken ct = default)
    {
        var targetName = Required(settings.Target, "--target");
        var url = Required(settings.Url, "--url");
        var database = Required(settings.Database, "--database");
        var warmup = CliParsing.ParseDuration(scenario.Warmup);
        var duration = CliParsing.ParseDuration(scenario.Duration);
        var k = scenario.K;

        var set = setOverride ?? ResolveSet(scenario.Dataset);
        if (scenario.VectorCountCap is { } cap)
        {
            scenario.RequireInsertSliceBelow(cap, warmup, duration);
            set = new CappedVectorDataset(set, cap);
        }
        using var target = BuildTarget(targetName, url, database, set.Metric, set.Dimensions, scenario.Readers, scenario);
        var efforts = scenario.EffortsFor(target.EffortFamily);
        if (efforts.Knob != target.Effort(efforts.Default).Knob)
            throw new VectorScenarioException($"Scenario key 'Efforts.{target.EffortFamily}.Knob' is '{efforts.Knob}'; the product's knob is '{target.Effort(efforts.Default).Knob}'.");
        using var nodeExporter = await NodeExporterClient.ConnectAsync(CliParsing.ParseNodeExporterUrl(settings.NodeExporterUrl));

        // Verified on every run before any product request.
        var files = await PinnedFiles.EnsureAsync(set, Environment.ExpandEnvironmentVariables(scenario.DataDirectory), ct: ct);
        var selection = new QuerySelection(scenario.Seed, scenario.QueryCount);
        var queries = await set.GetQueriesAsync(files, selection, scenario.TruthDepth, ct);
        var baseCount = await set.BaseCountAsync(files, selection, ct);
        scenario.RequireInsertSliceBelow(baseCount, warmup, duration);
        var split = new VectorSplit(baseCount, scenario.Seed, scenario.InsertCount(warmup, duration), scenario.FilterSelectivity);

        VectorResourceCheck.Require(split.BaseCount, set.Dimensions, Environment.ExpandEnvironmentVariables(scenario.DataDirectory));

        // One pass outside any product: the insert slice, the labelled subset, and the vectors of the quiet truth.
        var slice = new List<BaseVector>();
        var labelled = new List<BaseVector>();
        await foreach (var (v, label) in Split(set.ReadBaseAsync(files, selection, ct), split, slice))
            if (label == VectorSplit.LabelIn)
                labelled.Add(v);
        var sliceIds = slice.Select(v => v.Id).ToHashSet(StringComparer.Ordinal);
        var quietTruth = VectorRunMath.WithoutSlice(queries.Neighbors, sliceIds, k);
        var filteredTruth = await BruteForceTruth.ComputeAsync(queries.Queries, labelled.ToAsyncEnumerable(), set.Metric, k, ct);
        var quietIds = quietTruth.SelectMany(t => t).ToHashSet(StringComparer.Ordinal);
        var quietVectors = new Dictionary<string, float[]>(StringComparer.Ordinal);
        await foreach (var v in set.ReadBaseAsync(files, selection, ct))
            if (quietIds.Contains(v.Id))
                quietVectors[v.Id] = v.Vector;
        var quietTopK = quietTruth.Select(t => (IReadOnlyList<BaseVector>)t.Select(id => new BaseVector(id, quietVectors[id])).ToList()).ToArray();

        var datasetInfo = new VectorDatasetInfo(set.Name, set.Metric.ToString(), set.Dimensions, scenario.VectorCountCap, split.LoadedCount, files.Fingerprint, set switch
        {
            CappedVectorDataset c => $"exact float32 brute force over the first {c.Cap} base vectors, computed outside any product and cached next to the data",
            HeldOutVectorDataset => "exact float32 brute force over the loaded base, computed outside any product and cached next to the data",
            _ => "the set's published neighbours for its query split"
        });
        Console.WriteLine($"[Vector] {set.Name}: {split.LoadedCount:N0} loaded, {slice.Count:N0} held for inserts, {split.LabelledCount:N0} labelled, {queries.Queries.Length} queries");

        // load
        var loadStep = await BracketAsync(nodeExporter, async () =>
        {
            await target.LoadAsync(Labelled(set.ReadBaseAsync(files, selection, ct), split), ct);
            return split.LoadedCount;
        }, ct);
        var peakMemory = loadStep.Peak;
        var loadInfo = new VectorLoadInfo(loadStep.Step.MeasuredDuration!.Value.TotalSeconds, peakMemory.MemoryMB, peakMemory.Source, peakMemory.Unavailable, await target.StoredSizeAsync());

        var productName = target.Transport.ProductName;
        var serverVersion = await target.Transport.GetServerVersionAsync();
        var productSettings = await target.ReportedSettingsAsync(ct);
        var container = IsContainerized(targetName) && Uri.TryCreate(url, UriKind.Absolute, out var uri) && uri.Port > 0
            ? new DockerDatabaseContainerLocator().Locate(uri.Port)
            : null;
        var fingerprint = new MachineFingerprintCollector(new NativeMachineFingerprintSource()).Collect(RepositoryRootLocator.Find(), container);

        // recall
        var curve = new List<VectorEffortPoint>();
        var recallSteps = new List<StepResult>();
        foreach (var (label, value) in efforts.All)
        {
            var (step, ids) = await SequentialAsync(target, queries.Queries, k, target.Effort(value), filter: null, nodeExporter, ct);
            var recall = ids.Select((r, q) => VectorRunMath.Recall(r, quietTruth[q], k)).Average();
            curve.Add(new VectorEffortPoint(label, efforts.Knob, value, recall, step.Throughput, recallSteps.Count, ids.Sum(r => (long)r.Count), ids.Count(r => r.Count < k)));
            recallSteps.Add(step);
            Console.WriteLine($"[Vector] recall@{k} at {efforts.Knob}={value} ({label}): {recall:P2}, {step.Throughput:F0} q/s");
        }
        var selected = VectorRunMath.SelectLowest(curve, scenario.RecallThreshold);
        var recallInfo = new VectorRecallInfo(k, scenario.RecallThreshold, curve, selected, selected is null
            ? $"no setting reached recall@{k} {scenario.RecallThreshold}"
            : $"{efforts.Knob}={selected.Value} is the lowest setting that reached recall@{k} {scenario.RecallThreshold}");
        var inForce = selected ?? curve.MaxBy(p => p.Value)!;
        var effortStatement = selected is null
            ? $"no setting reached recall@{k} {scenario.RecallThreshold}; readers, filtered and under-insert run at the high setting, {efforts.Knob}={inForce.Value}"
            : $"readers, filtered and under-insert run at {efforts.Knob}={inForce.Value}, the lowest setting that reached recall@{k} {scenario.RecallThreshold}";
        var effort = target.Effort(inForce.Value);

        // readers: the closed loop finds the sustained rate, then the fixed-rate runner runs at it.
        var readersWorkload = new VectorQueryWorkload(queries.Queries, target, k, effort);
        var closed = await RampAsync(target.Transport, readersWorkload, url, database, LoadShape.Closed, scenario.Readers, rateWorkers: null, warmup, duration, "readers-closed", nodeExporter);
        var sustained = Math.Max(1, (int)Math.Floor(closed.Steps[^1].Throughput));
        var fixedRate = await RampAsync(target.Transport, readersWorkload, url, database, LoadShape.Rate, sustained, rateWorkers: scenario.Readers, warmup, duration, "readers-rate", nodeExporter);
        var readerSteps = closed.Steps.Concat(fixedRate.Steps).ToList();
        var readersInfo = new VectorReadersInfo(scenario.Readers, closed.Steps[^1].Throughput, sustained, readerSteps.Any(s => ClientSaturation.IsSaturated(s.ClientCpu)));

        // filtered
        var filter = new VectorFilter(target.FilterField, VectorSplit.LabelIn);
        var (filteredStep, filteredIds) = await SequentialAsync(target, queries.Queries, k, effort, filter, nodeExporter, ct);
        var rowCounts = filteredIds.Select(r => r.Count).ToList();
        var filteredInfo = new VectorFilteredInfo(scenario.FilterSelectivity, split.LabelledCount,
            "drawn with the scenario seed over the loaded base; no set here ships labels",
            rowCounts, rowCounts.Count(c => c < k),
            filteredIds.Select((r, q) => VectorRunMath.Recall(r, filteredTruth[q], k)).Average(),
            $"truth is the exact top {k} within the label; a query that returned fewer than {k} rows keeps its count and scores its missing rows as misses"
            + (productSettings.TryGetValue("hnsw.iterative_scan", out var iterative) ? $"; hnsw.iterative_scan={iterative}, hnsw.max_scan_tuples={productSettings["hnsw.max_scan_tuples"]} as the server reports them" : ""));

        // under-insert
        var (underInsertRamp, underInsertInfo) = await UnderInsertAsync(target, queries.Queries, quietTopK, slice, set.Metric, effort, inForce.Recall, url, database, warmup, duration, nodeExporter, ct);

        // cross-check: last, because it replaces the index the other runs measured.
        var crossCheck = await CrossCheckAsync(target, set, queries, slice, split.LoadedCount, productSettings, datasetInfo.TruthSource, nodeExporter, ct);

        var common = (Target: targetName, Product: productName, Version: serverVersion, Container: container, Settings: productSettings, Dataset: datasetInfo, Fingerprint: fingerprint, Durability: target.Durability);
        VectorRunResult Result(string run, List<StepResult> steps, List<HistogramArtifact>? histograms, Func<VectorRunInfo, VectorRunInfo> fill)
        {
            var opts = new RunOptions { Url = url, Database = database, Seed = scenario.Seed, Warmup = warmup, Duration = duration };
            var info = fill(new VectorRunInfo
            {
                Run = run,
                ResolvedScenario = scenario,
                Overrides = overrides,
                Target = common.Target,
                ProductName = common.Product,
                ServerVersion = common.Version,
                ImageReference = common.Container?.ImageReference,
                ImageDigest = common.Container?.ImageDigest,
                Durability = common.Durability,
                ProductSettings = common.Settings,
                VectorStorage = target.VectorStorage,
                RowLabel = $"{common.Target} {target.VectorStorage}",
                Dataset = common.Dataset,
                ServerColumns = ServerColumnAvailability.FromSteps(common.Product, steps),
                EffortInForce = run is "readers" or "filtered" or "under-insert" ? inForce : null,
                EffortStatement = run is "readers" or "filtered" or "under-insert" ? effortStatement : null
            });
            return new VectorRunResult(run, new BenchmarkSummary
            {
                Options = opts,
                Steps = steps,
                Verdict = steps.Any(s => ClientSaturation.IsSaturated(s.ClientCpu)) ? "client-bound" : "measured",
                ClientCompression = "n/a",
                EffectiveHttpVersion = "n/a",
                HistogramArtifacts = histograms is { Count: > 0 } ? histograms : null,
                MachineFingerprint = common.Fingerprint,
                Vector = info
            });
        }

        try
        {
            return
            [
                Result("load", [loadStep.Step], null, i => i with { Load = loadInfo }),
                Result("recall", recallSteps, null, i => i with { Recall = recallInfo, CrossCheck = crossCheck }),
                Result("readers", readerSteps, closed.HistogramArtifacts.Concat(fixedRate.HistogramArtifacts).ToList(), i => i with { Readers = readersInfo }),
                Result("filtered", [filteredStep], null, i => i with { Filtered = filteredInfo }),
                Result("under-insert", underInsertRamp.Steps, underInsertRamp.HistogramArtifacts, i => i with { UnderInsert = underInsertInfo })
            ];
        }
        finally
        {
            if (settings.KeepData == false)
                await target.CleanupAsync();
        }
    }

    /// <summary>
    /// For pgvector on the cross-check set: inserts every slice vector the under-insert run left out, so the
    /// index holds the whole base, rebuilds the published index kind with the published options, searches at
    /// the published setting, and scores against the set's own truth for its query split.
    /// </summary>
    private async Task<VectorCrossCheckInfo?> CrossCheckAsync(IVectorTarget target, IVectorDataset set, VectorQuerySet queries,
        IReadOnlyList<BaseVector> slice, long loaded, IReadOnlyDictionary<string, string> defaultSettings, string truthSource, NodeExporterClient? nodeExporter, CancellationToken ct)
    {
        var check = scenario.CrossCheck;
        if (target is not PgVectorTarget pgvector || string.Equals(set.Name, check.Dataset, StringComparison.OrdinalIgnoreCase) == false)
            return null;

        foreach (var missing in VectorRunMath.MissingFromServer(slice, await pgvector.ReadStoredIdsAsync(ct)))
        {
            var result = await target.Transport.ExecuteAsync(target.InsertOperation(new LabelledVector(missing.Id, missing.Vector, VectorSplit.LabelOut)), ct);
            if (result.IsSuccess == false)
                throw new InvalidOperationException($"Insert of slice vector '{missing.Id}' for the cross-check failed: {result.ErrorDetails}");
        }

        var knob = PgVectorTransport.SearchKnobs[check.PublishedIndexKind];
        var defaultBuild = VectorBuildState.DefaultBuild(check.PublishedIndexKind, check.PublishedBuildOptions, defaultSettings["indexdef"]);
        var definition = await pgvector.ReplaceIndexAsync(check.PublishedIndexKind, check.PublishedBuildOptions, check.BuildSession, ct);
        var (_, ids) = await SequentialAsync(target, queries.Queries, scenario.K, new SearchEffort(knob, check.PublishedSearchValue), filter: null, nodeExporter, ct);
        var measured = ids.Select((r, q) => VectorRunMath.Recall(r, queries.Neighbors[q], scenario.K)).Average();
        var distance = Math.Abs(measured - check.PublishedRecall);
        var verdict = scenario.VectorCountCap is { } cap
            ? $"skipped: capped at {cap} vectors; the published figure is for the whole set"
            : distance <= check.Tolerance
                ? $"near: |{measured:F4} - {check.PublishedRecall:F4}| = {distance:F4} <= {check.Tolerance}"
                : $"not near: |{measured:F4} - {check.PublishedRecall:F4}| = {distance:F4} > {check.Tolerance}";
        return new VectorCrossCheckInfo(check.Dataset, check.PublishedRecall, check.Source, check.PublishedSettings, check.PublishedIndexKind, check.PublishedBuildOptions, check.BuildSession,
            knob, check.PublishedSearchValue, definition, defaultBuild, truthSource, loaded + slice.Count, measured, check.Evidence, check.EvidenceSha256, check.Tolerance, verdict);
    }

    private async Task<(BenchmarkRunner.RampResult Ramp, VectorUnderInsertInfo Info)> UnderInsertAsync(IVectorTarget target, float[][] queries,
        IReadOnlyList<BaseVector>[] quietTopK, IReadOnlyList<BaseVector> slice, VectorMetric metric, SearchEffort effort, double quietRecall,
        string url, string database, TimeSpan warmup, TimeSpan duration, NodeExporterClient? nodeExporter, CancellationToken ct)
    {
        long acknowledged = 0;
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        // One insert in flight at a time, in slice order, so the first n acknowledged inserts are always slice[0..n).
        var inserter = Task.Run(async () =>
        {
            var clock = Stopwatch.StartNew();
            for (int i = 0; i < slice.Count && stop.IsCancellationRequested == false; i++)
            {
                var due = TimeSpan.FromSeconds(i / scenario.InsertRate) - clock.Elapsed;
                if (due > TimeSpan.Zero)
                    await Task.Delay(due, stop.Token).ContinueWith(_ => { }, TaskScheduler.Default);
                if (stop.IsCancellationRequested)
                    break;
                var result = await target.Transport.ExecuteAsync(target.InsertOperation(new LabelledVector(slice[i].Id, slice[i].Vector, VectorSplit.LabelOut)), stop.Token);
                if (result.Cancelled)
                    break;
                if (result.IsSuccess == false)
                    throw new InvalidOperationException($"Insert of slice vector '{slice[i].Id}' failed: {result.ErrorDetails}");
                Interlocked.Increment(ref acknowledged);
            }
        }, CancellationToken.None);

        var recording = new RecordingTransport(target.Transport, () => Interlocked.Read(ref acknowledged));
        var workload = new VectorQueryWorkload(queries, target, scenario.K, effort);
        BenchmarkRunner.RampResult ramp;
        try
        {
            ramp = await RampAsync(recording, workload, url, database, LoadShape.Rate, (int)Math.Round(scenario.UnderInsertQueryRate), rateWorkers: null, warmup, duration, "under-insert", nodeExporter);
        }
        finally
        {
            stop.Cancel();
            await inserter;
        }

        var prefix = target.IdPrefix;
        var records = recording.Results.ToList();
        var truthByQuery = records.GroupBy(r => r.Query).ToDictionary(
            g => g.Key,
            g => VectorRunMath.TruthWithInserts(queries[g.Key], quietTopK[g.Key], slice, g.Select(r => r.InsertedBefore), metric, scenario.K));
        var recall = records.Count == 0
            ? throw new InvalidOperationException("The under-insert run recorded no successful query.")
            : records.Average(r => VectorRunMath.Recall(r.Ids.Select(id => StripPrefix(id, prefix)).ToList(), truthByQuery[r.Query][r.InsertedBefore], scenario.K));

        return (ramp, new VectorUnderInsertInfo(scenario.InsertRate, scenario.UnderInsertQueryRate, acknowledged, ramp.Steps[^1].Raw.P99, recall, quietRecall, records.Count,
            $"each query's truth is the loaded base plus every insert acknowledged before the query was sent; an insert still in flight at send time is not in its truth. Visibility is {target.InsertVisibility}"));
    }

    private static async IAsyncEnumerable<(BaseVector Vector, string Label)> Split(IAsyncEnumerable<BaseVector> stream, VectorSplit split, List<BaseVector> slice)
    {
        long position = 0, loaded = 0;
        await foreach (var v in stream)
        {
            if (split.IsInsertSlice(position++))
                slice.Add(v);
            else
                yield return (v, split.LabelOf(loaded++));
        }
    }

    private static async IAsyncEnumerable<LabelledVector> Labelled(IAsyncEnumerable<BaseVector> stream, VectorSplit split)
    {
        await foreach (var (v, label) in Split(stream, split, []))
            yield return new LabelledVector(v.Id, v.Vector, label);
    }

    private sealed record PeakMemory(long? MemoryMB, string? Source, string? Unavailable);

    /// <summary>Times a body bracketed by two node_exporter scrapes, polling memory each second for its peak.</summary>
    private static async Task<(StepResult Step, PeakMemory Peak)> BracketAsync(NodeExporterClient? nodeExporter, Func<Task<long>> body, CancellationToken ct)
    {
        using var polling = new CancellationTokenSource();
        long peak = 0;
        var poller = nodeExporter is null ? Task.CompletedTask : Task.Run(async () =>
        {
            while (polling.IsCancellationRequested == false)
            {
                peak = Math.Max(peak, (await nodeExporter.ScrapeAsync(ct)).UsedMemoryMB);
                await Task.Delay(TimeSpan.FromSeconds(1), polling.Token).ContinueWith(_ => { }, TaskScheduler.Default);
            }
        }, CancellationToken.None);

        var start = nodeExporter?.ScrapeAsync(ct);
        var cpu = new ProcessCpuTracker();
        cpu.Start();
        var clock = Stopwatch.StartNew();
        var count = await body();
        clock.Stop();
        var window = start is null ? null : await nodeExporter!.EndWindowAsync(start, ct);
        polling.Cancel();
        string? peakFailure = null;
        try
        {
            await poller;
        }
        catch (Exception ex) when (ex is NodeExporterException or HttpRequestException)
        {
            peakFailure = $"{NodeExporterClient.SourceName}: {ex.Message}";
        }

        cpu.Stop();
        var step = StepFrom(count / clock.Elapsed.TotalSeconds, 1, clock.Elapsed, cpu.AverageCpu, window);
        var peakMemory = nodeExporter is null
            ? new PeakMemory(null, null, "no server source configured: pass --node-exporter-url")
            : peakFailure is null ? new PeakMemory(peak, NodeExporterClient.SourceName + " (host-wide, 1 s polls)", null) : new PeakMemory(null, null, peakFailure);
        return (step, peakMemory);
    }

    /// <summary>Runs every query once, in order, and keeps the ids each returned.</summary>
    private static async Task<(StepResult Step, List<IReadOnlyList<string>> Ids)> SequentialAsync(IVectorTarget target, float[][] queries, int k, SearchEffort effort,
        VectorFilter? filter, NodeExporterClient? nodeExporter, CancellationToken ct)
    {
        var ids = new List<IReadOnlyList<string>>(queries.Length);
        var (step, _) = await BracketAsync(nodeExporter, async () =>
        {
            foreach (var query in queries)
            {
                var result = await target.Transport.ExecuteAsync(new VectorSearchOperation
                {
                    QueryVector = query,
                    FieldName = target.FieldName,
                    TopK = k,
                    ExpectedIndex = target.ExpectedIndex,
                    Effort = effort,
                    Filter = filter
                }, ct);
                if (result.IsSuccess == false)
                    throw new InvalidOperationException($"{target.Transport.ProductName} vector search failed: {result.ErrorDetails}");
                var returned = result.NeighborIds ?? throw new InvalidOperationException($"{target.Transport.ProductName} returned no neighbour ids.");
                ids.Add(returned.Select(id => StripPrefix(id, target.IdPrefix)).ToList());
            }
            return queries.Length;
        }, ct);
        return (step, ids);
    }

    private static StepResult StepFrom(double throughput, int concurrency, TimeSpan elapsed, double clientCpu, NodeExporterWindow? window) => new()
    {
        Concurrency = concurrency,
        Throughput = throughput,
        MeasuredDuration = elapsed,
        ClientCpu = clientCpu,
        InvalidReason = ClientSaturation.MarkingFor(clientCpu),
        ServerCpu = window?.CpuPercent,
        ServerMemoryMB = window?.MemoryMB,
        ServerCpuSource = window?.CpuPercent.HasValue == true ? NodeExporterClient.SourceName : null,
        ServerMemorySource = window?.MemoryMB.HasValue == true ? NodeExporterClient.SourceName : null,
        ServerMetricsHostWide = window?.CpuPercent.HasValue == true || window?.MemoryMB.HasValue == true ? true : null,
        ServerMetricsUnavailable = window?.Unavailable
    };

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

    private static bool IsContainerized(string target) =>
        new[] { Ravendb7Target, PgVectorTransport.Target, ElasticsearchVectorTransport.Target }.Contains(target, StringComparer.OrdinalIgnoreCase);

    private static string StripPrefix(string id, string prefix) =>
        id.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            ? id[prefix.Length..]
            : throw new InvalidDataException($"Returned id '{id}' lacks the document prefix '{prefix}'.");

    private static string Required(string? value, string option) =>
        string.IsNullOrWhiteSpace(value) ? throw new VectorScenarioException($"{option} is required.") : value;
}
