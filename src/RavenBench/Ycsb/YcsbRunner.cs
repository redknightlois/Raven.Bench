using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;

namespace RavenBench.Ycsb;

/// <summary>
/// Drives one ycsb scenario end to end: load fills the keyspace, then the C, A, B and
/// insert-stream runs address it, each through the existing ramp/warmup/latency-recording path
/// (<see cref="BenchmarkRunner.RunRampAsync"/>) and each leaving its own result.
/// </summary>
public sealed class YcsbRunner
{
    private readonly YcsbScenario _scenario;
    private readonly YcsbSettings _settings;

    public YcsbRunner(YcsbScenario scenario, YcsbSettings settings)
    {
        _scenario = scenario;
        _settings = settings;
    }

    public async Task<List<(YcsbRunKind Kind, BenchmarkSummary Summary)>> RunAsync()
    {
        if (string.Equals(_scenario.Target, "ravendb", StringComparison.OrdinalIgnoreCase) == false)
            throw new YcsbScenarioException($"Scenario key 'Target' is '{_scenario.Target}'; only 'ravendb' has a transport today.");

        var seed = _scenario.Seed;
        var docSizeBytes = CliParsing.ParseSize(_scenario.DocumentSize);
        var distributionKind = CliParsing.ParseDistribution(_scenario.Distribution);
        var distribution = ToKeyDistribution(distributionKind);
        var warmup = CliParsing.ParseDuration(_scenario.Warmup);
        var duration = CliParsing.ParseDuration(_scenario.Duration);
        var url = RequiredString(_settings.Url, "--url");
        var database = RequiredString(_settings.Database, "--database");
        var transportKind = CliParsing.ParseTransport(_settings.Transport);
        var compression = CliParsing.ParseCompression(_settings.Compression);

        var negotiatedHttpVersion = await HttpVersionNegotiator.NegotiateVersionAsync(
            url, _settings.HttpVersion, _settings.StrictHttpVersion);
        var effectiveHttpVersion = HttpHelper.FormatHttpVersion(negotiatedHttpVersion);

        using var transport = BuildTransport(transportKind, url, database, compression, negotiatedHttpVersion);
        await transport.EnsureDatabaseExistsAsync(database);
        var serverVersion = await transport.GetServerVersionAsync();
        var clientCompression = transport switch
        {
            RavenClientTransport rc => rc.EffectiveCompressionMode,
            RawHttpTransport raw => raw.EffectiveCompressionMode,
            _ => "unknown"
        };
        // RavenDB writes with its own default durability; PostgreSQL and Mongo record
        // synchronous_commit=on / j=true when their transports land.
        var durability = new DurabilityParity { Setting = "durability", Value = "ravendb-default" };

        RunOptions BaseOptions(WorkloadProfile profile, StepPlan step, LoadShape shape) => new()
        {
            Url = url,
            Database = database,
            Transport = transportKind,
            Compression = compression,
            HttpVersion = _settings.HttpVersion,
            StrictHttpVersion = _settings.StrictHttpVersion,
            Seed = seed,
            DocumentSizeBytes = docSizeBytes,
            Distribution = distributionKind,
            Warmup = warmup,
            Duration = duration,
            Step = step,
            Shape = shape,
            Profile = profile,
            BulkBatchSize = _settings.BulkBatchSize,
            BulkDepth = _settings.BulkDepth,
            MaxErrorRate = CliParsing.ParsePercent(_settings.MaxErrors),
            LinkMbps = _settings.LinkMbps
        };

        var cpuTracker = new ProcessCpuTracker();
        var rng = new Random(seed);
        var results = new List<(YcsbRunKind, BenchmarkSummary)>();

        BenchmarkSummary Summary(RunOptions opts, BenchmarkRunner.RampResult ramp, YcsbRunKind kind) => new()
        {
            Options = opts,
            Steps = ramp.Steps,
            Verdict = "ycsb",
            ClientCompression = clientCompression,
            EffectiveHttpVersion = effectiveHttpVersion,
            HistogramArtifacts = ramp.HistogramArtifacts.Count > 0 ? ramp.HistogramArtifacts : null,
            Ycsb = new YcsbRunInfo
            {
                Run = kind.ToResultName(),
                ResolvedScenario = _scenario,
                ProductName = transport.ProductName,
                ServerVersion = serverVersion,
                Durability = durability
            }
        };

        var (loadStep, loadShape) = ResolveStepPlan(_scenario);
        var loadOpts = BaseOptions(WorkloadProfile.BulkWrites, loadStep, loadShape);
        var loadWorkload = new BulkWriteWorkload(docSizeBytes, loadOpts.BulkBatchSize, seed, startingKey: 0);
        var loadExecutor = new BenchmarkExecutor(loadOpts, transport, loadWorkload, cpuTracker);
        var loadRamp = await BenchmarkRunner.RunRampAsync(loadOpts, transport, loadExecutor, loadWorkload, startupCalibration: null, rng);
        results.Add((YcsbRunKind.Load, Summary(loadOpts, loadRamp, YcsbRunKind.Load)));

        var loadedCount = await transport.GetDocumentCountAsync("bench/");
        if (loadedCount < _scenario.DocumentCount)
        {
            throw new InsufficientKeyspaceException(
                $"The keyspace holds {loadedCount} documents but the scenario requires DocumentCount={_scenario.DocumentCount}. " +
                "C, A, B and insert-stream do not run against a short keyspace, and do not load it themselves.");
        }

        var (workStep, workShape) = ResolveStepPlan(_scenario);

        foreach (var (kind, mix) in new[]
                 {
                     (YcsbRunKind.WorkloadC, YcsbRunKinds.WorkloadC),
                     (YcsbRunKind.WorkloadA, YcsbRunKinds.WorkloadA),
                     (YcsbRunKind.WorkloadB, YcsbRunKinds.WorkloadB)
                 })
        {
            var opts = BaseOptions(WorkloadProfile.Mixed, workStep, workShape) with { Preload = _scenario.DocumentCount };
            var workload = new MixedProfileWorkload(mix, distribution, docSizeBytes, seed, initialKeyspace: _scenario.DocumentCount);
            var executor = new BenchmarkExecutor(opts, transport, workload, cpuTracker);
            var ramp = await BenchmarkRunner.RunRampAsync(opts, transport, executor, workload, startupCalibration: null, rng);
            results.Add((kind, Summary(opts, ramp, kind)));
        }

        var insertOpts = BaseOptions(WorkloadProfile.Writes, workStep, workShape);
        var insertWorkload = new WriteWorkload(docSizeBytes, seed, startingKey: _scenario.DocumentCount);
        var insertExecutor = new BenchmarkExecutor(insertOpts, transport, insertWorkload, cpuTracker);
        var insertRamp = await BenchmarkRunner.RunRampAsync(insertOpts, transport, insertExecutor, insertWorkload, startupCalibration: null, rng);
        results.Add((YcsbRunKind.InsertStream, Summary(insertOpts, insertRamp, YcsbRunKind.InsertStream)));

        return results;
    }

    /// <summary>
    /// The scenario's own concurrency step plan drives every run of the sequence: one field, one
    /// meaning, per the scenario's parameter table. A scenario rate switches every run to the
    /// rate load shape, at a fixed target (no ramp) sized by the rate value itself.
    /// </summary>
    private static (StepPlan Step, LoadShape Shape) ResolveStepPlan(YcsbScenario scenario)
    {
        if (scenario.Rate.HasValue)
        {
            var rate = (int)Math.Max(1, Math.Round(scenario.Rate.Value));
            return (new StepPlan(rate, rate, 2.0), LoadShape.Rate);
        }

        return (CliParsing.ParseStepPlan(scenario.Concurrency).Normalize(), LoadShape.Closed);
    }

    private static ITransport BuildTransport(TransportKind kind, string url, string database, CompressionMode compression, Version httpVersion) => kind switch
    {
        TransportKind.Raw => new RawHttpTransport(url, database, compression, httpVersion),
        TransportKind.Client => new RavenClientTransport(url, database, compression, httpVersion),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };

    private static string RequiredString(string? value, string optionName) =>
        string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"{optionName} is required") : value;

    private static IKeyDistribution ToKeyDistribution(KeyDistributionKind kind) => kind switch
    {
        KeyDistributionKind.Uniform => new UniformDistribution(),
        KeyDistributionKind.Zipfian => new ZipfianDistribution(),
        KeyDistributionKind.Latest => new LatestDistribution(),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };
}
