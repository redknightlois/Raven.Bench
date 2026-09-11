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
/// (<see cref="BenchmarkRunner.RunRampAsync"/>) and each leaving its own result. The scenario's
/// target selects the transport before any product-specific setup runs, so a Mongo target never
/// negotiates HTTP and never reads the RavenDB-only options.
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
        var seed = _scenario.Seed;
        var docSizeBytes = CliParsing.ParseSize(_scenario.DocumentSize);
        var distributionKind = CliParsing.ParseDistribution(_scenario.Distribution);
        var distribution = ToKeyDistribution(distributionKind);
        var warmup = CliParsing.ParseDuration(_scenario.Warmup);
        var duration = CliParsing.ParseDuration(_scenario.Duration);
        var url = RequiredString(_settings.Url, "--url");
        var database = RequiredString(_settings.Database, "--database");

        var context = await BuildContextAsync(url, database);
        using var transport = context.Transport;

        await transport.EnsureDatabaseExistsAsync(database);
        var serverVersion = await transport.GetServerVersionAsync();

        RunOptions BaseOptions(WorkloadProfile profile, StepPlan step, LoadShape shape) => new()
        {
            Url = context.RecordedUrl,
            Database = database,
            Transport = context.TransportKind,
            Compression = context.Compression,
            HttpVersion = context.HttpVersion,
            StrictHttpVersion = context.StrictHttpVersion,
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
            ClientCompression = context.ClientCompression,
            EffectiveHttpVersion = context.EffectiveHttpVersion,
            HistogramArtifacts = ramp.HistogramArtifacts.Count > 0 ? ramp.HistogramArtifacts : null,
            Ycsb = new YcsbRunInfo
            {
                Run = kind.ToResultName(),
                ResolvedScenario = _scenario,
                ProductName = transport.ProductName,
                ServerVersion = serverVersion,
                Durability = context.Durability
            }
        };

        var (loadStep, loadShape) = ResolveLoadStepPlan(_scenario);
        var loadOpts = BaseOptions(WorkloadProfile.BulkWrites, loadStep, loadShape) with { Warmup = TimeSpan.Zero };
        var loadWorkload = new BulkWriteWorkload(docSizeBytes, loadOpts.BulkBatchSize, seed, _scenario.DocumentCount, startingKey: 0);
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
    /// Selects the transport from the scenario's target, before any RavenDB-only setup, and
    /// carries the facts the result records for the target that actually ran. An unknown target
    /// fails naming the value rather than falling back to a default.
    /// </summary>
    private async Task<RunContext> BuildContextAsync(string url, string database)
    {
        if (string.Equals(_scenario.Target, "ravendb", StringComparison.OrdinalIgnoreCase))
            return await BuildRavenContextAsync(url, database);

        if (IsMongoTarget(_scenario.Target))
        {
            var transport = new MongoYcsbTransport(url, database, _scenario.Target);
            return new RunContext(
                transport,
                transport.RecordedEndpoint,
                ClientCompression: "n/a",
                EffectiveHttpVersion: "n/a",
                // Mongo writes at j:true, the parity setting recorded for the target.
                Durability: new DurabilityParity { Setting = "writeConcern", Value = "j=true" },
                TransportKind: TransportKind.Raw,
                Compression: CompressionMode.Identity,
                HttpVersion: "auto",
                StrictHttpVersion: false);
        }

        throw new YcsbScenarioException(
            $"Scenario key 'Target' is '{_scenario.Target}'; valid targets are 'ravendb', '{MongoYcsbTransport.MongoDbTarget}' and '{MongoYcsbTransport.DocumentDbTarget}'.");
    }

    private async Task<RunContext> BuildRavenContextAsync(string url, string database)
    {
        var transportKind = CliParsing.ParseTransport(_settings.Transport);
        var compression = CliParsing.ParseCompression(_settings.Compression);

        var negotiatedHttpVersion = await HttpVersionNegotiator.NegotiateVersionAsync(
            url, _settings.HttpVersion, _settings.StrictHttpVersion);

        IYcsbTransport transport = BuildRavenDbTransport(transportKind, url, database, compression, negotiatedHttpVersion);
        var clientCompression = transport switch
        {
            RavenClientTransport rc => rc.EffectiveCompressionMode,
            RawHttpTransport raw => raw.EffectiveCompressionMode,
            _ => "unknown"
        };

        return new RunContext(
            transport,
            RecordedUrl: url,
            ClientCompression: clientCompression,
            EffectiveHttpVersion: HttpHelper.FormatHttpVersion(negotiatedHttpVersion),
            // RavenDB writes with its own default durability; PostgreSQL and Mongo record
            // synchronous_commit=on and j=true when their transports land.
            Durability: new DurabilityParity { Setting = "durability", Value = "ravendb-default" },
            TransportKind: transportKind,
            Compression: compression,
            HttpVersion: _settings.HttpVersion,
            StrictHttpVersion: _settings.StrictHttpVersion);
    }

    private static bool IsMongoTarget(string target) =>
        string.Equals(target, MongoYcsbTransport.MongoDbTarget, StringComparison.OrdinalIgnoreCase)
        || string.Equals(target, MongoYcsbTransport.DocumentDbTarget, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// What the run records about its target that the run sequence itself does not compute. The
    /// recorded URL is redacted for a target whose endpoint is a connection string.
    /// </summary>
    private sealed record RunContext(
        IYcsbTransport Transport,
        string RecordedUrl,
        string ClientCompression,
        string EffectiveHttpVersion,
        DurabilityParity Durability,
        TransportKind TransportKind,
        CompressionMode Compression,
        string HttpVersion,
        bool StrictHttpVersion);

    /// <summary>
    /// The load run fills exactly the scenario's document count, so it uses the closed-loop
    /// generator at the scenario's concurrency and no warmup: a fill has no steady state to warm,
    /// and the bounded workload ends the step once the keyspace holds every document.
    /// </summary>
    private static (StepPlan Step, LoadShape Shape) ResolveLoadStepPlan(YcsbScenario scenario)
        => (CliParsing.ParseStepPlan(scenario.Concurrency).Normalize(), LoadShape.Closed);

    /// <summary>
    /// The scenario's own concurrency step plan drives the C, A and B runs: one field, one
    /// meaning, per the scenario's parameter table. A scenario rate switches those runs to the
    /// rate load shape, at a fixed target (no ramp) sized by the rate value itself. The load run
    /// resolves its own plan, because a bounded fill holds no rate.
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

    private static ITransport BuildRavenDbTransport(TransportKind kind, string url, string database, CompressionMode compression, Version httpVersion) => kind switch
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
