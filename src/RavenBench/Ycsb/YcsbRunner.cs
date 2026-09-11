using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Diagnostics;
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
    /// <summary>The external RavenDB development server target; the benchmark script never starts it.</summary>
    public const string RavendbTarget = "ravendb";

    /// <summary>The containerized RavenDB 6.x series target.</summary>
    public const string Ravendb6Target = "ravendb-6";

    /// <summary>The containerized RavenDB 7.x series target.</summary>
    public const string Ravendb7Target = "ravendb-7";

    private readonly YcsbScenario _scenario;
    private readonly YcsbSettings _settings;
    private readonly DockerDatabaseContainerLocator _containerLocator;
    private readonly NativeMachineFingerprintSource _fingerprintSource;

    // Distinguishes the default histogram directory of two runners in one process, so parallel
    // gated tests never share an artifact path when neither supplies an output prefix.
    private readonly string _runToken = Guid.NewGuid().ToString("N")[..8];

    public YcsbRunner(YcsbScenario scenario, YcsbSettings settings)
    {
        _scenario = scenario;
        _settings = settings;
        _containerLocator = new DockerDatabaseContainerLocator();
        _fingerprintSource = new NativeMachineFingerprintSource();
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

        var (loadStep, loadShape) = ResolveLoadStepPlan(_scenario);
        var (workStep, workShape) = ResolveStepPlan(_scenario);

        var context = await BuildContextAsync(url, database, ResolveConcurrencyCeiling(_scenario, url, database));
        using var transport = context.Transport;

        await transport.EnsureDatabaseExistsAsync(database);
        var serverVersion = await transport.GetServerVersionAsync();

        var repositoryRoot = RepositoryRootLocator.Find();
        var databaseContainer = ResolveDatabaseContainer(context.RecordedUrl);
        var machineFingerprint = new MachineFingerprintCollector(_fingerprintSource).Collect(repositoryRoot, databaseContainer);

        RunOptions BaseOptions(WorkloadProfile profile, StepPlan step, LoadShape shape, string runName) => new()
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
            LinkMbps = _settings.LinkMbps,
            LatencyHistogramsDir = HistogramPrefixFor(runName),
            // A ycsb run always exports both the HdrHistogram log and the CSV, so the result
            // names two artifacts that exist for every step and the two runs never collide.
            LatencyHistogramsFormat = HistogramExportFormat.Both
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
            MachineFingerprint = machineFingerprint,
            Ycsb = new YcsbRunInfo
            {
                Run = kind.ToResultName(),
                ResolvedScenario = _scenario,
                ProductName = transport.ProductName,
                ServerVersion = serverVersion,
                Durability = context.Durability,
                ImageReference = databaseContainer?.ImageReference,
                ImageDigest = databaseContainer?.ImageDigest
            }
        };

        var loadOpts = BaseOptions(WorkloadProfile.BulkWrites, loadStep, loadShape, YcsbRunKind.Load.ToResultName()) with { Warmup = TimeSpan.Zero };
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

        foreach (var (kind, mix) in new[]
                 {
                     (YcsbRunKind.WorkloadC, YcsbRunKinds.WorkloadC),
                     (YcsbRunKind.WorkloadA, YcsbRunKinds.WorkloadA),
                     (YcsbRunKind.WorkloadB, YcsbRunKinds.WorkloadB)
                 })
        {
            var opts = BaseOptions(WorkloadProfile.Mixed, workStep, workShape, kind.ToResultName()) with { Preload = _scenario.DocumentCount };
            var workload = new MixedProfileWorkload(mix, distribution, docSizeBytes, seed, initialKeyspace: _scenario.DocumentCount);
            var executor = new BenchmarkExecutor(opts, transport, workload, cpuTracker);
            var ramp = await BenchmarkRunner.RunRampAsync(opts, transport, executor, workload, startupCalibration: null, rng);
            results.Add((kind, Summary(opts, ramp, kind)));
        }

        var insertOpts = BaseOptions(WorkloadProfile.Writes, workStep, workShape, YcsbRunKind.InsertStream.ToResultName());
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
    private async Task<RunContext> BuildContextAsync(string url, string database, int maxConcurrency)
    {
        if (IsRavenTarget(_scenario.Target))
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

        if (string.Equals(_scenario.Target, PostgresYcsbTransport.Target, StringComparison.OrdinalIgnoreCase))
        {
            var transport = new PostgresYcsbTransport(url, database, maxConcurrency);
            return new RunContext(
                transport,
                transport.RecordedEndpoint,
                ClientCompression: "n/a",
                EffectiveHttpVersion: "n/a",
                // PostgreSQL writes at synchronous_commit=on, the parity setting recorded for the target.
                Durability: new DurabilityParity
                {
                    Setting = PostgresYcsbTransport.DurabilitySetting,
                    Value = PostgresYcsbTransport.DurabilityValue
                },
                TransportKind: TransportKind.Raw,
                Compression: CompressionMode.Identity,
                HttpVersion: "auto",
                StrictHttpVersion: false);
        }

        throw new YcsbScenarioException(
            $"Scenario key 'Target' is '{_scenario.Target}'; valid targets are '{RavendbTarget}', '{Ravendb6Target}', '{Ravendb7Target}', '{PostgresYcsbTransport.Target}', '{MongoYcsbTransport.MongoDbTarget}' and '{MongoYcsbTransport.DocumentDbTarget}'.");
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
            // RavenDB writes with its own default durability; the PostgreSQL and Mongo dispatchers
            // record their own setting and value.
            Durability: new DurabilityParity { Setting = "durability", Value = "ravendb-default" },
            TransportKind: transportKind,
            Compression: compression,
            HttpVersion: _settings.HttpVersion,
            StrictHttpVersion: _settings.StrictHttpVersion);
    }

    private static bool IsRavenTarget(string target) =>
        string.Equals(target, RavendbTarget, StringComparison.OrdinalIgnoreCase)
        || string.Equals(target, Ravendb6Target, StringComparison.OrdinalIgnoreCase)
        || string.Equals(target, Ravendb7Target, StringComparison.OrdinalIgnoreCase);

    private static bool IsMongoTarget(string target) =>
        string.Equals(target, MongoYcsbTransport.MongoDbTarget, StringComparison.OrdinalIgnoreCase)
        || string.Equals(target, MongoYcsbTransport.DocumentDbTarget, StringComparison.OrdinalIgnoreCase);

    // The targets that run in a container the benchmark can read: the two containerized RavenDB
    // services, PostgreSQL, MongoDB and DocumentDB. The external RavenDB server is not one of them.
    private static bool IsContainerizedTarget(string target) =>
        IsMongoTarget(target)
        || string.Equals(target, PostgresYcsbTransport.Target, StringComparison.OrdinalIgnoreCase)
        || string.Equals(target, Ravendb6Target, StringComparison.OrdinalIgnoreCase)
        || string.Equals(target, Ravendb7Target, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The container that serves the target, for the five containerized targets, so the result
    /// records the image that actually ran. The external RavenDB target did not run in a container
    /// the benchmark used, so it has none. A client with no usable Docker records none either.
    /// </summary>
    private DatabaseContainerInfo? ResolveDatabaseContainer(string recordedUrl)
    {
        if (IsContainerizedTarget(_scenario.Target) == false)
            return null;

        return _containerLocator.Locate(ResolveHostPort(recordedUrl));
    }

    // The container is matched by the port the caller's endpoint names. Without a port the
    // container that serves the endpoint cannot be identified, so the run fails rather than
    // guessing a default that may belong to another server.
    private int ResolveHostPort(string recordedUrl)
    {
        if (Uri.TryCreate(recordedUrl, UriKind.Absolute, out var uri) && uri.Port > 0)
            return uri.Port;

        throw new YcsbScenarioException(
            $"The '{_scenario.Target}' endpoint does not carry a port, so the container that serves it cannot be identified. Name the port in --url.");
    }

    /// <summary>
    /// The per-run histogram prefix. It carries the run identity so the five runs never share an
    /// artifact path, and it sits next to the result when the caller named an output prefix. With
    /// no output prefix, a unique directory keeps two runs in one process apart.
    /// </summary>
    private string HistogramPrefixFor(string runName)
    {
        if (string.IsNullOrWhiteSpace(_settings.OutputDir) == false)
            return $"{_settings.OutputDir}-{runName}";

        if (string.IsNullOrWhiteSpace(_settings.OutJson) == false)
        {
            var directory = Path.GetDirectoryName(_settings.OutJson) ?? ".";
            var name = Path.GetFileNameWithoutExtension(_settings.OutJson);
            return Path.Combine(directory, $"{name}-{runName}");
        }

        return Path.Combine(Path.GetTempPath(), "raven-bench-ycsb", _runToken, runName);
    }

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

    /// <summary>
    /// The largest concurrency the resolved scenario reaches, which sizes a transport's per-worker
    /// connection set. The closed-loop plan gives it directly. A rate run holds one in-flight
    /// operation per worker, so its ceiling is the rate planner's own estimate at the run's
    /// fallback service time (the ycsb runs carry no measured baseline), and the load run's
    /// closed-loop plan is counted too because it runs first at the scenario's concurrency.
    /// </summary>
    internal static int ResolveConcurrencyCeiling(YcsbScenario scenario, string url, string database)
    {
        var closedEnd = CliParsing.ParseStepPlan(scenario.Concurrency).Normalize().End;
        if (scenario.Rate.HasValue == false)
            return closedEnd;

        var rate = (int)Math.Max(1, Math.Round(scenario.Rate.Value));
        var rateWorkers = RateWorkerPlanner.ResolveRateWorkerCount(
            new RunOptions { Url = url, Database = database }, rate, baselineLatencyMicros: 0);

        return Math.Max(closedEnd, rateWorkers);
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
