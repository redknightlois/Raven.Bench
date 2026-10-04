using RavenBench.Analysis;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Core.Ycsb;

namespace RavenBench.Ycsb;

/// <summary>One run of the set an invocation produced: what it was, and what it measured.</summary>
public sealed record YcsbRunResult(YcsbRunIdentity Identity, BenchmarkSummary Summary)
{
    public YcsbRunKind Kind => Identity.Kind;
}

/// <summary>
/// Drives one ycsb scenario end to end: the keyspace is loaded once, then every run the scenario's
/// set names (<see cref="YcsbRunPlan"/>) addresses it through the existing
/// ramp/warmup/latency-recording path (<see cref="BenchmarkRunner.RunRampAsync"/>), each leaving
/// its own result. The scenario's target selects the transport before any product-specific setup
/// runs, so a Mongo target never negotiates HTTP and never reads the RavenDB-only options.
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
    private readonly TransportKind? _transportOverride;
    private readonly Func<YcsbRunIdentity, bool> _include;

    // Distinguishes the default histogram directory of two runners in one process, so parallel
    // gated tests never share an artifact path when neither supplies an output prefix.
    private readonly string _runToken = Guid.NewGuid().ToString("N")[..8];

    public YcsbRunner(YcsbScenario scenario, YcsbSettings settings)
        : this(scenario, settings, transportOverride: null, include: _ => true)
    {
    }

    /// <param name="transportOverride">The mode to run instead of the one <c>--transport</c> names; null keeps the option's mode.</param>
    /// <param name="include">Selects the runs of the scenario's set this invocation runs.</param>
    internal YcsbRunner(YcsbScenario scenario, YcsbSettings settings, TransportKind? transportOverride, Func<YcsbRunIdentity, bool> include)
    {
        _scenario = scenario;
        _settings = settings;
        _transportOverride = transportOverride;
        _include = include;
        _containerLocator = new DockerDatabaseContainerLocator();
        _fingerprintSource = new NativeMachineFingerprintSource();
    }

    public async Task<List<YcsbRunResult>> RunAsync()
    {
        var transportKind = _transportOverride ?? ResolveTransportKind(_scenario.Target, _settings.Transport);
        var plan = YcsbRunPlan.Build(_scenario).Where(_include).ToList();
        var docSizeBytes = CliParsing.ParseSize(_scenario.DocumentSize);
        var warmup = CliParsing.ParseDuration(_scenario.Warmup);
        var duration = CliParsing.ParseDuration(_scenario.Duration);
        var url = RequiredString(_settings.Url, "--url");
        var database = RequiredString(_settings.Database, "--database");
        var closedStep = CliParsing.ParseStepPlan(_scenario.Concurrency).Normalize();

        using var nodeExporter = await NodeExporterClient.ConnectAsync(CliParsing.ParseNodeExporterUrl(_settings.NodeExporterUrl));
        var context = await BuildContextAsync(url, database, transportKind, ResolveConcurrencyCeiling(_scenario, url, database));
        using var transport = context.Transport;
        var payloadKind = context.TransportKind == TransportKind.ClientEntity ? PayloadKind.Entity : PayloadKind.Json;

        await transport.EnsureDatabaseExistsAsync(database);
        var serverVersion = await transport.GetServerVersionAsync();

        var repositoryRoot = RepositoryRootLocator.Find();
        var databaseContainer = ResolveDatabaseContainer(context.RecordedUrl);
        var machineFingerprint = new MachineFingerprintCollector(_fingerprintSource).Collect(repositoryRoot, databaseContainer);

        var cpuTracker = new ProcessCpuTracker();

        // Only a RavenDB target exposes a server metric this harness can read, and the tracker's
        // constructor says so at compile time. It starts and stops with the CPU tracker, so its
        // samples cover the same post-warmup measurement window.
        using var serverTracker = transport is ITransport ravenTransport
            ? new ServerMetricsTracker(ravenTransport, new RunOptions { Url = context.RecordedUrl, Database = database })
            : null;

        // The first id an insert-stream run may claim. It advances past every id the previous
        // insert-stream run issued, so two such runs of one invocation address disjoint ranges and
        // never re-insert an id a product rejects as a duplicate.
        long nextInsertKey = _scenario.DocumentCount;
        var outcomes = new List<RunOutcome>();

        foreach (var identity in plan)
        {
            // Every run draws from its own source, mixed from the scenario seed and the run's
            // identity: two invocations of one scenario repeat a run's stream, two repetitions of
            // one row never repeat each other's, and a run's stream does not depend on how many
            // runs preceded it.
            var runSeed = SeedMixer.Derive(_scenario.Seed, identity.ResultName);
            var distributionKind = CliParsing.ParseDistribution(identity.Distribution);
            var isWorkloadMix = identity.Kind is YcsbRunKind.WorkloadC or YcsbRunKind.WorkloadA or YcsbRunKind.WorkloadB;

            var opts = new RunOptions
            {
                Url = context.RecordedUrl,
                Database = database,
                Transport = context.TransportKind,
                Compression = context.Compression,
                HttpVersion = context.HttpVersion,
                StrictHttpVersion = context.StrictHttpVersion,
                Seed = runSeed,
                DocumentSizeBytes = docSizeBytes,
                Distribution = distributionKind,
                // A bounded fill has no steady state to warm: it ends once the keyspace holds
                // every document.
                Warmup = identity.Kind == YcsbRunKind.Load ? TimeSpan.Zero : warmup,
                Duration = duration,
                Step = StepPlanFor(identity, closedStep),
                Shape = identity.Shape,
                Profile = ProfileFor(identity.Kind),
                Preload = isWorkloadMix ? _scenario.DocumentCount : 0,
                BulkBatchSize = _settings.BulkBatchSize,
                BulkDepth = _settings.BulkDepth,
                MaxErrorRate = CliParsing.ParsePercent(_settings.MaxErrors),
                LinkMbps = _settings.LinkMbps,
                LatencyHistogramsDir = HistogramPrefixFor(ArtifactName(identity)),
                // A ycsb run always exports both the HdrHistogram log and the CSV, so the result
                // names two artifacts that exist for every step and no two runs collide.
                LatencyHistogramsFormat = HistogramExportFormat.Both
            };

            IWorkload workload = identity.Kind switch
            {
                YcsbRunKind.Load => new BulkWriteWorkload(docSizeBytes, opts.BulkBatchSize, runSeed, _scenario.DocumentCount, startingKey: 0, payload: payloadKind),
                YcsbRunKind.InsertStream => new WriteWorkload(docSizeBytes, runSeed, startingKey: nextInsertKey, payload: payloadKind),
                _ => new MixedProfileWorkload(YcsbRunKinds.MixFor(identity.Kind), KeyDistributions.Create(distributionKind), docSizeBytes, runSeed, initialKeyspace: _scenario.DocumentCount, payload: payloadKind)
            };

            var executor = new BenchmarkExecutor(opts, transport, workload, cpuTracker, serverTracker, identity.ResultName, nodeExporter);
            var ramp = await BenchmarkRunner.RunRampAsync(opts, transport, executor, workload, startupCalibration: null, new Random(runSeed));

            OnDiskSize? loadedSize = null;
            if (identity.Kind == YcsbRunKind.Load)
            {
                // Read once, after the load ramp returned and outside any measurement window, so
                // the figure is the size of what the load left behind.
                loadedSize = await ReadOnDiskSizeAsync(transport);
                await GuardKeyspaceAsync(transport);
            }

            if (workload is WriteWorkload insertStream)
                nextInsertKey = insertStream.HighestKeyIssued;

            outcomes.Add(new RunOutcome(identity, opts, ramp, loadedSize));
        }

        // The median rule reads the measured results, so it runs once every repetition of every
        // row has run and before the results are built.
        var medians = YcsbMedianSelector
            .SelectMedians(outcomes, o => o.Identity.RowKey, o => o.Ramp.Steps)
            .Select(o => o.Identity)
            .ToHashSet();

        return outcomes
            .Select(o => new YcsbRunResult(o.Identity, BuildSummary(o, medians.Contains(o.Identity), context, transport, serverVersion, machineFingerprint, databaseContainer)))
            .ToList();
    }

    private sealed record RunOutcome(YcsbRunIdentity Identity, RunOptions Options, BenchmarkRunner.RampResult Ramp, OnDiskSize? LoadedSize);

    /// <summary>The one load fills the keyspace every workload run addresses; no workload run loads its own.</summary>
    private async Task GuardKeyspaceAsync(IYcsbTransport transport)
    {
        var loadedCount = await transport.GetDocumentCountAsync("bench/");
        if (loadedCount < _scenario.DocumentCount)
        {
            throw new InsufficientKeyspaceException(
                $"The keyspace holds {loadedCount} documents but the scenario requires DocumentCount={_scenario.DocumentCount}. " +
                "C, A, B and insert-stream do not run against a short keyspace, and do not load it themselves.");
        }
    }

    private BenchmarkSummary BuildSummary(
        RunOutcome outcome,
        bool isRowMedian,
        RunContext context,
        IYcsbTransport transport,
        string serverVersion,
        MachineFingerprint machineFingerprint,
        DatabaseContainerInfo? databaseContainer)
    {
        var knee = KneeFinder.FindKnee(outcome.Ramp.Steps, outcome.Options.MaxErrorRate);

        return new BenchmarkSummary
        {
            Options = outcome.Options,
            Steps = outcome.Ramp.Steps,
            Knee = knee,
            // The verdict comes from the one attribution every command uses, so a client-bound
            // ycsb run says so.
            Verdict = ResultAnalyzer.BuildVerdict(knee, outcome.Options),
            ClientCompression = context.ClientCompression,
            EffectiveHttpVersion = context.EffectiveHttpVersion,
            TransportPath = (transport as ITransport)?.TransportPath,
            HistogramArtifacts = outcome.Ramp.HistogramArtifacts.Count > 0 ? outcome.Ramp.HistogramArtifacts : null,
            MachineFingerprint = machineFingerprint,
            Ycsb = new YcsbRunInfo
            {
                Run = outcome.Identity.Kind.ToResultName(),
                Shape = outcome.Identity.ShapeName,
                Distribution = outcome.Options.Distribution.ToString().ToLowerInvariant(),
                Rate = outcome.Identity.Rate,
                Repetition = outcome.Identity.Repetition,
                IsRowMedian = isRowMedian,
                MedianStatistic = YcsbMedianSelector.StatisticName,
                ResolvedScenario = _scenario,
                ProductName = transport.ProductName,
                ServerVersion = serverVersion,
                Durability = context.Durability,
                ClientLibrary = context.ClientLibrary,
                ClientLibraryVersion = context.ClientLibraryVersion,
                ImageReference = databaseContainer?.ImageReference,
                ImageDigest = databaseContainer?.ImageDigest,
                LoadedSize = outcome.LoadedSize,
                ServerColumns = ServerColumnAvailability.FromSteps(transport.ProductName, outcome.Ramp.Steps)
            }
        };
    }

    private static WorkloadProfile ProfileFor(YcsbRunKind kind) => kind switch
    {
        YcsbRunKind.Load => WorkloadProfile.BulkWrites,
        YcsbRunKind.InsertStream => WorkloadProfile.Writes,
        _ => WorkloadProfile.Mixed
    };

    /// <summary>
    /// A closed-loop run follows the scenario's own concurrency plan. A fixed-rate run holds no
    /// ramp: its step plan is the rate itself, at a fixed target.
    /// </summary>
    private static StepPlan StepPlanFor(YcsbRunIdentity identity, StepPlan closedStep)
    {
        if (identity.Shape == LoadShape.Closed)
            return closedStep;

        var rate = RateSteps(identity.Rate!.Value);
        return new StepPlan(rate, rate, 2.0);
    }

    private static int RateSteps(double rate) => (int)Math.Max(1, Math.Round(rate));

    /// <summary>
    /// The on-disk size of the loaded set, through the product's own named statistic. A product
    /// whose transport does not carry the optional capability states that fact by name instead.
    /// </summary>
    private static async Task<OnDiskSize> ReadOnDiskSizeAsync(IYcsbTransport transport)
    {
        if (transport is IReportsStorageSize reporter)
            return OnDiskSize.Reported(reporter.StorageSizeMetricName, await reporter.GetStorageSizeBytesAsync());

        return OnDiskSize.NotExposed(transport.ProductName);
    }

    /// <summary>
    /// Selects the transport from the scenario's target, before any RavenDB-only setup, and
    /// carries the facts the result records for the target that actually ran. An unknown target
    /// fails naming the value rather than falling back to a default.
    /// </summary>
    private async Task<RunContext> BuildContextAsync(string url, string database, TransportKind transportKind, int maxConcurrency)
    {
        if (IsRavenTarget(_scenario.Target))
            return await BuildRavenContextAsync(url, database, transportKind);

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
            // raw drives Apex.PgClient; both client modes drive Npgsql, the counterpart of the RavenDB client.
            var viaNpgsql = transportKind != TransportKind.Raw;
            IYcsbTransport transport;
            string recordedEndpoint;
            if (viaNpgsql)
            {
                var npgsql = new NpgsqlYcsbTransport(url, database, maxConcurrency, mapEntities: transportKind == TransportKind.ClientEntity);
                (transport, recordedEndpoint) = (npgsql, npgsql.RecordedEndpoint);
            }
            else
            {
                var apex = new PostgresYcsbTransport(url, database, maxConcurrency);
                (transport, recordedEndpoint) = (apex, apex.RecordedEndpoint);
            }

            return new RunContext(
                transport,
                recordedEndpoint,
                ClientCompression: "n/a",
                EffectiveHttpVersion: "n/a",
                // PostgreSQL writes at synchronous_commit=on, the parity setting recorded for the target.
                Durability: new DurabilityParity
                {
                    Setting = PostgresYcsbTransport.DurabilitySetting,
                    Value = PostgresYcsbTransport.DurabilityValue
                },
                TransportKind: transportKind,
                Compression: CompressionMode.Identity,
                HttpVersion: "auto",
                StrictHttpVersion: false,
                ClientLibrary: viaNpgsql ? NpgsqlYcsbTransport.ClientLibraryName : null,
                ClientLibraryVersion: viaNpgsql ? NpgsqlYcsbTransport.ClientLibraryVersion : null);
        }

        throw new YcsbScenarioException(
            $"Scenario key 'Target' is '{_scenario.Target}'; valid targets are '{RavendbTarget}', '{Ravendb6Target}', '{Ravendb7Target}', '{PostgresYcsbTransport.Target}', '{MongoYcsbTransport.MongoDbTarget}' and '{MongoYcsbTransport.DocumentDbTarget}'.");
    }

    private async Task<RunContext> BuildRavenContextAsync(string url, string database, TransportKind transportKind)
    {
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
    /// The name a run's artifacts carry. A runner whose mode was set by its caller prefixes the mode,
    /// so two modes over one scenario and one output prefix never share a path.
    /// </summary>
    internal string ArtifactName(YcsbRunIdentity identity) => _transportOverride is { } mode
        ? $"{CliParsing.FormatTransport(mode)}-{identity.ResultName}"
        : identity.ResultName;

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
        bool StrictHttpVersion,
        string? ClientLibrary = null,
        string? ClientLibraryVersion = null);

    /// <summary>
    /// The mode <c>--transport</c> selects for a target. RavenDB and PostgreSQL define all three
    /// modes; a Mongo target defines only raw and refuses the others by name, before the run loads,
    /// connects or creates anything.
    /// </summary>
    internal static TransportKind ResolveTransportKind(string target, string transport)
    {
        var kind = CliParsing.ParseTransport(transport);
        if (kind != TransportKind.Raw && IsMongoTarget(target))
            throw new YcsbScenarioException(
                $"Target '{target}' has no '--transport {CliParsing.FormatTransport(kind)}' mode; it runs only '--transport raw'.");
        return kind;
    }

    /// <summary>
    /// The largest concurrency any run of the invocation reaches, which sizes a transport's
    /// per-worker connection set. The closed-loop plan gives it directly. A rate run holds one
    /// in-flight operation per worker, so every rate the scenario names contributes the rate
    /// planner's own estimate at the run's fallback service time (the ycsb runs carry no measured
    /// baseline), and the largest of them all wins.
    /// </summary>
    internal static int ResolveConcurrencyCeiling(YcsbScenario scenario, string url, string database)
    {
        var ceiling = CliParsing.ParseStepPlan(scenario.Concurrency).Normalize().End;

        foreach (var rate in scenario.ResolvedRates)
        {
            var workers = RateWorkerPlanner.ResolveRateWorkerCount(
                new RunOptions { Url = url, Database = database }, RateSteps(rate), baselineLatencyMicros: 0);
            ceiling = Math.Max(ceiling, workers);
        }

        return ceiling;
    }

    private static ITransport BuildRavenDbTransport(TransportKind kind, string url, string database, CompressionMode compression, Version httpVersion) => kind switch
    {
        TransportKind.Raw => new RawHttpTransport(url, database, compression, httpVersion),
        TransportKind.Client => new RavenClientTransport(url, database, compression, httpVersion),
        TransportKind.ClientEntity => new RavenClientTransport(url, database, compression, httpVersion, mapEntities: true),
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };

    private static string RequiredString(string? value, string optionName) =>
        string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"{optionName} is required") : value;
}
