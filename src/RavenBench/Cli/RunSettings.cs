using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RunSettings : CommandSettings
{
    [CommandOption("--url")]
    public string? Url { get; init; }

    [CommandOption("--database")]
    public string? Database { get; init; }

    // Preferred numeric weights
    [CommandOption("--reads")]
    public string? Reads { get; init; }

    [CommandOption("--writes")]
    public string? Writes { get; init; }

    [CommandOption("--updates")]
    public string? Updates { get; init; }

    [CommandOption("--distribution")]
    public string Distribution { get; init; } = "uniform";

    [CommandOption("--doc-size")]
    public string DocSize { get; init; } = "1KB";

    [CommandOption("--compression")]
    public string Compression { get; init; } = "raw:identity";

    [CommandOption("--mode")]
    public string Mode { get; init; } = "closed";

    [CommandOption("--concurrency")]
    public string Concurrency { get; init; } = "8..512x2";

    [CommandOption("--warmup")]
    public string Warmup { get; init; } = "20s";

    [CommandOption("--duration")]
    public string Duration { get; init; } = "60s";

    [CommandOption("--max-errors")]
    public string MaxErrors { get; init; } = "0.5%";

    [CommandOption("--knee-rule")]
    public string KneeRule { get; init; } = "dthr=5%,dp95=20%";

    [CommandOption("--out")]
    public string? OutJson { get; init; }

    [CommandOption("--out-csv")]
    public string? OutCsv { get; init; }

    [CommandOption("--seed")]
    public int Seed { get; init; } = 42;

    [CommandOption("--preload")]
    public int Preload { get; init; } = 0;

    [CommandOption("--raw-endpoint")]
    public string? RawEndpoint { get; init; }

    [CommandOption("--tp-workers")]
    public int? TpWorkers { get; init; } = 8192;

    [CommandOption("--tp-iocp")]
    public int? TpIOCP { get; init; } = 8192;

    [CommandOption("--notes")]
    public string? Notes { get; init; }

    [CommandOption("--expected-cores")]
    public int? ExpectedCores { get; init; }

    [CommandOption("--network-limited")]
    public bool NetworkLimitedMode { get; init; }

    [CommandOption("--link-mbps")]
    public double LinkMbps { get; init; } = 1000;

    [CommandOption("--http-version")]
    public string HttpVersion { get; init; } = "auto";

    [CommandOption("--strict-http-version")]
    public bool StrictHttpVersion { get; init; }

    [CommandOption("--verbose")]
    public bool Verbose { get; init; }

    [CommandOption("--latencies")]
    public string Latencies { get; init; } = "normalized";
}
