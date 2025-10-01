using System.ComponentModel;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class RunSettings : CommandSettings
{
    [CommandOption("--url")]
    [Description("RavenDB server URL (e.g., http://localhost:8080)")]
    public string? Url { get; init; }

    [CommandOption("--database")]
    [Description("Target database name")]
    public string? Database { get; init; }

    // Preferred numeric weights
    [CommandOption("--reads")]
    [Description("Read operation weight (percentage or count)")]
    public string? Reads { get; init; }

    [CommandOption("--writes")]
    [Description("Write operation weight (percentage or count)")]
    public string? Writes { get; init; }

    [CommandOption("--updates")]
    [Description("Update operation weight (percentage or count)")]
    public string? Updates { get; init; }

    [CommandOption("--distribution")]
    [Description("Key distribution: uniform, zipfian (default: uniform)")]
    public string Distribution { get; init; } = "uniform";

    [CommandOption("--doc-size")]
    [Description("Document size (e.g., 1KB, 10KB, 1MB) (default: 1KB)")]
    public string DocSize { get; init; } = "1KB";

    [CommandOption("--transport")]
    [Description("Transport mode: raw, sdk (default: raw)")]
    public string Transport { get; init; } = "raw";

    [CommandOption("--compression")]
    [Description("Compression mode: identity, gzip, brotli (default: identity)")]
    public string Compression { get; init; } = "identity";

    [CommandOption("--mode")]
    [Description("Load mode: closed (default: closed)")]
    public string Mode { get; init; } = "closed";

    [CommandOption("--concurrency")]
    [Description("Concurrency ramp: start..end or start..endxfactor (e.g., 8..512x2) (default: 8..512x2)")]
    public string Concurrency { get; init; } = "8..512x2";

    [CommandOption("--warmup")]
    [Description("Warmup duration (e.g., 20s, 1m) (default: 20s)")]
    public string Warmup { get; init; } = "20s";

    [CommandOption("--duration")]
    [Description("Step duration (e.g., 60s, 2m) (default: 60s)")]
    public string Duration { get; init; } = "60s";

    [CommandOption("--max-errors")]
    [Description("Maximum error rate before stopping (e.g., 0.5%, 100) (default: 0.5%)")]
    public string MaxErrors { get; init; } = "0.5%";

    [CommandOption("--knee-rule")]
    [Description("Knee detection rule: dthr=X%,dp95=Y% (default: dthr=5%,dp95=20%)")]
    public string KneeRule { get; init; } = "dthr=5%,dp95=20%";

    [CommandOption("--out")]
    [Description("Output path for JSON results")]
    public string? OutJson { get; init; }

    [CommandOption("--out-csv")]
    [Description("Output path for CSV results")]
    public string? OutCsv { get; init; }

    [CommandOption("--seed")]
    [Description("Random seed for reproducibility (default: 42)")]
    public int Seed { get; init; } = 42;

    [CommandOption("--preload")]
    [Description("Number of documents to preload before benchmark (default: 0)")]
    public int Preload { get; init; } = 0;

    [CommandOption("--raw-endpoint")]
    [Description("Custom raw HTTP endpoint path")]
    public string? RawEndpoint { get; init; }

    [CommandOption("--tp-workers")]
    [Description("ThreadPool worker threads (default: 8192)")]
    public int? TpWorkers { get; init; } = 8192;

    [CommandOption("--tp-iocp")]
    [Description("ThreadPool IOCP threads (default: 8192)")]
    public int? TpIOCP { get; init; } = 8192;

    [CommandOption("--notes")]
    [Description("Custom notes to include in output")]
    public string? Notes { get; init; }

    [CommandOption("--expected-cores")]
    [Description("Expected CPU core count for validation")]
    public int? ExpectedCores { get; init; }

    [CommandOption("--network-limited")]
    [Description("Enable network-limited mode")]
    public bool NetworkLimitedMode { get; init; }

    [CommandOption("--link-mbps")]
    [Description("Network link speed in Mbps (default: 1000)")]
    public double LinkMbps { get; init; } = 1000;

    [CommandOption("--http-version")]
    [Description("HTTP version: auto, 1.1, 2.0, 3.0 (default: auto)")]
    public string HttpVersion { get; init; } = "auto";

    [CommandOption("--strict-http-version")]
    [Description("Enforce strict HTTP version (fail if unavailable)")]
    public bool StrictHttpVersion { get; init; }

    [CommandOption("--verbose")]
    [Description("Enable verbose output")]
    public bool Verbose { get; init; }

    [CommandOption("--latencies")]
    [Description("Latency display: normalized, raw, both (default: normalized)")]
    public string Latencies { get; init; } = "normalized";

    [CommandOption("--snmp-enabled")]
    [Description("Enable SNMP monitoring (default: true)")]
    public bool SnmpEnabled { get; init; } = true;


    [CommandOption("--snmp-port")]
    [Description("SNMP port (default: 161)")]
    public int SnmpPort { get; init; } = 161;

    [CommandOption("--snmp-interval")]
    [Description("SNMP polling interval (e.g., 250ms, 1s)")]
    public string? SnmpInterval { get; init; }

    [CommandOption("--snmp-profile")]
    [Description("SNMP profile: minimal, extended (default: minimal)")]
    public string SnmpProfile { get; init; } = "minimal";

    [CommandOption("--snmp-timeout")]
    [Description("SNMP timeout (e.g., 5s, 10s) (default: 5s)")]
    public string SnmpTimeout { get; init; } = "5s";
}