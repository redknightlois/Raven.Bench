namespace RavenBench.Core;

/// <summary>
/// SNMP profile determines which metrics to collect during benchmarking.
/// </summary>
public enum SnmpProfile
{
    /// <summary>
    /// Minimal profile - only essential metrics (CPU, memory).
    /// </summary>
    Minimal,

    /// <summary>
    /// Extended profile - comprehensive metrics including IO, load averages, and request counters.
    /// </summary>
    Extended
}

/// <summary>
/// Configuration options for SNMP telemetry collection.
/// </summary>
public sealed class SnmpOptions
{
    public bool Enabled { get; init; }
    public int Port { get; init; } = 161;
    public TimeSpan PollInterval { get; init; } = TimeSpan.FromMilliseconds(250);
    public SnmpProfile Profile { get; init; } = SnmpProfile.Minimal;
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(5);

    internal const string Community = "ravendb";

    public static SnmpOptions Disabled => new() { Enabled = false };
}