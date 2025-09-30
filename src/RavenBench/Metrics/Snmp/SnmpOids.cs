using System.ComponentModel;
using System.Collections.Generic;
using RavenBench.Util;

namespace RavenBench.Metrics.Snmp;

public static class SnmpOids
{
    private const string BaseOid = "1.3.6.1.4.1.45751.1.1.1";

    // CPU metrics
    [Description("Process CPU usage in %")]
    public const string ProcessCpu = BaseOid + ".5.1";

    [Description("Machine CPU usage in %")]
    public const string MachineCpu = BaseOid + ".5.2";

    // Memory metrics
    [Description("System-wide allocated memory in MB")]
    public const string TotalMemory = BaseOid + ".6.1";

    [Description("Server managed memory size in MB")]
    public const string ManagedMemory = BaseOid + ".6.7";

    [Description("Server unmanaged memory size in MB")]
    public const string UnmanagedMemory = BaseOid + ".6.8";

    [Description("Dirty memory in MB")]
    public const string DirtyMemory = BaseOid + ".6.6";

    // Load averages (Linux only)
    [Description("1-minute load average (Linux only)")]
    public const string Load1Min = BaseOid + ".5.6.1";

    [Description("5-minute load average (Linux only)")]
    public const string Load5Min = BaseOid + ".5.6.2";

    [Description("15-minute load average (Linux only)")]
    public const string Load15Min = BaseOid + ".5.6.3";

    // IO metrics (these are RATES per second, not counters)
    [Description("IO read operations per second")]
    public const string IoReadOps = BaseOid + ".10.5";

    [Description("IO write operations per second")]
    public const string IoWriteOps = BaseOid + ".10.6";

    [Description("Read throughput in kilobytes per second")]
    public const string IoReadBytes = BaseOid + ".10.7";

    [Description("Write throughput in kilobytes per second")]
    public const string IoWriteBytes = BaseOid + ".10.8";

    // Request metrics
    [Description("Total request count since server startup")]
    public const string RequestCount = BaseOid + ".7.2";

    [Description("Requests per second (one minute rate)")]
    public const string RequestsPerSecond = BaseOid + ".7.3";

    /// <summary>
    /// Returns the OID set for the specified SNMP profile.
    /// </summary>
    public static IReadOnlyList<string> GetOidsForProfile(SnmpProfile profile)
    {
        return profile switch
        {
            SnmpProfile.Minimal => new[]
            {
                MachineCpu,
                ProcessCpu,
                ManagedMemory,
                UnmanagedMemory
            },
            SnmpProfile.Extended => new[]
            {
                MachineCpu,
                ProcessCpu,
                ManagedMemory,
                UnmanagedMemory,
                DirtyMemory,
                Load1Min,
                Load5Min,
                Load15Min,
                IoReadOps,
                IoWriteOps,
                IoReadBytes,
                IoWriteBytes,
                RequestCount,
                RequestsPerSecond
            },
            _ => throw new System.ArgumentException($"Unknown SNMP profile: {profile}")
        };
    }
}