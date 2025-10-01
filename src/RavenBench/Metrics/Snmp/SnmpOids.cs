using System.ComponentModel;
using System.Collections.Generic;
using RavenBench.Util;

namespace RavenBench.Metrics.Snmp;

public static class SnmpOids
{
    private const string BaseOid = "1.3.6.1.4.1.45751.1.1.1";
    private const string DatabaseOidPrefix = "1.3.6.1.4.1.45751.1.1.5.2";

    // CPU metrics (server-wide only)
    [Description("Process CPU usage in %")]
    public const string ProcessCpu = BaseOid + ".5.1";

    [Description("Machine CPU usage in %")]
    public const string MachineCpu = BaseOid + ".5.2";

    // Memory metrics (server-wide only)
    [Description("System-wide allocated memory in MB")]
    public const string TotalMemory = BaseOid + ".6.1";

    [Description("Server managed memory size in MB")]
    public const string ManagedMemory = BaseOid + ".6.7";

    [Description("Server unmanaged memory size in MB")]
    public const string UnmanagedMemory = BaseOid + ".6.8";

    [Description("Dirty memory in MB")]
    public const string DirtyMemory = BaseOid + ".6.6";

    // Load averages (Linux only, server-wide)
    [Description("1-minute load average (Linux only)")]
    public const string Load1Min = BaseOid + ".5.6.1";

    [Description("5-minute load average (Linux only)")]
    public const string Load5Min = BaseOid + ".5.6.2";

    [Description("15-minute load average (Linux only)")]
    public const string Load15Min = BaseOid + ".5.6.3";

    // Server-wide IO metrics (for System database)
    [Description("IO read operations per second (server-wide)")]
    public const string ServerIoReadOps = BaseOid + ".10.5";

    [Description("IO write operations per second (server-wide)")]
    public const string ServerIoWriteOps = BaseOid + ".10.6";

    [Description("Read throughput in kilobytes per second (server-wide)")]
    public const string ServerIoReadBytes = BaseOid + ".10.7";

    [Description("Write throughput in kilobytes per second (server-wide)")]
    public const string ServerIoWriteBytes = BaseOid + ".10.8";

    // Server-wide request metrics
    [Description("Total request count since server startup (server-wide)")]
    public const string ServerRequestCount = BaseOid + ".7.2";

    [Description("Requests per second (one minute rate, server-wide)")]
    public const string ServerRequestsPerSecond = BaseOid + ".7.3";

    // Database-specific OID templates (use string.Format with database index)
    private const string DbIoReadOps = DatabaseOidPrefix + ".{0}.2.7";
    private const string DbIoWriteOps = DatabaseOidPrefix + ".{0}.2.8";
    private const string DbIoReadBytes = DatabaseOidPrefix + ".{0}.2.9";
    private const string DbIoWriteBytes = DatabaseOidPrefix + ".{0}.2.10";
    private const string DbRequestCount = DatabaseOidPrefix + ".{0}.3.6";
    private const string DbRequestsPerSecond = DatabaseOidPrefix + ".{0}.3.5";

    /// <summary>
    /// Returns the OID set for the specified SNMP profile.
    /// When databaseIndex is null, returns server-wide OIDs only.
    /// When databaseIndex is provided, returns both server-wide OIDs and database-specific OIDs.
    /// </summary>
    public static IReadOnlyList<string> GetOidsForProfile(SnmpProfile profile, long? databaseIndex = null)
    {
        var oids = new List<string>();

        // Always include server-wide metrics
        switch (profile)
        {
            case SnmpProfile.Minimal:
                oids.AddRange(new[]
                {
                    MachineCpu,
                    ProcessCpu,
                    ManagedMemory,
                    UnmanagedMemory
                });
                break;
            case SnmpProfile.Extended:
                oids.AddRange(new[]
                {
                    MachineCpu,
                    ProcessCpu,
                    ManagedMemory,
                    UnmanagedMemory,
                    DirtyMemory,
                    Load1Min,
                    Load5Min,
                    Load15Min
                });
                break;
            default:
                throw new System.ArgumentException($"Unknown SNMP profile: {profile}");
        }

        // Add database-specific OIDs if database index is provided
        // Note: IO metrics are ONLY meaningful per-database, not server-wide
        if (databaseIndex.HasValue && profile == SnmpProfile.Extended)
        {
            oids.AddRange(new[]
            {
                string.Format(DbIoReadOps, databaseIndex.Value),
                string.Format(DbIoWriteOps, databaseIndex.Value),
                string.Format(DbIoReadBytes, databaseIndex.Value),
                string.Format(DbIoWriteBytes, databaseIndex.Value),
                string.Format(DbRequestCount, databaseIndex.Value),
                string.Format(DbRequestsPerSecond, databaseIndex.Value)
            });
        }
        // If no database index is provided for Extended profile, skip IO/request metrics
        // (they're only meaningful in the context of a specific database)

        return oids;
    }

    /// <summary>
    /// Attempts to parse the database index from a database-specific SNMP OID.
    /// OID format: 1.3.6.1.4.1.45751.1.1.5.2.{index}.X.Y
    /// The database index is at position 11 when split by '.'.
    /// </summary>
    /// <param name="oid">The SNMP OID string from the database's @General section</param>
    /// <param name="databaseIndex">When this method returns, contains the database index if parsing succeeded, or 0 if parsing failed</param>
    /// <returns>true if the database index was successfully parsed; otherwise, false</returns>
    public static bool TryParseDatabaseIndexFromOid(string? oid, out long databaseIndex)
    {
        databaseIndex = 0;

        if (string.IsNullOrEmpty(oid))
            return false;

        var parts = oid.Split('.');

        // OID format: 1.3.6.1.4.1.45751.1.1.5.2.{index}.X.Y
        // Positions:   0 1 2 3 4 5  6    7 8 9 10  11   12 13
        // The database index is at position 11
        if (parts.Length >= 12 && long.TryParse(parts[11], out databaseIndex))
        {
            return true;
        }

        return false;
    }
}