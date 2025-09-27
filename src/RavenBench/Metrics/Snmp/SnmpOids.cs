using System.ComponentModel;

namespace RavenBench.Metrics.Snmp;

public static class SnmpOids
{
    private const string BaseOid = "1.3.6.1.4.1.45751.1.1.1";

    [Description("Process CPU usage in %")]
    public const string ProcessCpu = BaseOid + ".5.1";

    [Description("Machine CPU usage in %")]
    public const string MachineCpu = BaseOid + ".5.2";

    [Description("System-wide allocated memory in MB")]
    public const string TotalMemory = BaseOid + ".6.1";

    [Description("Server managed memory size in MB")]
    public const string ManagedMemory = BaseOid + ".6.7";

    [Description("Server unmanaged memory size in MB")]
    public const string UnmanagedMemory = BaseOid + ".6.8";
}