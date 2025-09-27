using System.Collections.Generic;
using Lextm.SharpSnmpLib;

namespace RavenBench.Metrics.Snmp;

public static class SnmpMetricMapper
{
    public static (double? machineCpu, double? processCpu, long? managedMemoryMb, long? unmanagedMemoryMb) MapMetrics(Dictionary<string, Variable> values)
    {
        double? machineCpu = null;
        double? processCpu = null;
        long? managedMemoryMb = null;
        long? unmanagedMemoryMb = null;

        if (values.TryGetValue(SnmpOids.MachineCpu, out var mc) && mc.Data is Gauge32 mcG)
            machineCpu = mcG.ToUInt32();

        if (values.TryGetValue(SnmpOids.ProcessCpu, out var pc) && pc.Data is Gauge32 pcG)
            processCpu = pcG.ToUInt32();

        if (values.TryGetValue(SnmpOids.ManagedMemory, out var mm) && mm.Data is Gauge32 mmG)
            managedMemoryMb = mmG.ToUInt32();

        if (values.TryGetValue(SnmpOids.UnmanagedMemory, out var um) && um.Data is Gauge32 umG)
            unmanagedMemoryMb = umG.ToUInt32();

        return (machineCpu, processCpu, managedMemoryMb, unmanagedMemoryMb);
    }
}
