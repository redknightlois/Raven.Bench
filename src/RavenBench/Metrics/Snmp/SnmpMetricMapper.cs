using System;
using System.Collections.Generic;
using Lextm.SharpSnmpLib;

namespace RavenBench.Metrics.Snmp;

public static class SnmpMetricMapper
{
    /// <summary>
    /// Maps raw SNMP variables to a structured SnmpSample.
    /// </summary>
    public static SnmpSample MapToSample(Dictionary<string, Variable> values)
    {
        return new SnmpSample
        {
            Timestamp = DateTime.UtcNow,
            MachineCpu = ExtractGauge32AsDouble(values, SnmpOids.MachineCpu),
            ProcessCpu = ExtractGauge32AsDouble(values, SnmpOids.ProcessCpu),
            ManagedMemoryMb = ExtractGauge32AsLong(values, SnmpOids.ManagedMemory),
            UnmanagedMemoryMb = ExtractGauge32AsLong(values, SnmpOids.UnmanagedMemory),
            DirtyMemoryMb = ExtractGauge32AsLong(values, SnmpOids.DirtyMemory),
            Load1Min = ExtractOctetStringAsDouble(values, SnmpOids.Load1Min),
            Load5Min = ExtractOctetStringAsDouble(values, SnmpOids.Load5Min),
            Load15Min = ExtractOctetStringAsDouble(values, SnmpOids.Load15Min),
            IoReadOpsPerSec = ExtractGauge32AsDouble(values, SnmpOids.IoReadOps),
            IoWriteOpsPerSec = ExtractGauge32AsDouble(values, SnmpOids.IoWriteOps),
            IoReadKbPerSec = ExtractGauge32AsDouble(values, SnmpOids.IoReadBytes),
            IoWriteKbPerSec = ExtractGauge32AsDouble(values, SnmpOids.IoWriteBytes),
            TotalRequests = ExtractInteger32AsLong(values, SnmpOids.RequestCount),
            RequestsPerSec = ExtractGauge32AsDouble(values, SnmpOids.RequestsPerSecond)
        };
    }

    /// <summary>
    /// Legacy method for backwards compatibility with v0 tuple-based API.
    /// </summary>
    public static (double? machineCpu, double? processCpu, long? managedMemoryMb, long? unmanagedMemoryMb) MapMetrics(Dictionary<string, Variable> values)
    {
        var sample = MapToSample(values);
        return (sample.MachineCpu, sample.ProcessCpu, sample.ManagedMemoryMb, sample.UnmanagedMemoryMb);
    }

    private static double? ExtractGauge32AsDouble(Dictionary<string, Variable> values, string oid)
    {
        if (values.TryGetValue(oid, out var variable))
        {
            return variable.Data switch
            {
                Gauge32 g32 => (double)g32.ToUInt32(),
                Counter32 c32 => (double)c32.ToUInt32(),
                Integer32 i32 => (double)i32.ToInt32(),
                Counter64 c64 => (double)c64.ToUInt64(),
                _ => null
            };
        }
        return null;
    }

    private static long? ExtractGauge32AsLong(Dictionary<string, Variable> values, string oid)
    {
        if (values.TryGetValue(oid, out var variable))
        {
            return variable.Data switch
            {
                Gauge32 g32 => (long)g32.ToUInt32(),
                Counter32 c32 => (long)c32.ToUInt32(),
                Counter64 c64 => (long)c64.ToUInt64(),
                Integer32 i32 => (long)i32.ToInt32(),
                _ => null
            };
        }
        return null;
    }

    private static long? ExtractInteger32AsLong(Dictionary<string, Variable> values, string oid)
    {
        if (values.TryGetValue(oid, out var variable))
        {
            return variable.Data switch
            {
                Integer32 i32 => (long)i32.ToInt32(),
                Gauge32 g32 => (long)g32.ToUInt32(),
                Counter32 c32 => (long)c32.ToUInt32(),
                Counter64 c64 => (long)c64.ToUInt64(),
                _ => null
            };
        }
        return null;
    }

    private static double? ExtractOctetStringAsDouble(Dictionary<string, Variable> values, string oid)
    {
        if (values.TryGetValue(oid, out var variable))
        {
            return variable.Data switch
            {
                OctetString octetString => double.TryParse(octetString.ToString(), out var result) ? result : null,
                Integer32 i32 => (double)i32.ToInt32(),
                _ => null
            };
        } 
        return null;   
    }
}
