using System;
using System.Collections.Generic;
using Lextm.SharpSnmpLib;

namespace RavenBench.Core.Metrics.Snmp;

public static class SnmpMetricMapper
{
    /// <summary>
    /// Maps raw SNMP variables to a structured SnmpSample.
    /// Automatically detects whether values contain server-wide or database-specific OIDs.
    /// </summary>
    public static SnmpSample MapToSample(Dictionary<string, Variable> values)
    {
        // Try to extract IO/request metrics from any OID that matches the pattern
        // This works for both server-wide and database-specific OIDs
        var ioReadOps = ExtractByPattern(values, ".10.5") ?? ExtractByPattern(values, ".2.7");
        var ioWriteOps = ExtractByPattern(values, ".10.6") ?? ExtractByPattern(values, ".2.8");
        var ioReadBytes = ExtractByPattern(values, ".10.7") ?? ExtractByPattern(values, ".2.9");
        var ioWriteBytes = ExtractByPattern(values, ".10.8") ?? ExtractByPattern(values, ".2.10");
        var totalRequests = ExtractByPatternAsLong(values, ".7.2") ?? ExtractByPatternAsLong(values, ".3.6");
        var requestsPerSec = ExtractByPattern(values, ".7.3") ?? ExtractByPattern(values, ".3.5");

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
            IoReadOpsPerSec = ioReadOps,
            IoWriteOpsPerSec = ioWriteOps,
            IoReadKbPerSec = ioReadBytes,
            IoWriteKbPerSec = ioWriteBytes,
            TotalRequests = totalRequests,
            RequestsPerSec = requestsPerSec
        };
    }

    /// <summary>
    /// Extracts a value from any OID ending with the given pattern.
    /// </summary>
    private static double? ExtractByPattern(Dictionary<string, Variable> values, string oidPattern)
    {
        foreach (var kvp in values)
        {
            if (kvp.Key.EndsWith(oidPattern))
            {
                return kvp.Value.Data switch
                {
                    Gauge32 g32 => (double)g32.ToUInt32(),
                    Counter32 c32 => (double)c32.ToUInt32(),
                    Integer32 i32 => (double)i32.ToInt32(),
                    Counter64 c64 => (double)c64.ToUInt64(),
                    _ => null
                };
            }
        }
        return null;
    }

    /// <summary>
    /// Extracts a long value from any OID ending with the given pattern.
    /// </summary>
    private static long? ExtractByPatternAsLong(Dictionary<string, Variable> values, string oidPattern)
    {
        foreach (var kvp in values)
        {
            if (kvp.Key.EndsWith(oidPattern))
            {
                return kvp.Value.Data switch
                {
                    Integer32 i32 => (long)i32.ToInt32(),
                    Gauge32 g32 => (long)g32.ToUInt32(),
                    Counter32 c32 => (long)c32.ToUInt32(),
                    Counter64 c64 => (long)c64.ToUInt64(),
                    _ => null
                };
            }
        }
        return null;
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
