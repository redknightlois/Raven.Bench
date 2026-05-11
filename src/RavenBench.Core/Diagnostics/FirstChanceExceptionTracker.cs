using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;

namespace RavenBench.Core.Diagnostics;

// Counts first-chance exceptions thrown anywhere in the process while enabled.
// First-chance fires before any catch runs, so it surfaces exceptions that frameworks
// (HttpClient, System.Text.Json, ...) catch internally and would otherwise be invisible.
//
// Per-step usage:
//   using (var t = FirstChanceExceptionTracker.BeginStep()) { ... measurement ... }
//   var snap = t.Snapshot;  // counts by type, first-stack sample
public sealed class FirstChanceExceptionTracker : IDisposable
{
    private static readonly object _gate = new();
    private static FirstChanceExceptionTracker? _active;
    private static bool _hooked;

    private long _total;
    private readonly ConcurrentDictionary<string, long> _byType = new();
    private string? _firstSampleType;
    private string? _firstSampleMessage;
    private string? _firstSampleStack;
    private int _firstCaptured;

    private FirstChanceExceptionTracker() { }

    public static FirstChanceExceptionTracker BeginStep()
    {
        lock (_gate)
        {
            var t = new FirstChanceExceptionTracker();
            _active = t;
            if (_hooked == false)
            {
                AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
                _hooked = true;
            }
            return t;
        }
    }

    private static void OnFirstChance(object? sender, FirstChanceExceptionEventArgs e)
    {
        var t = Volatile.Read(ref _active);
        if (t == null) return;
        Interlocked.Increment(ref t._total);
        var typeName = e.Exception.GetType().FullName ?? "Unknown";
        t._byType.AddOrUpdate(typeName, 1, (_, v) => v + 1);
        if (Interlocked.CompareExchange(ref t._firstCaptured, 1, 0) == 0)
        {
            t._firstSampleType = typeName;
            t._firstSampleMessage = e.Exception.Message;
            // Capture the throwing stack at the moment it was thrown.
            t._firstSampleStack = e.Exception.StackTrace ?? new System.Diagnostics.StackTrace(true).ToString();
        }
    }

    public Snapshot Take() => new(
        Volatile.Read(ref _total),
        _byType.ToArray()
            .OrderByDescending(kv => kv.Value)
            .Select(kv => (kv.Key, kv.Value))
            .ToArray(),
        _firstSampleType,
        _firstSampleMessage,
        _firstSampleStack);

    public void Dispose()
    {
        lock (_gate)
        {
            if (ReferenceEquals(_active, this)) _active = null;
        }
    }

    public readonly record struct Snapshot(
        long Total,
        (string Type, long Count)[] ByType,
        string? FirstType,
        string? FirstMessage,
        string? FirstStack);
}
