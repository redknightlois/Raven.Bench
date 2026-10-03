using System.Collections.Concurrent;

namespace RavenBench;

/// <summary>
/// Simple error deduplication for verbose logging to prevent spam.
/// </summary>
internal static class VerboseErrorTracker
{
    private static readonly ConcurrentDictionary<string, int> ErrorCounts = new();

    public static void LogError(string errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return;

        var newCount = ErrorCounts.AddOrUpdate(errorMessage, 1, (_, v) => v + 1);

        if (newCount == 1)
            Console.WriteLine($"[Raven.Bench] Error (first occurrence): {errorMessage}");
    }

    public static void Reset()
    {
        ErrorCounts.Clear();
    }
}
