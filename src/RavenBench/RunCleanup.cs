namespace RavenBench;

/// <summary>
/// Runs a step and then removes what the run created. A step failure still runs the cleanup and reaches the caller;
/// a cleanup failure after a step failure is reported on the error stream, never in its place.
/// </summary>
internal static class RunCleanup
{
    /// <param name="cleanup">Null when the data is kept, which skips the cleanup.</param>
    /// <param name="component">The prefix of the reported cleanup failure, for example "Vector".</param>
    internal static async Task<T> AfterAsync<T>(Func<Task>? cleanup, string component, Func<Task<T>> run)
    {
        T result;
        try
        {
            result = await run();
        }
        catch
        {
            if (cleanup is not null)
            {
                try
                {
                    await cleanup();
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"[{component}] cleanup after a failed run also failed: {ex.GetType().Name}: {ex.Message}");
                }
            }
            throw;
        }
        if (cleanup is not null)
            await cleanup();
        return result;
    }
}
