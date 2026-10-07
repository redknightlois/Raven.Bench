namespace RavenBench;

/// <summary>
/// Runs a step and then removes what the run created. A step failure still runs the cleanup and reaches the caller;
/// a cleanup failure is reported on the error stream, never in place of the step failure and never in place of a successful result.
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
            await CleanAsync(cleanup, component, "a failed run");
            throw;
        }
        await CleanAsync(cleanup, component, "a successful run");
        return result;
    }

    private static async Task CleanAsync(Func<Task>? cleanup, string component, string outcome)
    {
        if (cleanup is null)
            return;

        try
        {
            await cleanup();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{component}] cleanup after {outcome} also failed: {ex.GetType().Name}: {ex.Message}");
        }
    }
}
