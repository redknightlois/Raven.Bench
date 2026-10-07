using System;
using System.IO;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench;
using Xunit;

namespace RavenBench.Tests;

[Trait("Category", "Unit")]
public class RunCleanupTests
{
    private sealed class RunFailed : Exception;

    private sealed class CleanupFailed() : Exception("cleanup exploded");

    private static async Task<string> CaptureStderr(Func<Task> act)
    {
        var original = Console.Error;
        var writer = new StringWriter();
        Console.SetError(writer);
        try
        {
            await act();
        }
        finally
        {
            Console.SetError(original);
        }
        return writer.ToString();
    }

    [Fact]
    public async Task A_Cleanup_Failure_After_A_Successful_Run_Is_Logged_And_The_Result_Is_Returned()
    {
        var result = 0;
        var log = await CaptureStderr(async () =>
            result = await RunCleanup.AfterAsync(() => throw new CleanupFailed(), "Vector", () => Task.FromResult(7)));

        result.Should().Be(7);
        log.Should().Contain("[Vector]").And.Contain("cleanup exploded");
    }

    [Fact]
    public async Task A_Cleanup_Failure_After_A_Failed_Run_Is_Logged_And_The_Run_Failure_Surfaces()
    {
        var log = await CaptureStderr(async () =>
        {
            var act = () => RunCleanup.AfterAsync<int>(() => throw new CleanupFailed(), "Aggregate", () => throw new RunFailed());
            await act.Should().ThrowAsync<RunFailed>();
        });

        log.Should().Contain("[Aggregate]").And.Contain("cleanup exploded");
    }

    [Fact]
    public async Task A_Successful_Cleanup_Logs_Nothing()
    {
        var log = await CaptureStderr(async () =>
            (await RunCleanup.AfterAsync(() => Task.CompletedTask, "Vector", () => Task.FromResult(7))).Should().Be(7));

        log.Should().NotContain("cleanup after");
    }
}
