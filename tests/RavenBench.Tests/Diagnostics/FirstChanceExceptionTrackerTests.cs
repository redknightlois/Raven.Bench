using System;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Core.Diagnostics;
using Xunit;

namespace RavenBench.Tests.Diagnostics;

// The tracker counts into one process-wide active step, so no other step may begin during these tests.
[CollectionDefinition(nameof(FirstChanceExceptionTrackerTests), DisableParallelization = true)]
[Collection(nameof(FirstChanceExceptionTrackerTests))]
[Trait("Category", "Unit")]
public class FirstChanceExceptionTrackerTests
{
    [Fact]
    public async Task Suppressed_Scope_Is_Not_Counted_Across_Awaits()
    {
        using var tracker = FirstChanceExceptionTracker.BeginStep();
        Throw(new FormatException("step"));

        using (FirstChanceExceptionTracker.Suppress())
        {
            await Task.Yield();
            Throw(new TimeoutException("poll"));
        }

        var snapshot = tracker.Take();
        snapshot.ByType.Should().Contain((typeof(FormatException).FullName!, 1L));
        snapshot.ByType.Should().NotContain(t => t.Type == typeof(TimeoutException).FullName);
    }

    private static void Throw(Exception exception)
    {
        try { throw exception; }
        catch (Exception e) when (ReferenceEquals(e, exception)) { }
    }
}
