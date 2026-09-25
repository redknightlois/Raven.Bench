using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// Serializes the tests that load data into a shared live server or run the benchmark scripts; parallel loads starve one another.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LiveServers
{
    public const string Name = "live-servers";
}
