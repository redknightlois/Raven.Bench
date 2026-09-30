using System;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// The Elasticsearch endpoints the gated tests use: one cluster under the trial licence and one under
/// the basic licence. Each is overridable, so a host can run the containers on free ports.
/// </summary>
internal static class ElasticsearchTestEndpoints
{
    public static Uri Trial { get; } = new(Environment.GetEnvironmentVariable("ELASTICSEARCH_TEST_URL") ?? "http://localhost:9200");
    public static Uri Basic { get; } = new(Environment.GetEnvironmentVariable("ELASTICSEARCH_BASIC_TEST_URL") ?? "http://localhost:9201");
}

/// <summary>
/// Skips the test when the Elasticsearch endpoint for the licence does not answer a TCP connect. A
/// cluster that answers but runs another licence is a failing test, not a skipped one.
/// </summary>
public sealed class RequiresElasticsearchFactAttribute : FactAttribute
{
    public RequiresElasticsearchFactAttribute(string licence)
    {
        var endpoint = licence == "basic" ? ElasticsearchTestEndpoints.Basic : ElasticsearchTestEndpoints.Trial;
        if (TcpProbe.CanConnect(endpoint.Host, endpoint.Port) == false)
            Skip = $"Elasticsearch ({licence} licence) is not reachable at {endpoint}.";
    }
}
