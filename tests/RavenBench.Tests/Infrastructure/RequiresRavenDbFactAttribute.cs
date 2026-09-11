using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// Skips the test when a containerized RavenDB endpoint does not answer on the host port. The
/// probe is a short TCP connect; a server that answers but rejects the request is a failing test,
/// not a skipped one.
/// </summary>
public sealed class RequiresRavenDbFactAttribute : FactAttribute
{
    public RequiresRavenDbFactAttribute(int port)
    {
        if (TcpProbe.CanConnect("localhost", port) == false)
            Skip = $"RavenDB is not reachable at localhost:{port}.";
    }
}
