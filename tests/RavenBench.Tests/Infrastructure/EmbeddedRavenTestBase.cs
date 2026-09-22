using Raven.Embedded;
using Raven.TestDriver;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// Base class for every test that drives the embedded RavenDB server. The driver's server options
/// are process-wide and may only be set before the first store is created, so they are configured
/// here once for the whole assembly.
/// </summary>
public abstract class EmbeddedRavenTestBase : RavenTestDriver
{
    static EmbeddedRavenTestBase()
    {
        ConfigureServer(new TestServerOptions
        {
            Licensing = new ServerOptions.LicensingOptions
            {
                ThrowOnInvalidOrMissingLicense = false
            }
        });
    }
}
