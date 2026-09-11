using System;
using System.Threading;
using System.Threading.Tasks;
using Apex.PgClient;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// The PostgreSQL endpoint a gated test needs and the connection string it uses. Kept in one place
/// so the probe and the test body address the same host and port.
/// </summary>
internal static class PostgreSqlTestEndpoints
{
    public const string Host = "localhost";
    public const int Port = 5432;
    public const string Database = "bench";

    public const string ConnectionString = "postgresql://bench:bench@localhost:5432/bench";
}

internal static class PostgreSqlAvailability
{
    private static readonly Lazy<bool> Cached = new(() => TcpProbe.CanConnect(PostgreSqlTestEndpoints.Host, PostgreSqlTestEndpoints.Port));

    public static bool IsAvailable => Cached.Value;
}

/// <summary>
/// A short TCP connect is the probe. The container either answers or it does not; a container that
/// answers but rejects authentication or a command is a failing test, not a skipped one.
/// </summary>
public sealed class RequiresPostgreSqlFactAttribute : FactAttribute
{
    public RequiresPostgreSqlFactAttribute()
    {
        if (PostgreSqlAvailability.IsAvailable == false)
            Skip = $"PostgreSQL is not reachable at {PostgreSqlTestEndpoints.Host}:{PostgreSqlTestEndpoints.Port}.";
    }
}

/// <summary>
/// A throwaway schema so parallel test classes do not contend for the fixed <c>ycsb</c> table in
/// the shared <c>bench</c> database. The test address string sets <c>search_path</c> to the schema;
/// dropping the schema removes everything the test created.
/// </summary>
internal sealed class PgTestSchema : IAsyncDisposable
{
    private readonly PgConnection _admin;

    private PgTestSchema(PgConnection admin, string name)
    {
        _admin = admin;
        Name = name;
    }

    public string Name { get; }

    public string ConnectionString => PostgreSqlTestEndpoints.ConnectionString + "?search_path=" + Name;

    public static async Task<PgTestSchema> CreateAsync()
    {
        var admin = await PgClient.ConnectAsync(PostgreSqlTestEndpoints.ConnectionString, CancellationToken.None);
        var name = "ycsb_pg_" + Guid.NewGuid().ToString("N");
        await admin.ExecuteAsync($"CREATE SCHEMA \"{name}\"", CancellationToken.None);
        return new PgTestSchema(admin, name);
    }

    public async ValueTask DisposeAsync()
    {
        await _admin.ExecuteAsync($"DROP SCHEMA IF EXISTS \"{Name}\" CASCADE", CancellationToken.None);
        await _admin.DisposeAsync();
    }
}
