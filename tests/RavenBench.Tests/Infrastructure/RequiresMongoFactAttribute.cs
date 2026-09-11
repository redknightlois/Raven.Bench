using System;
using System.Net.Sockets;
using Xunit;

namespace RavenBench.Tests.Infrastructure;

/// <summary>
/// The MongoDB-wire endpoints a gated test needs and the connection strings it uses. Kept in one
/// place so the probe and the test body address the same host and port.
/// </summary>
internal static class MongoTestEndpoints
{
    public const string MongoHost = "localhost";
    public const int MongoPort = 27017;
    public const string DocumentDbHost = "localhost";
    public const int DocumentDbPort = 10260;

    public const string MongoConnectionString = "mongodb://localhost:27017";

    public const string DocumentDbConnectionString =
        "mongodb://bench:bench@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true";
}

/// <summary>
/// A short TCP connect is the probe. The container either answers or it does not; a container that
/// answers but rejects TLS or authentication is a failing test, not a skipped one.
/// </summary>
internal static class TcpProbe
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(2);

    public static bool CanConnect(string host, int port)
    {
        try
        {
            using var client = new TcpClient();
            return client.ConnectAsync(host, port).Wait(Timeout) && client.Connected;
        }
        catch (Exception)
        {
            return false;
        }
    }
}

internal static class MongoAvailability
{
    private static readonly Lazy<bool> Cached = new(() => TcpProbe.CanConnect(MongoTestEndpoints.MongoHost, MongoTestEndpoints.MongoPort));

    public static bool IsAvailable => Cached.Value;
}

internal static class DocumentDbAvailability
{
    private static readonly Lazy<bool> Cached = new(() => TcpProbe.CanConnect(MongoTestEndpoints.DocumentDbHost, MongoTestEndpoints.DocumentDbPort));

    public static bool IsAvailable => Cached.Value;
}

/// <summary>Skips the test when MongoDB Community does not answer on its port.</summary>
public sealed class RequiresMongoFactAttribute : FactAttribute
{
    public RequiresMongoFactAttribute()
    {
        if (MongoAvailability.IsAvailable == false)
            Skip = $"MongoDB is not reachable at {MongoTestEndpoints.MongoHost}:{MongoTestEndpoints.MongoPort}.";
    }
}

/// <summary>Skips the test when DocumentDB does not answer on its port.</summary>
public sealed class RequiresDocumentDbFactAttribute : FactAttribute
{
    public RequiresDocumentDbFactAttribute()
    {
        if (DocumentDbAvailability.IsAvailable == false)
            Skip = $"DocumentDB is not reachable at {MongoTestEndpoints.DocumentDbHost}:{MongoTestEndpoints.DocumentDbPort}.";
    }
}

/// <summary>Skips the test unless both MongoDB and DocumentDB answer on their ports.</summary>
public sealed class RequiresMongoAndDocumentDbFactAttribute : FactAttribute
{
    public RequiresMongoAndDocumentDbFactAttribute()
    {
        if (MongoAvailability.IsAvailable == false || DocumentDbAvailability.IsAvailable == false)
            Skip = "Both MongoDB and DocumentDB are required for this test.";
    }
}

/// <summary>Skips the test unless MongoDB, DocumentDB and PostgreSQL all answer on their ports.</summary>
public sealed class RequiresMongoDocumentDbAndPostgreSqlFactAttribute : FactAttribute
{
    public RequiresMongoDocumentDbAndPostgreSqlFactAttribute()
    {
        if (MongoAvailability.IsAvailable == false || DocumentDbAvailability.IsAvailable == false || PostgreSqlAvailability.IsAvailable == false)
            Skip = "MongoDB, DocumentDB and PostgreSQL are all required for this test.";
    }
}
