using System.ComponentModel;
using System.Net;
using RavenBench.Core;
using RavenBench.Core.Transport;
using RavenBench.Core.Ycsb;
using RavenBench.Ycsb;
using Spectre.Console;
using Spectre.Console.Cli;

namespace RavenBench.Cli;

public sealed class ParitySettings : CommandSettings
{
    [CommandOption("--ravendb-url")]
    [Description("The RavenDB endpoint, the reference product of the check.")]
    public string? RavenDbUrl { get; init; }

    [CommandOption("--postgresql-url")]
    [Description("The PostgreSQL connection string.")]
    public string? PostgreSqlUrl { get; init; }

    [CommandOption("--mongodb-url")]
    [Description("The MongoDB connection string.")]
    public string? MongoDbUrl { get; init; }

    [CommandOption("--documentdb-url")]
    [Description("The DocumentDB connection string.")]
    public string? DocumentDbUrl { get; init; }

    [CommandOption("--database")]
    [Description("The database the check writes its sample into on every product. Use a throwaway database, not one a measured run loaded.")]
    public string? Database { get; init; }

    [CommandOption("--postgresql-database")]
    [Description("PostgreSQL creates no database on demand, so the check uses this existing one and deletes its sample again. Defaults to --database.")]
    public string? PostgreSqlDatabase { get; init; }

    [CommandOption("--sample")]
    [Description("How many documents the check compares. Defaults to 1,000.")]
    public int Sample { get; init; } = YcsbParityCheck.DefaultSampleSize;

    [CommandOption("--seed")]
    [Description("The seed the sample documents are generated from.")]
    public int Seed { get; init; } = 42;

    [CommandOption("--doc-size")]
    [Description("The requested size of each sample document.")]
    public string DocumentSize { get; init; } = "1KB";
}

/// <summary>
/// Runs the parity check on demand: over a sample of seeded documents, every typed operation must
/// leave the same state on RavenDB, PostgreSQL through each transport mode, MongoDB and DocumentDB. Full agreement exits zero;
/// any disagreement, or any product the check could not reach, exits non-zero.
/// </summary>
public sealed class ParityCommand : AsyncCommand<ParitySettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, ParitySettings settings)
    {
        var documentSize = CliParsing.ParseSize(settings.DocumentSize);
        var database = Required(settings.Database, "--database");

        // The first product is the reference, so RavenDB over raw HTTP comes first.
        using var ravendb = new RawHttpTransport(Required(settings.RavenDbUrl, "--ravendb-url"), database, CompressionMode.Identity, HttpVersion.Version11);
        var postgreSqlDatabase = string.IsNullOrWhiteSpace(settings.PostgreSqlDatabase) ? database : settings.PostgreSqlDatabase;
        var postgreSqlUrl = Required(settings.PostgreSqlUrl, "--postgresql-url");
        using var postgresql = new PostgresYcsbTransport(postgreSqlUrl, postgreSqlDatabase, maxConcurrency: 1);
        using var npgsql = new NpgsqlYcsbTransport(postgreSqlUrl, postgreSqlDatabase, maxConcurrency: 1, mapEntities: false);
        using var npgsqlEntity = new NpgsqlYcsbTransport(postgreSqlUrl, postgreSqlDatabase, maxConcurrency: 1, mapEntities: true);
        using var mongodb = new MongoYcsbTransport(Required(settings.MongoDbUrl, "--mongodb-url"), database, MongoYcsbTransport.MongoDbTarget);
        using var documentdb = new MongoYcsbTransport(Required(settings.DocumentDbUrl, "--documentdb-url"), database, MongoYcsbTransport.DocumentDbTarget);

        // Named by the target that selects it, not by the server, so a product the check cannot
        // reach is still named in the report. PostgreSQL appears once per transport mode.
        var products = new[]
        {
            new YcsbParityProduct(YcsbRunner.RavendbTarget, ravendb.RecordedEndpoint, database, ravendb),
            new YcsbParityProduct(PostgresYcsbTransport.Target, postgresql.RecordedEndpoint, postgreSqlDatabase, postgresql),
            new YcsbParityProduct(PostgreSqlModeName(TransportKind.Client), npgsql.RecordedEndpoint, postgreSqlDatabase, npgsql),
            new YcsbParityProduct(PostgreSqlModeName(TransportKind.ClientEntity), npgsqlEntity.RecordedEndpoint, postgreSqlDatabase, npgsqlEntity),
            new YcsbParityProduct(MongoYcsbTransport.MongoDbTarget, mongodb.RecordedEndpoint, database, mongodb),
            new YcsbParityProduct(MongoYcsbTransport.DocumentDbTarget, documentdb.RecordedEndpoint, database, documentdb)
        };

        var report = await new YcsbParityCheck(settings.Seed, documentSize, settings.Sample).RunAsync(products, CancellationToken.None);
        Print(report);

        return report.ExitCode;
    }

    /// <summary>Prints every pair, one row per operation and product, agreements included.</summary>
    internal static void Print(YcsbParityReport report)
    {
        AnsiConsole.MarkupLine($"Parity over a {report.SampleSize}-document sample against {report.Products.Count} products, reference [cyan]{report.ReferenceProduct}[/]: {string.Join(", ", report.Products)}");

        var table = new Table();
        table.AddColumn("Operation");
        table.AddColumn("Product");
        table.AddColumn("Compared");
        table.AddColumn("Result");

        foreach (var pair in report.Pairs)
        {
            var result = pair.Failure is not null
                ? $"[red]unreachable[/] {Markup.Escape(pair.Failure)}"
                : pair.Agreed
                    ? "[green]agreed[/]"
                    : $"[red]{pair.Mismatches} differ[/] {Markup.Escape(pair.FirstDifference ?? string.Empty)}";

            table.AddRow(pair.Operation, Markup.Escape(pair.Product), pair.Compared.ToString(), result);
        }

        AnsiConsole.Write(table);

        foreach (var statement in report.LeftBehind)
            AnsiConsole.MarkupLine($"Left behind: {Markup.Escape(statement)}");
    }

    /// <summary>The report name of PostgreSQL through one Npgsql mode, such as <c>postgresql/client</c>.</summary>
    internal static string PostgreSqlModeName(TransportKind kind) => $"{PostgresYcsbTransport.Target}/{CliParsing.FormatTransport(kind)}";

    private static string Required(string? value, string optionName) =>
        string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"{optionName} is required") : value;
}
