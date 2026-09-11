using System;
using System.Collections.Generic;
using System.Threading;
using FluentAssertions;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using RavenBench.Tests.Infrastructure;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>
/// Pins the typed-operation-to-SQL translation without a live server: the read filters on id, the
/// insert carries the id and a jsonb value, the update is a one-field <c>jsonb_set</c> (never a
/// whole-document assignment), the load is a binary COPY, and the connection-string redaction
/// keeps the user, host, port and options while removing the password.
/// </summary>
public class PostgresYcsbTransportMappingTests
{
    [Fact]
    public void Read_Statement_Filters_On_The_Id_Parameter()
    {
        PostgresYcsbTransport.ReadSql.Should().Contain("FROM ycsb").And.Contain("WHERE id = $1");
        PostgresYcsbTransport.ReadSql.Should().Contain("doc::text", "the pinned driver cannot read jsonb through Get<string> without the cast");
    }

    [Fact]
    public void Insert_Statement_Carries_The_Id_And_A_Jsonb_Value()
    {
        PostgresYcsbTransport.InsertSql.Should().Contain("(id, doc)").And.Contain("$1").And.Contain("$2::jsonb");
    }

    [Fact]
    public void Update_Statement_Is_A_One_Field_Jsonb_Set_Not_A_Whole_Document_Assignment()
    {
        PostgresYcsbTransport.UpdateSql.Should().Contain("jsonb_set(doc, ARRAY[$2], to_jsonb($3::text))");
        PostgresYcsbTransport.UpdateSql.Should().Contain("WHERE id = $1");
        PostgresYcsbTransport.UpdateSql.Should().NotContain("SET doc = $2", "a whole-document replace would drift the other nine fields");
    }

    [Fact]
    public void Bulk_Statement_Is_A_Binary_Copy()
    {
        PostgresYcsbTransport.BulkCopySql.Should().Contain("COPY ycsb (id, doc)").And.Contain("FORMAT BINARY");
    }

    [Fact]
    public void The_Durability_Statement_Turns_Synchronous_Commit_On()
    {
        PostgresYcsbTransport.SetDurabilitySql.Should().Be("SET synchronous_commit = on");
    }

    [Fact]
    public void An_Unsupported_Operation_Throws_A_Specific_Exception()
    {
        using var transport = new PostgresYcsbTransport(PostgreSqlTestEndpoints.ConnectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 1);
        var query = new QueryOperation { QueryText = "from @all_docs", Parameters = new Dictionary<string, object?>() };

        Action act = () => transport.ExecuteAsync(query, CancellationToken.None);

        act.Should().Throw<NotSupportedException>().WithMessage("*QueryOperation*");
    }

    [Fact]
    public void Wire_Bytes_Are_Not_Reported()
    {
        using var transport = new PostgresYcsbTransport(PostgreSqlTestEndpoints.ConnectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 1);

        transport.ReportsWireBytes.Should().BeFalse();
    }

    [Fact]
    public void The_Connection_Set_Is_Sized_To_The_Requested_Concurrency()
    {
        using var transport = new PostgresYcsbTransport(PostgreSqlTestEndpoints.ConnectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 7);

        transport.MeasuredConnectionCount.Should().Be(7);
    }

    [Fact]
    public void A_Missing_Connection_String_Is_Rejected()
    {
        Action act = () => new PostgresYcsbTransport(" ", PostgreSqlTestEndpoints.Database, maxConcurrency: 1);

        act.Should().Throw<ArgumentException>().WithMessage("*connection string*");
    }

    [Fact]
    public void A_Missing_Database_Name_Is_Rejected()
    {
        Action act = () => new PostgresYcsbTransport(PostgreSqlTestEndpoints.ConnectionString, " ", maxConcurrency: 1);

        act.Should().Throw<ArgumentException>().WithMessage("*database name*");
    }

    [Fact]
    public void A_Non_Positive_Concurrency_Is_Rejected()
    {
        Action act = () => new PostgresYcsbTransport(PostgreSqlTestEndpoints.ConnectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 0);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Redaction_Removes_The_Password_And_Keeps_The_User_Host_Port_Database_And_Options()
    {
        using var transport = new PostgresYcsbTransport(
            PostgreSqlTestEndpoints.ConnectionString + "?search_path=ycsb_test",
            PostgreSqlTestEndpoints.Database,
            maxConcurrency: 1);

        transport.RecordedEndpoint.Should().Be("postgresql://bench:***@localhost:5432/bench?search_path=ycsb_test");
        transport.RecordedEndpoint.Should().NotContain("bench:bench");
    }

    [Theory]
    [InlineData("postgresql://localhost:5432/bench")]
    [InlineData("postgresql://bench@localhost:5432/bench")]
    [InlineData("postgresql://localhost:5432/bench?application_name=bench@home")]
    public void Redaction_Leaves_A_String_Without_A_Password_Unchanged(string connectionString)
    {
        using var transport = new PostgresYcsbTransport(connectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 1);

        transport.RecordedEndpoint.Should().Be(connectionString);
    }

    [Fact]
    public void Redaction_Removes_The_Password_From_A_Keyword_Connection_String()
    {
        using var transport = new PostgresYcsbTransport(
            "Host=localhost Port=5432 Username=bench Password=bench Database=bench",
            PostgreSqlTestEndpoints.Database,
            maxConcurrency: 1);

        transport.RecordedEndpoint.Should().Be("Host=localhost Port=5432 Username=bench Password=*** Database=bench");
        transport.RecordedEndpoint.Should().NotContain("Password=bench");
    }

    [Fact]
    public void Redaction_Leaves_A_Credential_Free_Keyword_String_Unchanged()
    {
        const string connectionString = "Host=localhost Port=5432 Username=bench Database=bench";

        using var transport = new PostgresYcsbTransport(connectionString, PostgreSqlTestEndpoints.Database, maxConcurrency: 1);

        transport.RecordedEndpoint.Should().Be(connectionString);
    }

    [Theory]
    [InlineData("postgresql://bench:bench@localhost:5432/other")]
    [InlineData("Host=localhost Port=5432 Username=bench Password=bench")]
    public void The_Run_Database_Overrides_The_Connection_Strings_Database(string connectionString)
    {
        var options = PostgresYcsbTransport.ResolveOptions(connectionString, "bench");

        options.Database.Should().Be("bench");
    }
}
