using System;
using System.Collections.Generic;
using System.Text.Json;
using FluentAssertions;
using RavenBench.Core;
using RavenBench.Core.Reporting;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests.Transport;

public class CredentialRedactionTests
{
    public static TheoryData<string, string> CredentialedEndpoints => new()
    {
        { "pgvector", "postgresql://bench:secret@host:5432/bench" },
        { MongoYcsbTransport.MongoDbTarget, "mongodb://user:pass@host" },
        { MongoYcsbTransport.DocumentDbTarget, "mongodb://user:pass@host:27017/?tls=true" },
        { "elasticsearch", "http://elastic:pass@host:9200" },
        { "ravendb", "http://admin:pass@host" },
    };

    private static IYcsbTransport Build(string target, string url) => target switch
    {
        "pgvector" => new PgVectorTransport(url, "bench", 1, VectorMetric.Cosine, 16),
        "elasticsearch" => new ElasticsearchVectorTransport(url, "bench", VectorMetric.Cosine, 16, ElasticsearchIndexKind.All[0].Name),
        "ravendb" => new RawHttpTransport(url, "bench", CompressionMode.Identity, System.Net.HttpVersion.Version11),
        _ => new MongoYcsbTransport(url, "bench", target),
    };

    [Theory]
    [MemberData(nameof(CredentialedEndpoints))]
    public void A_Summary_Recorded_From_The_Transport_Endpoint_Carries_No_Password(string target, string url)
    {
        var password = new Uri(url).UserInfo.Split(':')[1];
        using var transport = Build(target, url);

        var summary = new BenchmarkSummary
        {
            Options = new RunOptions { Url = transport.RecordedEndpoint, Database = "bench" },
            Steps = new List<StepResult>(),
            Verdict = "measured",
            ClientCompression = "n/a",
            EffectiveHttpVersion = "n/a",
        };
        var json = JsonSerializer.Serialize(summary);

        json.Should().NotContain(password);
        json.Should().Contain(new Uri(url).Host);
    }

    [Theory]
    [InlineData(@"password=ab\ cd host=x", "ab", "cd", "host=x")]
    [InlineData(@"host=x password=ab\ cd\ ef port=5432", "ab", "ef", "port=5432")]
    [InlineData(@"password='a\'b' host=x", "a\\'b", "'b", "host=x")]
    public void A_Keyword_Password_Is_Redacted_Whole_And_The_Text_After_It_Is_Kept(string connectionString, string head, string tail, string after)
    {
        var redacted = ConnectionStringRedaction.Redact(connectionString);

        redacted.Should().NotContain(head).And.NotContain(tail);
        redacted.Should().EndWith(after);
    }

    [Theory]
    [InlineData("postgresql://u@h/db?password=s3cret")]
    [InlineData("host=h user=u sslpassword=s3cret")]
    [InlineData("mongodb://u:p@h/?tlsCertificateKeyFilePassword=s3cret")]
    [InlineData("postgresql://u@h/db?sslmode=require&PASSWORD=s3cret&application_name=x")]
    [InlineData("host=h pwd=s3cret")]
    public void Every_Password_Like_Value_Is_Redacted(string connectionString)
    {
        ConnectionStringRedaction.Redact(connectionString).Should().NotContain("s3cret");
    }

    [Theory]
    [InlineData("postgresql://u@h/db?sslmode=require&application_name=x#frag")]
    [InlineData("host=h user=u sslmode=require")]
    public void A_Credential_Free_String_Is_Unchanged(string connectionString)
    {
        ConnectionStringRedaction.Redact(connectionString).Should().Be(connectionString);
    }
}
