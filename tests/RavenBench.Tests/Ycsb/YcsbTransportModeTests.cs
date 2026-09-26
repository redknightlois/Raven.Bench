using System;
using System.Threading.Tasks;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Ycsb;
using RavenBench.Ycsb;
using Xunit;

namespace RavenBench.Tests.Ycsb;

/// <summary>Mode selection per target and the refusal on targets without a mode.</summary>
public class YcsbTransportModeTests
{
    [Theory]
    [InlineData("postgresql", "raw", TransportKind.Raw)]
    [InlineData("postgresql", "client", TransportKind.Client)]
    [InlineData("postgresql", "client-entity", TransportKind.ClientEntity)]
    [InlineData("ravendb", "client-entity", TransportKind.ClientEntity)]
    [InlineData("mongodb", "raw", TransportKind.Raw)]
    [InlineData("documentdb", "raw", TransportKind.Raw)]
    public void A_Defined_Mode_Is_Selected(string target, string transport, TransportKind expected) =>
        YcsbRunner.ResolveTransportKind(target, transport).Should().Be(expected);

    [Theory]
    [InlineData("mongodb", "client")]
    [InlineData("mongodb", "client-entity")]
    [InlineData("documentdb", "client")]
    [InlineData("documentdb", "client-entity")]
    public void A_Target_Without_The_Mode_Refuses_It_By_Name(string target, string transport)
    {
        var refusal = () => YcsbRunner.ResolveTransportKind(target, transport);
        refusal.Should().Throw<YcsbScenarioException>().Where(e => e.Message.Contains(target) && e.Message.Contains(transport));
    }

    [Fact]
    public void An_Invalid_Mode_Keeps_The_Existing_Parse_Error() =>
        ((Action)(() => YcsbRunner.ResolveTransportKind("postgresql", "npgsql"))).Should().Throw<ArgumentException>().WithMessage("Invalid transport: npgsql*");

    [Fact]
    public async Task A_Refused_Mode_Fails_Before_Any_Connection()
    {
        // Nothing answers on this port: a run that reached a connection would fail on it instead.
        var scenario = new YcsbScenario
        {
            Seed = 1, Target = "mongodb", DocumentCount = 10, DocumentSize = "256B",
            Concurrency = "2..2", Distribution = "uniform", Warmup = "0s", Duration = "200ms"
        };
        var settings = new YcsbSettings { Url = "mongodb://localhost:1", Database = "never", Scenario = "unused.json", Transport = "client" };

        var run = () => new YcsbRunner(scenario, settings).RunAsync();

        (await run.Should().ThrowAsync<YcsbScenarioException>()).Which.Message.Should().Contain("mongodb").And.Contain("client");
    }
}
