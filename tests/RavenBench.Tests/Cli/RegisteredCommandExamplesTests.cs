using FluentAssertions;
using Spectre.Console.Cli;
using Xunit;

namespace RavenBench.Tests.Cli;

/// <summary>
/// Startup validation must accept every example the application registers. A registered example
/// that does not parse aborts the whole application through <c>ValidateExamples</c>.
/// </summary>
public class RegisteredCommandExamplesTests
{
    [Theory]
    [InlineData("closed")]
    [InlineData("rate")]
    [InlineData("recall")]
    [InlineData("index-build")]
    [InlineData("ycsb")]
    public void Registered_Example_Parses_During_Startup_Validation(string command)
    {
        var app = new CommandApp();
        app.Configure(global::RavenBench.Program.Configure);

        var exitCode = app.Run(new[] { command, "--help" });

        exitCode.Should().Be(0, $"the registered '{command}' example must parse, or ValidateExamples aborts the application");
    }
}
