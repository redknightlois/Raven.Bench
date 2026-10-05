using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using FluentAssertions;
using RavenBench.Tests.Infrastructure;
using Xunit;
using static RavenBench.Tests.VectorBench.VectorRunScriptTests;

namespace RavenBench.Tests.Scripts;

/// <summary>Holds the three benchmark scripts to the rules they share through one sourced helper file, against fake tools on PATH.</summary>
[Trait("Category", "Unit")]
public class RunScriptSharedRulesTests
{
    [Fact]
    public void The_Postgresql_Services_Are_Healthy_Only_When_A_Query_Over_Tcp_Succeeds()
    {
        foreach (var (bench, service) in new[] { ("ycsb", "postgresql"), ("vector", "pgvector") })
        {
            var compose = File.ReadAllText(Path.Combine(Folder(bench), "docker-compose.yml"));
            var block = Regex.Match(compose, $@"(?ms)^  {service}:\n(.*?)(?=^  \S|^\S)").Groups[1].Value;
            block.Should().MatchRegex(@"healthcheck:\s+test: \[""CMD-SHELL"", "".*psql -h 127\.0\.0\.1 .*-\w*c 'SELECT 1'", $"--wait honours the {bench} {service} healthcheck");
        }
    }
}
