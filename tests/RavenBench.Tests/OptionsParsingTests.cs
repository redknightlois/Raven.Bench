using System;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests;

public class CliParsingTests
{
    [Fact]
    public void Converts_Settings_To_RunOptions_With_Concurrency_Range()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Concurrency = "16..256x1.5",
            KneeRule = "dthr=3%,dp95=30%",
            MaxErrors = "1%",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.ConcurrencyStart.Should().Be(16);
        opts.ConcurrencyEnd.Should().Be(256);
        opts.ConcurrencyFactor.Should().Be(1.5);
        opts.KneeThroughputDelta.Should().BeApproximately(0.03, 1e-9);
        opts.KneeP95Delta.Should().BeApproximately(0.30, 1e-9);
        opts.MaxErrorRate.Should().BeApproximately(0.01, 1e-9);
    }

    [Fact]
    public void Converts_Weight_Based_Mix_Flags()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:10101",
            Database = "ycsb",
            Reads = "3",
            Writes = "1",
            Updates = "0",
            Profile = "mixed"
        };

        var opts = settings.ToRunOptions();

        opts.Reads.Should().Be(3);
        opts.Writes.Should().Be(1);
        opts.Updates.Should().Be(0);
    }

    [Fact]
    public void Parses_Query_Profile_Defaults_To_Equality()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-queries"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.Equality);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearch()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-queries",
            QueryProfile = "text-search"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearch);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearchRare()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-queries",
            QueryProfile = "text-search-rare"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearchRare);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearchCommon()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-queries",
            QueryProfile = "text-search-common"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearchCommon);
    }

    [Fact]
    public void Parses_Query_Profile_TextSearchMixed()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-queries",
            QueryProfile = "text-search-mixed"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.TextSearchMixed);
    }

    [Fact]
    public void Parses_Query_Profile_Range()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "query-users-by-name",
            QueryProfile = "range"
        };

        var opts = settings.ToRunOptions();

        opts.QueryProfile.Should().Be(QueryProfile.Range);
    }

    [Fact]
    public void Throws_On_Invalid_Query_Profile()
    {
        var settings = new RunSettings
        {
            Url = "http://localhost:8080",
            Database = "test",
            Profile = "stackoverflow-queries",
            QueryProfile = "invalid"
        };

        var act = () => settings.ToRunOptions();

        act.Should().Throw<ArgumentException>()
            .WithMessage("Invalid query profile: invalid. Valid options: equality, range, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed");
    }
}
