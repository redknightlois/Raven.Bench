using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using FluentAssertions;
using RavenBench.Core.Transport;
using Xunit;

namespace RavenBench.Tests.Transport;

public class TransportFailureStatusTests
{
    [Fact]
    public void A_Failed_Http_Operation_Names_Its_Status_Code()
    {
        var result = TransportResult.FromException(new HttpRequestException("x", null, HttpStatusCode.ServiceUnavailable), CancellationToken.None);

        result.IsSuccess.Should().BeFalse();
        result.ErrorDetails.Should().Contain("503");
    }

    [Fact]
    public void A_Failure_Without_A_Status_Keeps_The_Generic_Error_Text()
    {
        var result = TransportResult.FromException(new HttpRequestException("refused"), CancellationToken.None);

        result.ErrorDetails.Should().Be("HTTP Error: refused");
    }
}
