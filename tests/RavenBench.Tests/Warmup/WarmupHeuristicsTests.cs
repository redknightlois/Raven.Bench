using System;
using System.Collections.Generic;
using FluentAssertions;
using RavenBench.Core;
using Xunit;

namespace RavenBench.Tests.Warmup;

public sealed class WarmupHeuristicsTests
{
    [Fact]
    public void HasConverged_WhenSingleIteration_ReturnsTrue()
    {
        var diagnostics = new List<WarmupDiagnostics>
        {
            CreateDiagnostics(p95Micros: 1_000, sampleCount: 10_000)
        };

        WarmupStabilityHeuristics.HasConverged(diagnostics).Should().BeTrue();
    }

    [Fact]
    public void HasConverged_WhenP95DriftWithinThreshold_ReturnsTrue()
    {
        var diagnostics = new List<WarmupDiagnostics>
        {
            CreateDiagnostics(p95Micros: 10_000, sampleCount: 20_000),
            CreateDiagnostics(p95Micros: 10_800, sampleCount: 22_000) // 8% drift
        };

        WarmupStabilityHeuristics.HasConverged(diagnostics).Should().BeTrue();
    }

    [Fact]
    public void HasConverged_WhenP95DriftExceedsThreshold_ReturnsFalse()
    {
        var diagnostics = new List<WarmupDiagnostics>
        {
            CreateDiagnostics(p95Micros: 10_000, sampleCount: 20_000),
            CreateDiagnostics(p95Micros: 12_500, sampleCount: 22_000) // 25% drift
        };

        WarmupStabilityHeuristics.HasConverged(diagnostics).Should().BeFalse();
    }

    [Fact]
    public void BuildSummary_WhenConverged_ReturnsSuccess()
    {
        var diagnostics = new List<WarmupDiagnostics>
        {
            CreateDiagnostics(p95Micros: 10_000, sampleCount: 20_000),
            CreateDiagnostics(p95Micros: 10_900, sampleCount: 21_000)
        };

        var summary = WarmupStabilityHeuristics.BuildSummary(diagnostics, requireConvergence: true, maxIterations: 3);

        summary.Converged.Should().BeTrue();
        summary.Reason.Should().Be(WarmupFailureReason.None);
        summary.Iterations.Should().HaveCount(2);
    }

    [Fact]
    public void BuildSummary_WhenExceedsIterations_ReturnsFailure()
    {
        var diagnostics = new List<WarmupDiagnostics>
        {
            CreateDiagnostics(p95Micros: 10_000, sampleCount: 20_000),
            CreateDiagnostics(p95Micros: 12_000, sampleCount: 21_000),
            CreateDiagnostics(p95Micros: 12_500, sampleCount: 22_000)
        };

        var summary = WarmupStabilityHeuristics.BuildSummary(diagnostics, requireConvergence: true, maxIterations: 3);

        summary.Converged.Should().BeFalse();
        summary.Reason.Should().Be(WarmupFailureReason.MaxIterations);
    }

    [Fact]
    public void HasConverged_WhenHighErrorRate_ReturnsFalse()
    {
        var diagnostics = new List<WarmupDiagnostics>
        {
            CreateDiagnostics(errorRate: 0.25)
        };

        WarmupStabilityHeuristics.HasConverged(diagnostics).Should().BeFalse();
    }

    private static WarmupDiagnostics CreateDiagnostics(
        double p95Micros = 1_000,
        long sampleCount = 10_000,
        double errorRate = 0.0)
    {
        return new WarmupDiagnosticsBuilder()
            .WithP95Micros(p95Micros)
            .WithSampleCount(sampleCount)
            .WithErrorRate(errorRate)
            .Build();
    }

    private sealed class WarmupDiagnosticsBuilder
    {
        private int _iteration = 0;
        private TimeSpan _duration = TimeSpan.FromSeconds(10);
        private double _throughput = 5_000;
        private double _errorRate = 0.0;
        private long _sampleCount = 10_000;
        private double _p50Micros = 500;
        private double _p95Micros = 1_000;
        private double _p99Micros = 1_500;
        private double _maxMicros = 5_000;

        public WarmupDiagnosticsBuilder WithErrorRate(double errorRate)
        {
            _errorRate = errorRate;
            return this;
        }

        public WarmupDiagnosticsBuilder WithP95Micros(double value)
        {
            _p95Micros = value;
            return this;
        }

        public WarmupDiagnosticsBuilder WithSampleCount(long value)
        {
            _sampleCount = value;
            return this;
        }

        public WarmupDiagnostics Build()
        {
            return new WarmupDiagnostics(
                iteration: _iteration,
                duration: _duration,
                throughput: _throughput,
                errorRate: _errorRate,
                sampleCount: _sampleCount,
                p50Micros: _p50Micros,
                p95Micros: _p95Micros,
                p99Micros: _p99Micros,
                maxMicros: _maxMicros);
        }
    }
}
