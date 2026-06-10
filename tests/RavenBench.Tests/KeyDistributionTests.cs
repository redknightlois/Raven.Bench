using System;
using System.Linq;
using FluentAssertions;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class KeyDistributionTests
{
    [Fact]
    public void Zipfian_Key1_Is_Most_Frequent()
    {
        var counts = SampleZipfian(maxKey: 100, samples: 100_000);

        counts[1].Should().Be(counts.Skip(1).Max());
    }

    [Fact]
    public void Zipfian_Frequencies_Are_Monotonically_NonIncreasing_Over_First_Ranks()
    {
        var counts = SampleZipfian(maxKey: 100, samples: 100_000);

        // Statistical slack: each rank must hold at least 80% of the next-lower rank's count.
        for (int k = 1; k < 8; k++)
        {
            counts[k].Should().BeGreaterThan((int)(counts[k + 1] * 0.8),
                $"rank {k} should not be less frequent than rank {k + 1}");
        }
    }

    [Fact]
    public void Zipfian_Max_Key_Gets_No_Anomalous_Mass()
    {
        var counts = SampleZipfian(maxKey: 100, samples: 100_000);

        counts[100].Should().BeLessThan(counts[3]);
    }

    [Fact]
    public void Zipfian_Samples_Are_Within_Range()
    {
        var rng = new Random(42);
        var zipf = new ZipfianDistribution();
        for (int i = 0; i < 100_000; i++)
        {
            int k = zipf.NextKey(rng, 100);
            k.Should().BeInRange(1, 100);
        }
    }

    [Fact]
    public void Zipfian_Handles_Growing_And_Shrinking_Keyspace()
    {
        var rng = new Random(7);
        var zipf = new ZipfianDistribution();
        foreach (int max in new[] { 10, 1000, 100, 1_000_000 })
        {
            for (int i = 0; i < 1000; i++)
            {
                zipf.NextKey(rng, max).Should().BeInRange(1, max);
            }
        }
    }

    [Fact]
    public void Latest_Keys_Near_Max_Dominate()
    {
        var rng = new Random(42);
        var latest = new LatestDistribution();
        const int maxKey = 1000;
        const int samples = 100_000;

        int hotCount = 0;
        for (int i = 0; i < samples; i++)
        {
            int k = latest.NextKey(rng, maxKey);
            k.Should().BeInRange(1, maxKey);
            if (k > maxKey * 0.8)
                hotCount++;
        }

        // 80% targeted + ~4% uniform spillover into the hot range; require well above uniform's 20%.
        hotCount.Should().BeGreaterThan((int)(samples * 0.7));
    }

    [Fact]
    public void Uniform_MaxKey_One_Returns_One()
    {
        var rng = new Random(42);
        new UniformDistribution().NextKey(rng, 1).Should().Be(1);
    }

    [Fact]
    public void Uniform_Large_Max_Does_Not_Throw()
    {
        var rng = new Random(42);
        var uniform = new UniformDistribution();
        for (int i = 0; i < 1000; i++)
        {
            int k = uniform.NextKey(rng, int.MaxValue);
            k.Should().BeGreaterOrEqualTo(1);
        }
    }

    private static int[] SampleZipfian(int maxKey, int samples)
    {
        var rng = new Random(42);
        var zipf = new ZipfianDistribution();
        var counts = new int[maxKey + 1];
        for (int i = 0; i < samples; i++)
        {
            counts[zipf.NextKey(rng, maxKey)]++;
        }

        return counts;
    }
}
