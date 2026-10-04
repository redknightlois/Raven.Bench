using System;
using System.Linq;
using FluentAssertions;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class KeyDistributionTests
{
    [Fact]
    public void Zipfian_Hottest_Keys_Are_Spread_Over_The_Keyspace()
    {
        var counts = SampleZipfian(maxKey: 1000, samples: 100_000);

        var hottest = Enumerable.Range(1, 1000).OrderByDescending(k => counts[k]).Take(10).ToArray();

        hottest.Should().NotBeEquivalentTo(Enumerable.Range(1, 10));
        hottest.Count(k => k <= 10).Should().BeLessThan(5, "a contiguous hot set packs the hottest keys onto the first ids");
    }

    [Fact]
    public void Zipfian_Sorted_Key_Frequencies_Keep_The_Zipfian_Rank_Shape()
    {
        const double theta = 0.99;
        var counts = SampleZipfian(maxKey: 100, samples: 200_000);
        var byRank = counts.Skip(1).OrderByDescending(c => c).ToArray();

        // f(1) / f(k) = k^theta for a zipfian rank-frequency curve; the slack covers sampling noise.
        for (int k = 2; k <= 8; k++)
        {
            ((double)byRank[0] / byRank[k - 1]).Should().BeApproximately(Math.Pow(k, theta), Math.Pow(k, theta) * 0.2,
                $"rank {k} should hold 1/{k}^theta of rank 1's frequency");
        }
    }

    [Fact]
    public void Zipfian_Alternating_Keyspace_Sizes_Do_Not_Resum_The_Zeta_Cache()
    {
        const int n = 100_000;
        var rng = new Random(3);
        var zipf = new ZipfianDistribution();

        zipf.NextKey(rng, n + 1);
        var afterFirst = zipf.ZetaTermsComputed;
        for (int i = 0; i < 100; i++)
        {
            zipf.NextKey(rng, n).Should().BeInRange(1, n);
            zipf.NextKey(rng, n + 1).Should().BeInRange(1, n + 1);
        }

        afterFirst.Should().Be(n + 1);
        (zipf.ZetaTermsComputed - afterFirst).Should().Be(200, "each step between n and n + 1 moves the cache by one term");
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
    public void Zipfian_Same_Seed_Draws_The_Same_Key_Sequence()
    {
        // INVARIANT: the hot keys come from the run's seeded source, so a seed reproduces them.
        var zipfian = new ZipfianDistribution();

        var first = Draw(zipfian, seed: 99);
        var second = Draw(zipfian, seed: 99);

        first.Should().Equal(second);
    }

    [Fact]
    public void Latest_Same_Seed_Draws_The_Same_Key_Sequence()
    {
        var latest = new LatestDistribution();

        var first = Draw(latest, seed: 55);
        var second = Draw(latest, seed: 55);

        first.Should().Equal(second);
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

    [Fact]
    public void Every_Distribution_Name_Parses_And_Builds_Through_One_Rule()
    {
        foreach (var kind in Enum.GetValues<RavenBench.Core.KeyDistributionKind>())
        {
            KeyDistributions.TryParse(kind.ToString().ToUpperInvariant(), out var parsed).Should().BeTrue();
            parsed.Should().Be(kind);
            KeyDistributions.Create(kind).NextKey(new Random(1), 10).Should().BeInRange(1, 10);
        }

        KeyDistributions.TryParse(((int)RavenBench.Core.KeyDistributionKind.Zipfian).ToString(System.Globalization.CultureInfo.InvariantCulture), out _).Should().BeFalse();
    }

    private static int[] Draw(IKeyDistribution distribution, int seed)
    {
        var rng = new Random(seed);
        return Enumerable.Range(0, 1000).Select(_ => distribution.NextKey(rng, 1000)).ToArray();
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
