using System.Numerics;

namespace RavenBench.Core;

public static class EnumerableExtensions
{
    /// <summary>
    /// Computes the sum and count of non-null values in a single pass.
    /// </summary>
    public static (TSum sum, int count) SumAndCount<TSource, TSum>(
        this IEnumerable<TSource> source,
        Func<TSource, TSum?> selector)
        where TSum : struct, INumber<TSum>
    {
        var sum = TSum.Zero;
        var count = 0;

        foreach (var item in source)
        {
            var value = selector(item);
            if (value.HasValue)
            {
                sum += value.Value;
                count++;
            }
        }

        return (sum, count);
    }
}
