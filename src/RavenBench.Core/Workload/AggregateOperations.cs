using System.Reflection;
using System.Text.Json;
using RavenBench.Core.Aggregate;

namespace RavenBench.Core.Workload;

/// <summary>A typed filter over one document field. Each transport translates it into its own query language.</summary>
public abstract record AggregateFilter(string Field);

/// <summary>Keeps the documents whose field equals the value.</summary>
public sealed record EqualityFilter(string Field, string Value) : AggregateFilter(Field);

/// <summary>
/// Keeps the documents whose field lies in [<see cref="Lower"/>, <see cref="Upper"/>): the lower
/// bound is inclusive and the upper bound is exclusive. The bounds compare as strings.
/// </summary>
public sealed record RangeFilter(string Field, string Lower, string Upper) : AggregateFilter(Field);

public enum AggregateKind
{
    Count,
    Sum
}

/// <summary>One group of a grouped aggregate result.</summary>
public readonly record struct AggregateGroup(string Key, long Value);

/// <summary>
/// Groups documents by <see cref="GroupBy"/>, computes a count or the integer sum of
/// <see cref="SumField"/> per group, and returns the top <see cref="TopN"/> groups in
/// <see cref="AggregateOrdering"/> order. When fewer groups exist, every group is returned.
/// </summary>
public sealed class GroupedAggregateOperation : OperationBase
{
    public required string GroupBy { get; init; }
    public required AggregateKind Kind { get; init; }

    /// <summary>The summed field; set for <see cref="AggregateKind.Sum"/> and null for a count.</summary>
    public string? SumField { get; init; }

    public AggregateFilter? Filter { get; init; }
    public required int TopN { get; init; }

    /// <summary>Throws when the operation cannot describe a query.</summary>
    public void Validate()
    {
        if (TopN < 1)
            throw new ArgumentOutOfRangeException(nameof(TopN), TopN, "A grouped aggregate needs a top N of at least 1.");
        if ((Kind == AggregateKind.Sum) != (SumField is not null))
            throw new ArgumentException($"A {Kind} aggregate {(Kind == AggregateKind.Sum ? "needs" : "takes no")} sum field.", nameof(SumField));
    }

    /// <summary>
    /// The name of the one RavenDB map-reduce index that serves this shape. The index groups by the
    /// group field and, when the filter is on another field, by the filter field too.
    /// </summary>
    public string IndexName
    {
        get
        {
            var name = Kind == AggregateKind.Count ? $"Aggregate/Count/By/{GroupBy}" : $"Aggregate/Sum/{SumField}/By/{GroupBy}";
            return Filter is null || Filter.Field == GroupBy ? name : $"{name}/Where/{Filter.Field}";
        }
    }
}

/// <summary>
/// The one order of a grouped aggregate result: value descending, then group key ascending by
/// ordinal comparison. Every product's result and the parity check use this order.
/// </summary>
public static class AggregateOrdering
{
    /// <summary>
    /// The tie break. Ordinal UTF-16 order equals MongoDB's binary UTF-8 order for every key
    /// outside the surrogate range.
    /// </summary>
    public static readonly StringComparer KeyComparer = StringComparer.Ordinal;

    public static int Compare(AggregateGroup a, AggregateGroup b)
    {
        int byValue = b.Value.CompareTo(a.Value);
        return byValue != 0 ? byValue : KeyComparer.Compare(a.Key, b.Key);
    }

    /// <summary>Orders the groups and keeps the first <paramref name="topN"/>.</summary>
    public static IReadOnlyList<AggregateGroup> Top(IEnumerable<AggregateGroup> groups, int topN)
    {
        var ordered = groups.ToList();
        ordered.Sort(Compare);
        return ordered.Count > topN ? ordered.GetRange(0, topN) : ordered;
    }

    /// <summary>The expected result of an operation over an in-memory set.</summary>
    public static IReadOnlyList<AggregateGroup> Compute(GroupedAggregateOperation op, IEnumerable<AggregateDocument> documents)
    {
        op.Validate();
        var totals = new Dictionary<string, long>(KeyComparer);
        foreach (var d in documents)
        {
            if (op.Filter is not null && Matches(op.Filter, Field(d, op.Filter.Field)) == false)
                continue;
            var key = Field(d, op.GroupBy);
            long add = op.Kind == AggregateKind.Count ? 1 : long.Parse(Field(d, op.SumField!));
            totals[key] = totals.GetValueOrDefault(key) + add;
        }
        return Top(totals.Select(t => new AggregateGroup(t.Key, t.Value)), op.TopN);
    }

    private static bool Matches(AggregateFilter filter, string value) => filter switch
    {
        EqualityFilter eq => KeyComparer.Equals(value, eq.Value),
        RangeFilter range => KeyComparer.Compare(value, range.Lower) >= 0 && KeyComparer.Compare(value, range.Upper) < 0,
        _ => throw new NotSupportedException($"Unknown filter type {filter.GetType().Name}.")
    };

    private static string Field(AggregateDocument d, string field) => field switch
    {
        AggregateDocument.CategoryField => d.Category,
        AggregateDocument.RegionField => d.Region,
        AggregateDocument.AmountField => d.Amount.ToString(),
        AggregateDocument.TimestampField => d.Timestamp,
        _ => throw new ArgumentException($"The aggregate document has no field '{field}'.", nameof(field))
    };
}

/// <summary>
/// The query shapes the aggregate benchmark runs, and the index definitions that serve them. The
/// definitions are embedded JSON files: one RavenDB map-reduce index per shape, and the MongoDB
/// indexes that only the indexed MongoDB target creates.
/// </summary>
public static class AggregateShapes
{
    public const string CountByCategory = "count-by-category";
    public const string SumByRegion = "sum-by-region";
    public const string FilteredGroup = "filtered-group";

    public static readonly IReadOnlyList<string> All = [CountByCategory, SumByRegion, FilteredGroup];

    /// <summary>The operation of a shape. The filter value applies to the filtered shape only.</summary>
    public static GroupedAggregateOperation Create(string shape, int topN, string? category = null) => shape switch
    {
        CountByCategory => new() { GroupBy = AggregateDocument.CategoryField, Kind = AggregateKind.Count, TopN = topN },
        SumByRegion => new() { GroupBy = AggregateDocument.RegionField, Kind = AggregateKind.Sum, SumField = AggregateDocument.AmountField, TopN = topN },
        FilteredGroup => new()
        {
            GroupBy = AggregateDocument.RegionField, Kind = AggregateKind.Sum, SumField = AggregateDocument.AmountField, TopN = topN,
            Filter = new EqualityFilter(AggregateDocument.CategoryField, category ?? throw new ArgumentNullException(nameof(category), "The filtered shape needs a category."))
        },
        _ => throw new ArgumentException($"Unknown aggregate shape '{shape}'; known are {string.Join(", ", All)}.", nameof(shape))
    };

    /// <summary>The RavenDB index definitions as <c>PUT /admin/indexes</c> accepts them, one per shape.</summary>
    public static IReadOnlyList<JsonElement> RavenDbIndexes() => Load("ravendb");

    /// <summary>The MongoDB index specifications, each with a <c>name</c> and a <c>key</c> document.</summary>
    public static IReadOnlyList<JsonElement> MongoIndexes() => Load("mongodb");

    private static IReadOnlyList<JsonElement> Load(string product)
    {
        var assembly = typeof(AggregateShapes).Assembly;
        var prefix = $"{assembly.GetName().Name}.Aggregate.Indexes.{product}.";
        var list = new List<JsonElement>();
        foreach (var name in assembly.GetManifestResourceNames().Where(n => n.StartsWith(prefix, StringComparison.Ordinal)).Order(StringComparer.Ordinal))
        {
            using var stream = assembly.GetManifestResourceStream(name)!;
            list.Add(JsonDocument.Parse(stream).RootElement.Clone());
        }
        return list.Count > 0 ? list : throw new InvalidOperationException($"No embedded {product} aggregate index definitions under '{prefix}'.");
    }
}

/// <summary>
/// Sets the category and the amount of one aggregate document. The values are data on the
/// operation; each transport owns the product syntax that writes them.
/// </summary>
public sealed class AggregateUpdateOperation : OperationBase
{
    public required string Id { get; init; }
    public required string Category { get; init; }
    public required long Amount { get; init; }
}
