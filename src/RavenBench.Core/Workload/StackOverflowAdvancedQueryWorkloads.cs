namespace RavenBench.Core.Workload;

/// <summary>
/// Spatial radius queries against the Users collection.
/// The spatial index synthesizes deterministic coordinates from AccountId (the dataset
/// has no real coordinates), so random centers yield real, varied selectivity.
/// </summary>
public sealed class UsersSpatialQueryWorkload : IWorkload
{
    private static readonly double[] RadiiKm = { 10, 50, 100, 500, 1000 };

    private readonly string _expectedIndexName;

    public UsersSpatialQueryWorkload(StackOverflowUsersWorkloadMetadata metadata)
    {
        _expectedIndexName = metadata.SpatialIndexName
            ?? throw new ArgumentException("Metadata must contain SpatialIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var radius = RadiiKm[rng.Next(RadiiKm.Length)];
        var latitude = rng.NextDouble() * 180.0 - 90.0;
        var longitude = rng.NextDouble() * 360.0 - 180.0;

        return new QueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' where spatial.within(Coordinates, spatial.circle($radius, $lat, $lng)) limit 100",
            Parameters = new Dictionary<string, object?>
            {
                ["radius"] = radius,
                ["lat"] = latitude,
                ["lng"] = longitude
            },
            ExpectedIndex = _expectedIndexName
        };
    }
}

/// <summary>
/// Term suggestion queries against question titles (suggest() over a suggestions-enabled field).
/// </summary>
public sealed class QuestionsSuggestionsQueryWorkload : IWorkload
{
    private readonly string[] _terms;
    private readonly string _expectedIndexName;

    public QuestionsSuggestionsQueryWorkload(StackOverflowWorkloadMetadata metadata)
    {
        // Terms shorter than 4 chars risk collapsing to a stopword even after the typo
        // mutation; the analyzer drops stopwords, which the suggest() endpoint rejects.
        _terms = metadata.SearchTermsRare.Concat(metadata.SearchTermsCommon)
            .Where(t => t.Length >= 4).ToArray();
        if (_terms.Length == 0)
            throw new ArgumentException("Metadata must contain search terms of at least 4 characters");

        _expectedIndexName = metadata.TitleSuggestionsIndexName
            ?? throw new ArgumentException("Metadata must contain TitleSuggestionsIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var term = Misspell(_terms[rng.Next(_terms.Length)], rng);

        return new QueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' select suggest(Title, $term, '{{ \"Accuracy\": 0.4, \"PageSize\": 5, \"Distance\": \"NGram\", \"SortMode\": \"Popularity\" }}')",
            Parameters = new Dictionary<string, object?> { ["term"] = term },
            ExpectedIndex = _expectedIndexName
        };
    }

    /// <summary>
    /// Replaces an interior letter with 'z', yielding a did-you-mean lookup. Real dictionary
    /// words can be analyzer stopwords, which suggest() rejects; no English stopword
    /// contains 'z', so the mutated term always survives analysis.
    /// </summary>
    internal static string Misspell(string term, Random rng)
    {
        var i = 1 + rng.Next(term.Length - 2);
        var chars = term.ToCharArray();
        chars[i] = chars[i] == 'z' ? 'x' : 'z';
        return new string(chars);
    }
}

/// <summary>
/// More-like-this queries: find questions similar to a sampled question by title terms.
/// </summary>
public sealed class QuestionsMoreLikeThisQueryWorkload : IWorkload
{
    private readonly int[] _questionIds;
    private readonly string _expectedIndexName;

    public QuestionsMoreLikeThisQueryWorkload(StackOverflowWorkloadMetadata metadata)
    {
        if (metadata.QuestionIds.Length == 0)
            throw new ArgumentException("Metadata must contain sampled question IDs");

        _questionIds = metadata.QuestionIds;
        _expectedIndexName = metadata.TitleMoreLikeThisIndexName
            ?? throw new ArgumentException("Metadata must contain TitleMoreLikeThisIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var id = $"questions/{_questionIds[rng.Next(_questionIds.Length)]}";

        return new QueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' where morelikethis(id() = $id, '{{ \"Fields\": [\"Title\"] }}') limit 16",
            Parameters = new Dictionary<string, object?> { ["id"] = id },
            ExpectedIndex = _expectedIndexName
        };
    }
}

/// <summary>
/// Aggregation queries over a static map-reduce index grouping questions by view count.
/// </summary>
public sealed class QuestionsGroupByQueryWorkload : IWorkload
{
    private readonly string _expectedIndexName;

    public QuestionsGroupByQueryWorkload(StackOverflowWorkloadMetadata metadata)
    {
        _expectedIndexName = metadata.ViewCountGroupedIndexName
            ?? throw new ArgumentException("Metadata must contain ViewCountGroupedIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var min = rng.Next(2, 11);
        var max = min + rng.Next(0, 51);

        return new QueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' where Count between $min and $max limit 128",
            Parameters = new Dictionary<string, object?>
            {
                ["min"] = min,
                ["max"] = max
            },
            ExpectedIndex = _expectedIndexName
        };
    }
}

/// <summary>
/// Streaming queries by tag through the streams endpoint, draining the full result set.
/// Skip/take bound the stream so per-operation cost stays independent of tag popularity.
/// </summary>
public sealed class QuestionsStreamByTagQueryWorkload : IWorkload
{
    private const int Take = 1024;

    private readonly string[] _tags;
    private readonly string _expectedIndexName;

    public QuestionsStreamByTagQueryWorkload(StackOverflowWorkloadMetadata metadata)
    {
        if (metadata.Tags.Length == 0)
            throw new ArgumentException("Metadata must contain sampled question tags");

        _tags = metadata.Tags;
        _expectedIndexName = metadata.TagsIndexName
            ?? throw new ArgumentException("Metadata must contain TagsIndexName for static index");
    }

    public OperationBase NextOperation(Random rng)
    {
        var tag = _tags[rng.Next(_tags.Length)];

        return new StreamQueryOperation
        {
            QueryText = $"from index '{_expectedIndexName}' where Tag = $tag limit {Take}",
            Parameters = new Dictionary<string, object?> { ["tag"] = tag },
            ExpectedIndex = _expectedIndexName
        };
    }
}
