namespace RavenBench.Workload;

/// <summary>
/// Workload that exercises parameterized prefix text search queries against the Questions collection.
/// Queries use the pattern: FROM Questions WHERE startsWith(Title, $prefix)
/// Uses pre-discovered title prefixes with varying selectivity.
/// </summary>
public sealed class QuestionsByTitlePrefixWorkload : IWorkload
{
    private readonly string[] _titlePrefixes;
    private const string ExpectedIndexName = "Auto/Questions/ByTitle";

    /// <summary>
    /// Creates a Questions prefix search workload using sampled title prefixes.
    /// </summary>
    /// <param name="metadata">Workload metadata containing title prefixes</param>
    public QuestionsByTitlePrefixWorkload(StackOverflowWorkloadMetadata metadata)
    {
        if (metadata.TitlePrefixes.Length == 0)
        {
            throw new ArgumentException("Metadata must contain sampled title prefixes");
        }

        _titlePrefixes = metadata.TitlePrefixes;
    }

    public OperationBase NextOperation(Random rng)
    {
        // Select a random prefix from the sampled set
        var prefix = _titlePrefixes[rng.Next(_titlePrefixes.Length)];

        return new QueryOperation
        {
            QueryText = "from questions where startsWith(Title, $prefix)",
            Parameters = new Dictionary<string, object?> { ["prefix"] = prefix },
            ExpectedIndex = ExpectedIndexName
        };
    }
}

/// <summary>
/// Workload that exercises full-text search queries against the Questions collection.
/// Queries use the pattern: FROM Questions WHERE search(Title, $term)
/// Uses configurable mix of rare and common search terms to test different selectivity.
/// </summary>
public sealed class QuestionsByTitleSearchWorkload : IWorkload
{
    private readonly string[] _searchTermsRare;
    private readonly string[] _searchTermsCommon;
    private const string ExpectedIndexName = "Auto/Questions/Search(Title)";
    private readonly double _rareTermProbability; // Probability of selecting rare terms (0.0 = common only, 1.0 = rare only)

    /// <summary>
    /// Creates a Questions full-text search workload using sampled search terms.
    /// </summary>
    /// <param name="metadata">Workload metadata containing rare and common search terms</param>
    /// <param name="rareTermProbability">Probability of selecting rare terms (0.0 to 1.0). Must be between 0.0 and 1.0 inclusive.</param>
    public QuestionsByTitleSearchWorkload(StackOverflowWorkloadMetadata metadata, double rareTermProbability = 0.3)
    {
        if (rareTermProbability < 0.0 || rareTermProbability > 1.0)
        {
            throw new ArgumentOutOfRangeException(nameof(rareTermProbability), rareTermProbability, "Rare term probability must be between 0.0 and 1.0");
        }

        if (metadata.SearchTermsRare.Length == 0 || metadata.SearchTermsCommon.Length == 0)
        {
            throw new ArgumentException("Metadata must contain both rare and common search terms");
        }

        _searchTermsRare = metadata.SearchTermsRare;
        _searchTermsCommon = metadata.SearchTermsCommon;
        _rareTermProbability = rareTermProbability;
    }

    public OperationBase NextOperation(Random rng)
    {
        // Randomly choose between rare and common terms based on configured probability
        // Rare terms produce fewer results (higher selectivity), common terms produce more results
        var useRareTerm = rng.NextDouble() < _rareTermProbability;
        var termArray = useRareTerm ? _searchTermsRare : _searchTermsCommon;
        var searchTerm = termArray[rng.Next(termArray.Length)];

        return new QueryOperation
        {
            QueryText = "from questions where search(Title, $term)",
            Parameters = new Dictionary<string, object?> { ["term"] = searchTerm },
            ExpectedIndex = ExpectedIndexName
        };
    }
}
