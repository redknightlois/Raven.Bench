using RavenBench.Core;
using RavenBench.Core.Workload;

namespace RavenBench;

internal static class WorkloadFactory
{
    internal static IWorkload BuildWorkload(RunOptions opts, StackOverflowWorkloadMetadata? stackOverflowMetadata, StackOverflowUsersWorkloadMetadata? usersMetadata, VectorWorkloadMetadata? vectorMetadata)
    {
        if (opts.Profile == WorkloadProfile.Unspecified)
            throw new InvalidOperationException("Workload profile is required. Specify --profile query-by-id|stackoverflow-random-reads|stackoverflow-text-search|query-users-by-name|vector-search|vector-search-exact|patch|attachments.");

        IKeyDistribution CreateDistribution()
        {
            return opts.Distribution switch
            {
                KeyDistributionKind.Uniform => new UniformDistribution(),
                KeyDistributionKind.Zipfian => new ZipfianDistribution(),
                KeyDistributionKind.Latest => new LatestDistribution(),
                _ => throw new ArgumentOutOfRangeException(nameof(opts.Distribution), opts.Distribution, null)
            };
        }

        return opts.Profile switch
        {
            WorkloadProfile.QueryById => BuildQueryWorkload(opts, CreateDistribution()),
            WorkloadProfile.StackOverflowRandomReads => new StackOverflowReadWorkload(stackOverflowMetadata!),
            WorkloadProfile.StackOverflowTextSearch => BuildStackOverflowQueryWorkload(opts, stackOverflowMetadata!),
            WorkloadProfile.QueryUsersByName => BuildUsersQueryWorkload(opts, usersMetadata!),
            WorkloadProfile.VectorSearch => BuildVectorSearchWorkload(opts, vectorMetadata!),
            WorkloadProfile.VectorSearchExact => BuildVectorSearchWorkload(opts, vectorMetadata!, exactSearch: true),
            WorkloadProfile.Patch => new PatchWorkload(CreateDistribution(), opts.Preload),
            WorkloadProfile.Attachments => new AttachmentWorkload(CreateDistribution(), opts.Preload, opts.AttachmentOp, opts.DocumentSizeBytes, opts.Seed),
            _ => throw new NotSupportedException($"Unsupported profile: {opts.Profile}")
        };
    }

    private static IWorkload BuildQueryWorkload(RunOptions opts, IKeyDistribution distribution)
    {
        if (opts.Preload <= 0)
            throw new InvalidOperationException("Query profile requires --preload to seed the keyspace before the run.");
        return new QueryWorkload(distribution, opts.Preload);
    }

    private static IWorkload BuildStackOverflowQueryWorkload(RunOptions opts, StackOverflowWorkloadMetadata metadata)
    {
        return opts.QueryProfile switch
        {
            QueryProfile.VoronEquality => new StackOverflowQueryWorkload(metadata, useVoronPath: true), // direct Voron lookup via id()
            QueryProfile.IndexEquality => new StackOverflowQueryWorkload(metadata, useVoronPath: false), // index-based lookup
            QueryProfile.TextPrefix => new QuestionsByTitlePrefixWorkload(metadata),
            QueryProfile.TextSearch => new QuestionsByTitleSearchWorkload(metadata, 0.3), // 30% rare, 70% common
            QueryProfile.TextSearchRare => new QuestionsByTitleSearchWorkload(metadata, 1.0), // 100% rare
            QueryProfile.TextSearchCommon => new QuestionsByTitleSearchWorkload(metadata, 0.0), // 100% common
            QueryProfile.TextSearchMixed => new QuestionsByTitleSearchWorkload(metadata, 0.5), // 50% rare, 50% common
            QueryProfile.Suggestions => new QuestionsSuggestionsQueryWorkload(metadata),
            QueryProfile.MoreLikeThis => new QuestionsMoreLikeThisQueryWorkload(metadata),
            QueryProfile.GroupBy => new QuestionsGroupByQueryWorkload(metadata),
            QueryProfile.Stream => new QuestionsStreamByTagQueryWorkload(metadata),
            _ => throw new NotSupportedException($"Query profile '{opts.QueryProfile}' is not supported for StackOverflow queries. Supported profiles: voron-equality, index-equality, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed, suggestions, more-like-this, group-by, stream")
        };
    }

    private static IWorkload BuildUsersQueryWorkload(RunOptions opts, StackOverflowUsersWorkloadMetadata metadata)
    {
        return opts.QueryProfile switch
        {
            QueryProfile.VoronEquality or QueryProfile.IndexEquality => new StackOverflowUsersByNameQueryWorkload(metadata),
            QueryProfile.Range => new StackOverflowUsersRangeQueryWorkload(metadata),
            QueryProfile.Spatial => new UsersSpatialQueryWorkload(metadata),
            _ => throw new NotSupportedException($"Query profile '{opts.QueryProfile}' is not supported for Users queries. Supported profiles: voron-equality, index-equality, range, spatial")
        };
    }

    private static IWorkload BuildVectorSearchWorkload(
        RunOptions opts,
        VectorWorkloadMetadata metadata,
        bool exactSearch = false,
        VectorQuantization? quantization = null)
    {
        var effectiveQuantization = quantization ?? opts.VectorQuantization;
        var effectiveExactSearch = exactSearch || opts.VectorExactSearch;

        return new VectorSearchWorkload(
            metadata,
            topK: opts.VectorTopK,
            minimumSimilarity: opts.VectorMinSimilarity,
            useExactSearch: effectiveExactSearch,
            quantization: effectiveQuantization,
            efSearch: opts.VectorSearchEf);
    }

    internal static bool IsVectorSearchProfile(WorkloadProfile profile)
    {
        return profile == WorkloadProfile.VectorSearch ||
               profile == WorkloadProfile.VectorSearchExact;
    }

    /// <summary>
    /// Determines if a profile requires preloaded bench/ documents.
    /// Dataset-based profiles (StackOverflow, Users, Vector) use their own imported data.
    /// </summary>
    internal static bool ProfileRequiresPreload(WorkloadProfile profile)
    {
        return profile switch
        {
            WorkloadProfile.QueryById => true,
            WorkloadProfile.Patch => true,
            WorkloadProfile.Attachments => true,
            _ => false
        };
    }
}
