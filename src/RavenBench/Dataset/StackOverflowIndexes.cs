using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using RavenBench.Core;

namespace RavenBench.Dataset;

/// <summary>
/// Catalog of static indexes available for the StackOverflow dataset.
/// </summary>
public enum StackOverflowIndex
{
    UsersByDisplayName,
    UsersByReputation,
    QuestionsByTitle,
    QuestionsByTitleSearch,
    UsersBySpatial,
    QuestionsByTitleSuggestions,
    QuestionsByTitleMoreLikeThis,
    QuestionsByViewCountGrouped,
    QuestionsByTags,
    QuestionsAnswers,
    UsersTimeseries
}

// Minimal shapes of the dataset documents; class names must pluralize to the
// dataset collection names (Users, Questions) so index maps target the right collections.
public sealed class User
{
    public long AccountId { get; set; }
    public string? DisplayName { get; set; }
    public int Reputation { get; set; }
}

public sealed class Answer
{
    public int OwnerUserId { get; set; }
    public DateTime CreationDate { get; set; }
    public string? Body { get; set; }
}

public sealed class Question
{
    public string? Title { get; set; }
    public string[]? Tags { get; set; }
    public int ViewCount { get; set; }
    public List<Answer>? Answers { get; set; }
}

/// <summary>
/// Builds engine-suffixed names and definitions for StackOverflow static indexes.
/// Definitions come from strongly-typed index classes; the name and search-engine
/// configuration are stamped per requested engine.
/// </summary>
public static class StackOverflowIndexes
{
    public static string GetName(StackOverflowIndex index, IndexingEngine searchEngine)
    {
        var suffix = VectorIndexMapping.GetEngineSuffix(searchEngine);
        var baseName = index switch
        {
            StackOverflowIndex.UsersByDisplayName => "Users/ByDisplayName",
            StackOverflowIndex.UsersByReputation => "Users/ByReputation",
            StackOverflowIndex.QuestionsByTitle => "Questions/ByTitle",
            StackOverflowIndex.QuestionsByTitleSearch => "Questions/ByTitleSearch",
            StackOverflowIndex.UsersBySpatial => "Users/BySpatial",
            StackOverflowIndex.QuestionsByTitleSuggestions => "Questions/ByTitleSuggestions",
            StackOverflowIndex.QuestionsByTitleMoreLikeThis => "Questions/ByTitleMoreLikeThis",
            StackOverflowIndex.QuestionsByViewCountGrouped => "Questions/ByViewCountGrouped",
            StackOverflowIndex.QuestionsByTags => "Questions/ByTags",
            StackOverflowIndex.QuestionsAnswers => "Questions/Answers",
            StackOverflowIndex.UsersTimeseries => "Users/Timeseries",
            _ => throw new ArgumentOutOfRangeException(nameof(index), index, null)
        };
        return baseName + suffix;
    }

    public static IndexDefinition GetDefinition(StackOverflowIndex index, IndexingEngine searchEngine)
    {
        if (index == StackOverflowIndex.UsersTimeseries)
        {
            var tsTask = new UsersTimeseries { Conventions = new DocumentConventions() };
            return Finish(tsTask.CreateIndexDefinition(), index, searchEngine);
        }

        AbstractIndexCreationTask task = index switch
        {
            StackOverflowIndex.UsersByDisplayName => new UsersByDisplayName(),
            StackOverflowIndex.UsersByReputation => new UsersByReputation(),
            StackOverflowIndex.QuestionsByTitle => new QuestionsByTitle(),
            StackOverflowIndex.QuestionsByTitleSearch => new QuestionsByTitleSearch(),
            StackOverflowIndex.UsersBySpatial => new UsersBySpatial(),
            StackOverflowIndex.QuestionsByTitleSuggestions => new QuestionsByTitleSuggestions(),
            StackOverflowIndex.QuestionsByTitleMoreLikeThis => new QuestionsByTitleMoreLikeThis(),
            StackOverflowIndex.QuestionsByViewCountGrouped => new QuestionsByViewCountGrouped(),
            StackOverflowIndex.QuestionsByTags => new QuestionsByTags(),
            StackOverflowIndex.QuestionsAnswers => new QuestionsAnswers(),
            _ => throw new ArgumentOutOfRangeException(nameof(index), index, null)
        };

        task.Conventions = new DocumentConventions();
        return Finish(task.CreateIndexDefinition(), index, searchEngine);
    }

    private static IndexDefinition Finish(IndexDefinition definition, StackOverflowIndex index, IndexingEngine searchEngine)
    {
        definition.Name = GetName(index, searchEngine);
        definition.Configuration["Indexing.Static.SearchEngineType"] =
            searchEngine == IndexingEngine.Lucene ? "Lucene" : "Corax";
        return definition;
    }

    private sealed class UsersByDisplayName : AbstractIndexCreationTask<User>
    {
        public UsersByDisplayName()
        {
            Map = users => from u in users select new { u.DisplayName };
        }
    }

    private sealed class UsersByReputation : AbstractIndexCreationTask<User>
    {
        public UsersByReputation()
        {
            Map = users => from u in users select new { u.Reputation };
        }
    }

    private sealed class QuestionsByTitle : AbstractIndexCreationTask<Question>
    {
        public QuestionsByTitle()
        {
            Map = questions => from q in questions select new { q.Title };
        }
    }

    private sealed class QuestionsByTitleSearch : AbstractIndexCreationTask<Question>
    {
        public QuestionsByTitleSearch()
        {
            Map = questions => from q in questions select new { q.Title };
            Index(x => x.Title, FieldIndexing.Search);
        }
    }

    /// <summary>
    /// The dataset has no real coordinates; deterministic synthetic coordinates derived from
    /// AccountId exercise the spatial indexing and radius-query path with honest selectivity.
    /// </summary>
    private sealed class UsersBySpatial : AbstractIndexCreationTask<User>
    {
        public UsersBySpatial()
        {
            Map = users => from u in users
                           select new
                           {
                               Coordinates = CreateSpatialField(
                                   ((u.AccountId % 18000) + 18000) % 18000 / 100.0 - 90.0,
                                   ((u.AccountId * 7 % 36000) + 36000) % 36000 / 100.0 - 180.0)
                           };
        }
    }

    private sealed class QuestionsByTitleSuggestions : AbstractIndexCreationTask<Question>
    {
        public QuestionsByTitleSuggestions()
        {
            Map = questions => from q in questions select new { q.Title };
            Index(x => x.Title, FieldIndexing.Search);
            Suggestion(x => x.Title);
        }
    }

    private sealed class QuestionsByTitleMoreLikeThis : AbstractIndexCreationTask<Question>
    {
        public QuestionsByTitleMoreLikeThis()
        {
            Map = questions => from q in questions select new { q.Title };
            Index(x => x.Title, FieldIndexing.Search);
            Store(x => x.Title, FieldStorage.Yes);
            TermVector(x => x.Title, FieldTermVector.Yes);
        }
    }

    private sealed class QuestionsByViewCountGrouped : AbstractIndexCreationTask<Question, QuestionsByViewCountGrouped.Result>
    {
        public sealed class Result
        {
            public int ViewCount { get; set; }
            public int Count { get; set; }
        }

        public QuestionsByViewCountGrouped()
        {
            Map = questions => from q in questions
                               select new { q.ViewCount, Count = 1 };
            Reduce = results => from r in results
                                group r by r.ViewCount into g
                                select new { ViewCount = g.Key, Count = g.Sum(x => x.Count) };
        }
    }

    /// <summary>
    /// Fanout: one question produces one entry per tag.
    /// </summary>
    private sealed class QuestionsByTags : AbstractIndexCreationTask<Question>
    {
        public QuestionsByTags()
        {
            Map = questions => from q in questions
                               from tag in q.Tags
                               select new { Tag = tag };
        }
    }

    /// <summary>
    /// Heavy fanout mirroring the QA Questions/Answers index: one question produces one
    /// stored entry per answer, including the answer body.
    /// </summary>
    private sealed class QuestionsAnswers : AbstractIndexCreationTask<Question>
    {
        public QuestionsAnswers()
        {
            Map = questions => from q in questions
                               from a in q.Answers
                               select new
                               {
                                   PostedBy = "users/" + a.OwnerUserId,
                                   Date = a.CreationDate,
                                   Message = a.Body
                               };
            StoreAllFields(FieldStorage.Yes);
        }
    }

    /// <summary>
    /// Indexes every time-series entry on Users documents. The dataset ships without time
    /// series; the index-build command seeds them before building this index.
    /// </summary>
    private sealed class UsersTimeseries : Raven.Client.Documents.Indexes.TimeSeries.AbstractTimeSeriesIndexCreationTask<User>
    {
        public UsersTimeseries()
        {
            AddMapForAll(segments => from segment in segments
                                     from entry in segment.Entries
                                     select new { entry.Value, entry.Tag });
        }
    }
}
