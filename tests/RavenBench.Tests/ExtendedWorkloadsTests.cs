using System;
using FluentAssertions;
using RavenBench.Cli;
using RavenBench.Core;
using RavenBench.Core.Workload;
using Xunit;

namespace RavenBench.Tests;

public class ExtendedWorkloadsTests
{
    private static StackOverflowWorkloadMetadata SoMetadata() => new()
    {
        QuestionIds = new[] { 1, 2, 3 },
        UserIds = new[] { 1 },
        TitlePrefixes = new[] { "How" },
        SearchTermsRare = new[] { "algorithm" },
        SearchTermsCommon = new[] { "error" },
        Tags = new[] { "c#", "python" },
        TitleSuggestionsIndexName = "Questions/ByTitleSuggestions-corax",
        TitleMoreLikeThisIndexName = "Questions/ByTitleMoreLikeThis-corax",
        ViewCountGroupedIndexName = "Questions/ByViewCountGrouped-corax",
        TagsIndexName = "Questions/ByTags-corax"
    };

    [Fact]
    public void Spatial_Workload_Emits_Radius_Query_Against_Spatial_Index()
    {
        var metadata = new StackOverflowUsersWorkloadMetadata { SpatialIndexName = "Users/BySpatial-corax" };
        var op = new UsersSpatialQueryWorkload(metadata).NextOperation(new Random(42));

        var query = op.Should().BeOfType<QueryOperation>().Subject;
        query.QueryText.Should().Contain("spatial.within(Coordinates, spatial.circle($radius, $lat, $lng))");
        query.ExpectedIndex.Should().Be("Users/BySpatial-corax");
        query.Parameters.Should().ContainKeys("radius", "lat", "lng");
    }

    [Fact]
    public void Suggestions_Workload_Emits_Suggest_Query()
    {
        var op = new QuestionsSuggestionsQueryWorkload(SoMetadata()).NextOperation(new Random(42));

        var query = op.Should().BeOfType<QueryOperation>().Subject;
        query.QueryText.Should().Contain("select suggest(Title, $term");
        query.ExpectedIndex.Should().Be("Questions/ByTitleSuggestions-corax");
        query.Parameters.Should().ContainKey("term");
    }

    [Fact]
    public void MoreLikeThis_Workload_Emits_MoreLikeThis_Query_With_Existing_Question_Id()
    {
        var op = new QuestionsMoreLikeThisQueryWorkload(SoMetadata()).NextOperation(new Random(42));

        var query = op.Should().BeOfType<QueryOperation>().Subject;
        query.QueryText.Should().Contain("morelikethis(id() = $id");
        query.Parameters["id"].Should().BeOfType<string>().Which.Should().StartWith("questions/");
    }

    [Fact]
    public void GroupBy_Workload_Emits_Bounded_Count_Range()
    {
        var op = new QuestionsGroupByQueryWorkload(SoMetadata()).NextOperation(new Random(42));

        var query = op.Should().BeOfType<QueryOperation>().Subject;
        query.QueryText.Should().Contain("where Count between $min and $max");
        ((int)query.Parameters["max"]!).Should().BeGreaterThanOrEqualTo((int)query.Parameters["min"]!);
    }

    [Fact]
    public void TextSearch_And_Prefix_Queries_Are_Bounded()
    {
        // Unbounded text-search/prefix queries return the whole matching set; common terms
        // match most of the corpus and OOM the load generator. Both must carry a limit.
        var meta = new StackOverflowWorkloadMetadata
        {
            TitlePrefixes = new[] { "How" },
            SearchTermsRare = new[] { "algorithm" },
            SearchTermsCommon = new[] { "error" },
            TitleIndexName = "Questions/ByTitle-corax",
            TitleSearchIndexName = "Questions/ByTitleSearch-corax"
        };

        new QuestionsByTitlePrefixWorkload(meta).NextOperation(new Random(1))
            .Should().BeOfType<QueryOperation>().Which.QueryText.Should().Contain("limit");
        new QuestionsByTitleSearchWorkload(meta).NextOperation(new Random(1))
            .Should().BeOfType<QueryOperation>().Which.QueryText.Should().Contain("limit");
    }

    [Fact]
    public void Stream_Workload_Emits_StreamQueryOperation_With_Sampled_Tag()
    {
        var op = new QuestionsStreamByTagQueryWorkload(SoMetadata()).NextOperation(new Random(42));

        var query = op.Should().BeOfType<StreamQueryOperation>().Subject;
        query.QueryText.Should().Contain("where Tag = $tag");
        SoMetadata().Tags.Should().Contain((string)query.Parameters["tag"]!);
    }

    [Fact]
    public void Patch_Workload_Targets_Preloaded_Documents()
    {
        var op = new PatchWorkload(new UniformDistribution(), 100).NextOperation(new Random(42));

        var patch = op.Should().BeOfType<DocumentPatchOperation>().Subject;
        patch.Id.Should().StartWith("bench/");
        patch.Script.Should().NotBeNullOrWhiteSpace();
    }

    [Theory]
    [InlineData(AttachmentOperationKind.Put, true)]
    [InlineData(AttachmentOperationKind.Get, false)]
    [InlineData(AttachmentOperationKind.Delete, false)]
    public void Attachment_Workload_Carries_Payload_Only_For_Put(AttachmentOperationKind kind, bool hasPayload)
    {
        var workload = new AttachmentWorkload(new UniformDistribution(), 100, kind, attachmentSizeBytes: 1024, seed: 42);
        var op = workload.NextOperation(new Random(42));

        var attachment = op.Should().BeOfType<AttachmentOperation>().Subject;
        attachment.Kind.Should().Be(kind);
        attachment.Name.Should().Be(AttachmentWorkload.NameFor(attachment.DocumentId));
        (attachment.Payload != null).Should().Be(hasPayload);
        if (hasPayload)
            attachment.Payload!.Length.Should().Be(1024);
    }

    [Theory]
    [InlineData("patch", WorkloadProfile.Patch)]
    [InlineData("attachments", WorkloadProfile.Attachments)]
    public void Parses_New_Profiles(string profile, WorkloadProfile expected)
    {
        var settings = new ClosedSettings { Url = "http://localhost:8080", Database = "test", Profile = profile, Preload = 10 };
        settings.ToRunOptions().Profile.Should().Be(expected);
    }

    [Theory]
    [InlineData("spatial", QueryProfile.Spatial)]
    [InlineData("suggestions", QueryProfile.Suggestions)]
    [InlineData("more-like-this", QueryProfile.MoreLikeThis)]
    [InlineData("group-by", QueryProfile.GroupBy)]
    [InlineData("stream", QueryProfile.Stream)]
    public void Parses_New_Query_Profiles(string queryProfile, QueryProfile expected)
    {
        var settings = new ClosedSettings { Url = "http://localhost:8080", Database = "test", Profile = "stackoverflow-text-search", QueryProfile = queryProfile };
        settings.ToRunOptions().QueryProfile.Should().Be(expected);
    }

    [Theory]
    [InlineData("create", AttachmentOperationKind.Put)]
    [InlineData("get", AttachmentOperationKind.Get)]
    [InlineData("delete", AttachmentOperationKind.Delete)]
    public void Parses_Attachment_Op(string attachmentOp, AttachmentOperationKind expected)
    {
        var settings = new ClosedSettings { Url = "http://localhost:8080", Database = "test", Profile = "attachments", AttachmentOp = attachmentOp, Preload = 10 };
        settings.ToRunOptions().AttachmentOp.Should().Be(expected);
    }
}
