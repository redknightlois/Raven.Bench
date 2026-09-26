using RavenBench.Core.Workload;

namespace RavenBench.Core.Aggregate;

/// <summary>
/// Splits the documents outside the tracked group between the probe and the bulk writers, so no
/// document is written by both: the probe takes the even positions, the bulk writers the odd ones.
/// </summary>
public static class UnderWriteSplit
{
    /// <summary>The documents the probe moves into the tracked group, in order.</summary>
    public static IEnumerable<AggregateDocument> ProbeSources(IEnumerable<AggregateDocument> documents, string trackedGroup) =>
        OutsideTracked(documents, trackedGroup).Where((_, i) => i % 2 == 0);

    /// <summary>
    /// One <see cref="BulkWriter"/> per writer, each with up to <paramref name="documentsPerWriter"/> documents of its own.
    /// A bulk writer raises only its target group, by at most one at any moment, so a group is the target of
    /// fewer writers than its distance below the tracked count, and the tracked group stays first.
    /// </summary>
    /// <exception cref="InvalidOperationException">The groups below the tracked count cannot take every writer, or a writer gets no document to move.</exception>
    public static BulkWriter[] BulkWriters(IEnumerable<AggregateDocument> documents, IReadOnlyDictionary<string, long> categoryCounts, string trackedGroup,
        int writers, long documentsPerWriter, int seed)
    {
        var baseline = categoryCounts[trackedGroup];
        var room = categoryCounts
            .Where(c => AggregateOrdering.KeyComparer.Equals(c.Key, trackedGroup) == false && baseline - c.Value > 1)
            .OrderByDescending(c => baseline - c.Value).ThenBy(c => c.Key, AggregateOrdering.KeyComparer)
            .ToDictionary(c => c.Key, c => baseline - c.Value - 1, AggregateOrdering.KeyComparer);
        if (room.Values.Sum() < writers)
            throw new InvalidOperationException($"The groups below the tracked group '{trackedGroup}' ({baseline} documents) can take {room.Values.Sum()} bulk writer(s), not {writers}; lower writers or raise documentCount.");

        var bulk = new BulkWriter[writers];
        for (int w = 0; w < writers; w++)
        {
            var target = room.Where(r => r.Value > 0).MaxBy(r => r.Value).Key;
            room[target]--;
            bulk[w] = new BulkWriter(target, SeedMixer.Derive(seed, $"bulk-{w}"));
        }

        long dealt = 0;
        foreach (var d in OutsideTracked(documents, trackedGroup).Where((_, i) => i % 2 == 1))
        {
            if (dealt == writers * documentsPerWriter)
                break;
            bulk[dealt++ % writers].Add(d.Id, d.Category);
        }
        if (bulk.Any(b => b.CanMove == false))
            throw new InvalidOperationException($"A bulk writer has no document outside its target group; lower writers or raise documentCount.");
        return bulk;
    }

    private static IEnumerable<AggregateDocument> OutsideTracked(IEnumerable<AggregateDocument> documents, string trackedGroup) =>
        documents.Where(d => AggregateOrdering.KeyComparer.Equals(d.Category, trackedGroup) == false);
}

/// <summary>
/// One bulk writer's updates over documents it alone owns. Odd updates move a document from its group
/// into the target group; even updates move a document of the target group back to the group the
/// previous update emptied. Every update also sets a new amount. The target group is therefore at most
/// one above its start, and no other group ever rises above its start. Not thread-safe; its updates
/// must be sent one at a time, in order.
/// </summary>
public sealed class BulkWriter(string target, int seed)
{
    private readonly Queue<(string Id, string Category)> _outside = new();
    private readonly Queue<string> _inTarget = new();
    private readonly Random _amounts = new(seed);
    private string? _returnTo;

    public string Target => target;

    internal bool CanMove => _outside.Count > 0;

    internal void Add(string id, string category)
    {
        if (AggregateOrdering.KeyComparer.Equals(category, target))
            _inTarget.Enqueue(id);
        else
            _outside.Enqueue((id, category));
    }

    public AggregateUpdateOperation Next()
    {
        string id, category;
        if (_returnTo is null)
        {
            (id, _returnTo) = _outside.Dequeue();
            _inTarget.Enqueue(id);
            category = target;
        }
        else
        {
            id = _inTarget.Dequeue();
            category = _returnTo;
            _outside.Enqueue((id, category));
            _returnTo = null;
        }
        return new AggregateUpdateOperation { Id = id, Category = category, Amount = _amounts.NextInt64(1, AggregateDataSet.MaxAmount + 1) };
    }
}
