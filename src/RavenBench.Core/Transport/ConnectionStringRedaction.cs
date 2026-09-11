namespace RavenBench.Core.Transport;

/// <summary>
/// Replaces the password of a URI-style connection-string endpoint with a fixed token, so a result
/// records where a run addressed its target without carrying the credential. The authority bounds
/// the search, so a credential-free string is returned unchanged even when its query carries an
/// <c>@</c>. One rule serves every product whose endpoint is a connection string.
/// </summary>
internal static class ConnectionStringRedaction
{
    private const string SchemeSeparator = "://";
    private const string RedactionToken = "***";

    internal static string Redact(string connectionString)
    {
        var schemeEnd = connectionString.IndexOf(SchemeSeparator, StringComparison.Ordinal);
        if (schemeEnd < 0)
            return connectionString;

        var userInfoStart = schemeEnd + SchemeSeparator.Length;
        var authorityEnd = connectionString.IndexOfAny(['/', '?', '#'], userInfoStart);
        if (authorityEnd < 0)
            authorityEnd = connectionString.Length;
        if (authorityEnd <= userInfoStart)
            return connectionString;

        var at = connectionString.LastIndexOf('@', authorityEnd - 1);
        if (at < userInfoStart)
            return connectionString;

        var colon = connectionString.IndexOf(':', userInfoStart, at - userInfoStart);
        if (colon < 0)
            return connectionString;

        return string.Concat(connectionString.AsSpan(0, colon + 1), RedactionToken, connectionString.AsSpan(at));
    }
}
