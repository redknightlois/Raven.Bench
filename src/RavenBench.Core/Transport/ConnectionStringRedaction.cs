using System.Text;

namespace RavenBench.Core.Transport;

/// <summary>
/// Replaces the password of a connection-string endpoint with a fixed token, so a result records
/// where a run addressed its target without carrying the credential. A URI's authority bounds the
/// search, so a credential-free URI is returned unchanged even when its query carries an <c>@</c>.
/// A URI query option whose name contains "password", and a keyword string key whose name
/// contains "password" or equals "pwd", have the value replaced; every other key is copied byte for byte.
/// </summary>
internal static class ConnectionStringRedaction
{
    private const string SchemeSeparator = "://";
    private const string RedactionToken = "***";

    internal static string Redact(string connectionString) =>
        connectionString.Contains(SchemeSeparator, StringComparison.Ordinal)
            ? RedactQueryPasswords(RedactUriPassword(connectionString))
            : RedactKeywordPassword(connectionString);

    private static string RedactUriPassword(string connectionString)
    {
        var schemeEnd = connectionString.IndexOf(SchemeSeparator, StringComparison.Ordinal);
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

    private static string RedactQueryPasswords(string uri)
    {
        var queryStart = uri.IndexOf('?');
        if (queryStart < 0)
            return uri;

        var queryEnd = uri.IndexOf('#', queryStart);
        if (queryEnd < 0)
            queryEnd = uri.Length;

        var options = uri[(queryStart + 1)..queryEnd].Split('&').Select(option =>
        {
            var equals = option.IndexOf('=');
            return equals >= 0 && IsPasswordKey(option.AsSpan(0, equals)) ? string.Concat(option.AsSpan(0, equals + 1), RedactionToken) : option;
        });
        return string.Concat(uri.AsSpan(0, queryStart + 1), string.Join('&', options), uri.AsSpan(queryEnd));
    }

    private static bool IsPasswordKey(ReadOnlySpan<char> name) =>
        name.Contains("password", StringComparison.OrdinalIgnoreCase) || name.Equals("pwd", StringComparison.OrdinalIgnoreCase);

    private static string RedactKeywordPassword(string connectionString)
    {
        var builder = new StringBuilder(connectionString.Length);
        int i = 0;
        while (i < connectionString.Length)
        {
            while (i < connectionString.Length && char.IsWhiteSpace(connectionString[i]))
            {
                builder.Append(connectionString[i]);
                i++;
            }

            if (i >= connectionString.Length)
                break;

            var keyStart = i;
            while (i < connectionString.Length && connectionString[i] != '=' && char.IsWhiteSpace(connectionString[i]) == false)
                i++;
            var name = connectionString.AsSpan(keyStart, i - keyStart);

            // The key, the whitespace around the '=' and the '=' itself are copied unchanged.
            while (i < connectionString.Length && char.IsWhiteSpace(connectionString[i]))
                i++;
            if (i < connectionString.Length && connectionString[i] == '=')
                i++;
            while (i < connectionString.Length && char.IsWhiteSpace(connectionString[i]))
                i++;
            builder.Append(connectionString, keyStart, i - keyStart);

            if (i >= connectionString.Length)
                break;

            var valueStart = i;
            var isQuoted = connectionString[i] == '\'';
            if (isQuoted)
            {
                i++;
                while (i < connectionString.Length && connectionString[i] != '\'')
                {
                    if (connectionString[i] == '\\' && i + 1 < connectionString.Length)
                        i++;
                    i++;
                }
                if (i < connectionString.Length)
                    i++;
            }
            else
            {
                // libpq lets a backslash escape a space inside an unquoted value.
                while (i < connectionString.Length && char.IsWhiteSpace(connectionString[i]) == false)
                {
                    if (connectionString[i] == '\\' && i + 1 < connectionString.Length)
                        i++;
                    i++;
                }
            }

            if (IsPasswordKey(name))
                builder.Append(isQuoted ? "'***'" : RedactionToken);
            else
                builder.Append(connectionString, valueStart, i - valueStart);
        }

        return builder.ToString();
    }
}
