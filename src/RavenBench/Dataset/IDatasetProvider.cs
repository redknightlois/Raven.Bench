using System;

namespace RavenBench.Dataset;

/// <summary>
/// Interface for dataset-specific operations including download, import, and verification.
/// Each dataset (StackOverflow, Northwind, etc.) should implement this interface.
/// </summary>
public interface IDatasetProvider
{
    /// <summary>
    /// The name of this dataset (e.g., "stackoverflow", "northwind").
    /// </summary>
    string DatasetName { get; }

    /// <summary>
    /// Gets the dataset information for a specific size profile or configuration.
    /// </summary>
    DatasetInfo GetDatasetInfo(string? profile = null, int? customSize = null);

    /// <summary>
    /// Determines the target database name based on size profile or custom size.
    /// Different sizes should use different databases to avoid conflicts.
    /// </summary>
    string GetDatabaseName(string? profile = null, int? customSize = null);

    /// <summary>
    /// Checks if this dataset is already imported in the specified database.
    /// Should verify expected collections and document counts exist.
    /// </summary>
    Task<bool> IsDatasetImportedAsync(string serverUrl, string databaseName, int expectedMinDocuments = 1000, Version? httpVersion = null);
}
