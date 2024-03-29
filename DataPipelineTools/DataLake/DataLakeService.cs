﻿using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Storage.Files.DataLake;
using Flurl;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Reflection;
using System.Threading.Tasks;
using Newtonsoft.Json.Converters;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public class DataLakeService
    {
        private readonly ILogger _logger;
        private readonly DataLakeFileSystemClient _client;
        internal DataLakeService (ILogger logger, DataLakeFileSystemClient client)
        {
            _logger = logger;
            _client = client;
        }

        public async Task<string> CheckPathAsync(string path, bool isDirectory)
        {
            // If the string is null, empty, whitespace or a single "/" return an empty string so that the client can check a directory for
            // files or concatenate a directory name with a filename etc without an error getting returned
            if (string.IsNullOrWhiteSpace(path) || path.Trim() == "/")
                return string.Empty;

            // Trim any leading or trailing directory separators
            path = path.Trim('/');

            // Check if the path exists with the casing as is...
            var pathExists = isDirectory ?
                                _client.GetDirectoryClient(path).Exists() :
                                _client.GetFileClient(path).Exists();
            if (pathExists)
                return path;

            _logger.LogInformation($"${(isDirectory ? "Path" : "File")} '${path}' not found, checking paths case using case insensitive compare...");

            // Split the paths so we can test them separately
            var directoryPath = isDirectory ? path : Path.GetDirectoryName(path)?.Replace(Path.DirectorySeparatorChar, '/');
            var filename = isDirectory ? null : Path.GetFileName(path);

            // If the directory does not exist, we find it
            string validDirectory = null;
            IEnumerable<string> validDirectories = null;
            if (!await _client.GetDirectoryClient(path).ExistsAsync())
            {
                var validPaths = MatchPaths(null, true,directoryPath.Split('/')).ToList();
                if (validPaths.Count == 0)
                    return null;
                if (validPaths.Count > 1 && isDirectory)
                    throw new MultipleMatchesException(ErrorMessage.MultipleDirectoryMatchesWithCaseInsensitiveCompare);

                validDirectory = validPaths[0];
                validDirectories = validPaths;
            }


            if (isDirectory)
                return validDirectory;

            // Now check if the file exists using the corrected directory, and if not find a match...
            var files = validDirectories.SelectMany(x => MatchPaths(x, false, filename)).ToList();

            if (files.Count > 1)
                throw new MultipleMatchesException(ErrorMessage.MultipleFileMatchesWithCaseInsensitiveCompare);
            return files.FirstOrDefault();
        }

        private IEnumerable<string> MatchPaths(string basePath, bool directoriesOnly, params string[] directoryParts)
        {
            if (directoryParts == null)
                return null;

            if (directoryParts.Count() == 0)
                return new []{ basePath };

            var matchedDirectories = MatchPathItemsCaseInsensitive(basePath, directoryParts.First(), directoriesOnly);
            var matchedChildDirectories = new List<string>();

            foreach (var directory in matchedDirectories)
            {
                var childDirectories = MatchPaths(directory, true, directoryParts.Skip(1).ToArray());
                matchedChildDirectories.AddRange(childDirectories);
            }

            return matchedChildDirectories;
        }

        private IList<string> MatchPathItemsCaseInsensitive(string basePath, string searchItem, bool isDirectory)
        {
            var paths = _client.GetPaths(basePath).ToList();
            return paths.Where(p => p.IsDirectory == isDirectory && Path.GetFileName(p.Name).Equals(searchItem, StringComparison.CurrentCultureIgnoreCase))
                         .Select(p => p.Name)
                         .ToList();

        }

        public async Task<JObject> GetItemsAsync(IDataLakeConnectionConfig dataLakeConnectionConfig, DataLakeGetItemsConfig getItemsConfig)
        {
            // Check the directory exists. If multiple directories match (ie different casing), it will throw an error, as we don't know
            // which one we wanted the files from.
            var directory = getItemsConfig.IgnoreDirectoryCase ?
                                await CheckPathAsync(getItemsConfig.Path, true) :
                                getItemsConfig.Path;

            // Only check the path if it is not the root. Checking is the root exists throws, and if the container is valid the root will always be valid
            if (directory != "/" && !await _client.GetDirectoryClient(directory).ExistsAsync())
                throw new DirectoryNotFoundException(string.Format(ErrorMessage.PathNotFound, directory));

            var paths = _client
                .GetPaths(path: directory ?? string.Empty, recursive: getItemsConfig.Recursive)
                .Select(p => new DataLakeItem
                {
                    Name = Path.GetFileName(p.Name),
                    Directory = Path.GetDirectoryName(p.Name).Replace(Path.DirectorySeparatorChar, '/'),
                    Url = Url.Combine(dataLakeConnectionConfig.BaseUrl, p.Name),
                    IsDirectory = p.IsDirectory.GetValueOrDefault(false),
                    ContentLength = p.ContentLength.GetValueOrDefault(0),
                    LastModified = p.LastModified.ToUniversalTime()
                })
                .ToList();

            // 1: Filter the results using dynamic LINQ
            foreach (var filter in getItemsConfig.Filters.Where(f => f.IsValid))
            {
                var dynamicLinqQuery = filter.GetDynamicLinqString();
                var dynamicLinqQueryValue = filter.GetDynamicLinqValue();
                _logger.LogInformation($"Applying filter: paths.AsQueryable().Where(\"{dynamicLinqQuery}\", \"{filter.Value}\").ToList()");
                paths = paths.AsQueryable().Where(dynamicLinqQuery, dynamicLinqQueryValue).ToList();
            }

            // 2: Sort the results
            if (!string.IsNullOrWhiteSpace(getItemsConfig.OrderByColumn))
            {
                paths = paths.AsQueryable()
                             .OrderBy(getItemsConfig.OrderByColumn + (getItemsConfig.OrderByDescending ? " descending" : string.Empty))
                             .ToList();
            }

            // 3: Do a top N if required
            if (getItemsConfig.Limit > 0 && getItemsConfig.Limit < paths.Count)
                paths = paths.Take(getItemsConfig.Limit).ToList();



            // Output the results
            var isEveryFilterValid = getItemsConfig.Filters.All(f => f.IsValid);
            if (!isEveryFilterValid)
                throw new InvalidFilterCriteriaException("Some filters are not valid");

            var formatter = new IsoDateTimeConverter() {DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffZ"};
            var filesListJson = isEveryFilterValid ?
                                     $"\"fileCount\": {paths.Count}," +
                                     $"\"files\": {JsonConvert.SerializeObject(paths, Formatting.Indented, formatter)}" :
                                     string.Empty;

            var resultJson = $"{{ {(getItemsConfig.IgnoreDirectoryCase && directory != getItemsConfig.Path ? $"\"correctedFilePath\": \"{directory}\"," : string.Empty)} {filesListJson} }}";

            return JObject.Parse(resultJson);
        }

        internal struct ErrorMessage
        {
            internal const string MultipleFileMatchesWithCaseInsensitiveCompare = "Multiple files matched with case insensitive compare.";
            internal const string MultipleDirectoryMatchesWithCaseInsensitiveCompare = "Multiple directories matched with case insensitive compare.";
            internal const string PathNotFound = "Path '{0}' could not be found";

        }
    }
}
