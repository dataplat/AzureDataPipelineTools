using Azure.Datafactory.Extensions.DataLake.Model;
using Azure.Datafactory.Extensions.Functions;
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
using System.Text;
using System.Threading.Tasks;

namespace Azure.Datafactory.Extensions.DataLake
{
    public class DataLakeController
    {
        private readonly ILogger _logger;
        private readonly DataLakeFileSystemClient _client;
        public DataLakeController (ILogger logger, DataLakeFileSystemClient client)
        {
            _logger = logger;
            _client = client;
        }

        public async Task<string> CheckPathAsync(string path, bool isDirectory)
        {
            if (path == null || path.Trim() == "/")
                return null;

            // Check if the path exists with the casing as is...
            var pathExists = isDirectory ?
                                _client.GetDirectoryClient(path).Exists() :
                                _client.GetFileClient(path).Exists();
            if (pathExists)
                return path;

            _logger.LogInformation($"${(isDirectory ? "Directory" : "File")} '${path}' not found, checking paths case using case insensitive compare...");

            // Split the paths so we can test them seperately
            var directoryPath = isDirectory ? path : Path.GetDirectoryName(path).Replace(Path.DirectorySeparatorChar, '/');
            var filename = isDirectory ? null : Path.GetFileName(path);

            // If the directory does not exist, we find it
            string validDirectory = null;
            if (!await _client.GetDirectoryClient(path).ExistsAsync())
            {
                var directoryParts = directoryPath.Split('/');
                foreach (var directoryPart in directoryParts)
                {
                    var searchItem = directoryPart;
                    var validPaths = MatchPathItemsCaseInsensitive(validDirectory, searchItem, true);

                    if (validPaths.Count == 0)
                        return null;
                    else if (validPaths.Count > 1)
                        throw new Exception("Multiple paths matched with case insensitive compare.");

                    validDirectory = validPaths[0];
                }
            }

            if (isDirectory)
                return validDirectory;

            // Now check if the file exists using the corrected directory, and if not find a match...
            var testFilePath = $"{validDirectory ?? ""}/{filename}".TrimStart('/');
            if (_client.GetFileClient(testFilePath).Exists())
                return testFilePath;

            var files = MatchPathItemsCaseInsensitive(validDirectory, filename, false);
            if (files.Count > 1)
                throw new Exception("Multiple paths matched with case insensitive compare.");
            return files.FirstOrDefault();
        }

        private IList<string> MatchPathItemsCaseInsensitive(string basePath, string searchItem, bool isDirectory)
        {
            var paths = _client.GetPaths(basePath).ToList();
            return paths.Where(p => p.IsDirectory == isDirectory && Path.GetFileName(p.Name).Equals(searchItem, StringComparison.CurrentCultureIgnoreCase))
                         .Select(p => p.Name)
                         .ToList();

        }

        private async Task<JObject> GetItemsAsync(DataLakeConfig dataLakeConfig, DataLakeGetItemsConfig getItemsConfig)
        {
            var directory = getItemsConfig.IgnoreDirectoryCase ?
                                await CheckPathAsync(getItemsConfig.Directory, true) :
                                getItemsConfig.Directory;

            var paramsJsonFragment = GetParamsJsonFragment(dataLakeConfig, getItemsConfig);

            if (!_client.GetDirectoryClient(directory).Exists())
                throw new DirectoryNotFoundException("Directory '{directory} could not be found'");
                //return new BadRequestObjectResult(JObject.Parse($"{{ {paramsJsonFragment}, \"error\": \"Directory '{directory} could not be found'\" }}"));

            var paths = _client
                .GetPaths(path: directory ?? string.Empty, recursive: getItemsConfig.Recursive)
                .Select(p => new DataLakeFile
                {
                    Name = Path.GetFileName(p.Name),
                    Directory = p.IsDirectory.GetValueOrDefault(false) ?
                                p.Name :
                                Path.GetDirectoryName(p.Name).Replace(Path.DirectorySeparatorChar, '/'),
                    FullPath = p.Name,
                    Url = Url.Combine(dataLakeConfig.BaseUrl, p.Name),
                    IsDirectory = p.IsDirectory.GetValueOrDefault(false),
                    ContentLength = p.ContentLength.GetValueOrDefault(0),
                    LastModified = p.LastModified.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
                })
                .ToList();

            // 1: Filter the results using dynamic LINQ
            foreach (var filter in getItemsConfig.Filters.Where(f => f.IsValid))
            {
                var dynamicLinqQuery = filter.GetDynamicLinqString();
                string dynamicLinqQueryValue = filter.GetDynamicLinqValue();
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
            var versionAttribute = Attribute.GetCustomAttribute(Assembly.GetExecutingAssembly(), typeof(AssemblyInformationalVersionAttribute)) as AssemblyInformationalVersionAttribute;

            var isEveryFilterValid = getItemsConfig.Filters.All(f => f.IsValid);
            if (!isEveryFilterValid)
                //throw InvalidFilterException()
                throw new InvalidFilterCriteriaException("Som filters are not valid");


            var filesListJson = isEveryFilterValid ?
                                     $"\"fileCount\": {paths.Count}," +
                                     $"\"files\": {JsonConvert.SerializeObject(paths, Formatting.Indented)}" :
                                     string.Empty;

            var resultJson = $"{{ {paramsJsonFragment}, {(getItemsConfig.IgnoreDirectoryCase && directory != getItemsConfig.Directory ? $"\"correctedFilePath\": \"{directory}\"," : string.Empty)} {filesListJson} }}";

            //return isEveryFilterValid ?
            //    (IActionResult)new OkObjectResult(JObject.Parse(resultJson)) :
            //    (IActionResult)new BadRequestObjectResult(JObject.Parse(resultJson));
            return JObject.Parse(resultJson);
        }


        private string GetParamsJsonFragment(DataLakeConfig dataLakeConfig, object parameters)
        {
            return $"\"debugInfo\": {AssemblyHelpers.GetAssemblyVersionInfoJson()}," +
                   $"\"storageContainerUrl\": {dataLakeConfig.BaseUrl}," +
                   parameters == null ?
                        string.Empty :
                        $"\"parameters\": {JsonConvert.SerializeObject(parameters, Formatting.Indented, new JsonSerializerSettings { NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore })}";
        }
    }
}
