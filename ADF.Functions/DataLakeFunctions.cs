using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Flurl;
using System.Linq;
using System.Reflection;
using System.Linq.Dynamic.Core;
using Azure.Storage.Files.DataLake.Models;
using Azure.Datafactory.Extensions.DataLake;
using Azure.Datafactory.Extensions.DataLake.Model;

namespace Azure.Datafactory.Extensions.Functions
{
    public partial class DataLakeFunctions
    {
        private readonly ILogger<DataLakeFunctions> _logger;
        private readonly DataLakeConfigFactory _configFactory;
        public DataLakeFunctions(ILogger<DataLakeFunctions> logger, DataLakeConfigFactory configFactory)
        {
            _logger = logger;
            _configFactory = configFactory;
        }



        [FunctionName("DataLakeGetItems")]
        public async Task<IActionResult> DataLakeGetItems(
            [HttpTrigger(AuthorizationLevel.Function, "get" /*, "post"*/, Route = null)] HttpRequest req)
        {
            req.GetQueryParameterDictionary();

            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            _logger.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");

            try
            {
                var dataLakeConfig = _configFactory.GetDataLakeConfig(req);
                var getItemsConfig = _configFactory.GetItemsConfig(req);

                if (string.IsNullOrWhiteSpace(dataLakeConfig.AccountUri))
                    throw new ArgumentException($"Account Uri '{dataLakeConfig.AccountUri}' not found. Check the URI is correct.");

                var clientFactory = new DataLakeClientFactory(_logger);
                var client = clientFactory.GetDataLakeClient(dataLakeConfig);
                return await GetItemsAsync(client, dataLakeConfig, getItemsConfig, _logger);
            }
            catch (ArgumentException ex)
            {
                _logger.LogError(ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return new BadRequestObjectResult("An error occurred, see the Azure Function logs for more details");
            }
        }



        [FunctionName("DataLakeCheckPathCase")]
        public async Task<IActionResult> DataLakeCheckPathCase(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            _logger.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");

            try
            {
                var dataLakeConfig = _configFactory.GetDataLakeConfig(req);
                var getItemsConfig = _configFactory.GetCheckPathCaseConfig(req);

                if (string.IsNullOrWhiteSpace(dataLakeConfig.AccountUri))
                    throw new ArgumentException($"Account Uri '{dataLakeConfig.AccountUri}' not found. Check the URI is correct.");

                var clientFactory = new DataLakeClientFactory(_logger);
                var client = clientFactory.GetDataLakeClient(dataLakeConfig);

                var paramsJsonFragment = GetParamsJsonFragment(dataLakeConfig, getItemsConfig);
                var validatedPath = await CheckPathAsync(client, getItemsConfig.Path, true, _logger);

                // If multiple files match, the function will throw and the catch block will return a BadRequestObjectResult
                // If the path could not be found as a directory, try for a file...
                validatedPath = validatedPath ?? await CheckPathAsync(client, getItemsConfig.Path, false, _logger);

                var resultJson = "{" +
                                   $"{paramsJsonFragment}, \"validatedPath\":\"{validatedPath}\" " +
                                 "}";

                return validatedPath != null ?
                    (IActionResult)new OkObjectResult(JObject.Parse(resultJson)) :
                    (IActionResult)new NotFoundObjectResult(JObject.Parse(resultJson));
            }
            catch (ArgumentException ex)
            {
                _logger.LogError(ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToString());
                return new BadRequestObjectResult("An error occurred, see the Azure Function logs for more details");
            }
        }






















        private string GetParamsJsonFragment(DataLakeConfig dataLakeConfig, object parameters)
        {
            return $"\"debugInfo\": {AssemblyHelpers.GetAssemblyVersionInfoJson()}," +
                   $"\"storageContainerUrl\": {dataLakeConfig.BaseUrl}," +
                   parameters == null ? 
                        string.Empty :
                        $"\"parameters\": {JsonConvert.SerializeObject(parameters, Formatting.Indented, new JsonSerializerSettings { NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore })}";
        }

        
        private async Task<string> CheckPathAsync(DataLakeFileSystemClient client, string path, bool isDirectory, ILogger log)
        {
            if (path == null || path.Trim() == "/")
                return null;

            // Check if the path exists with the casing as is...
            var pathExists = isDirectory ?
                                client.GetDirectoryClient(path).Exists() :
                                client.GetFileClient(path).Exists();
            if (pathExists)
                return path;

            log.LogInformation($"${(isDirectory ? "Directory" : "File")} '${path}' not found, checking paths case using case insensitive compare...");

            // Split the paths so we can test them seperately
            var directoryPath = isDirectory ? path : Path.GetDirectoryName(path).Replace(Path.DirectorySeparatorChar, '/');
            var filename = isDirectory ? null : Path.GetFileName(path);

            // If the directory does not exist, we find it
            string validDirectory = null;
            if (! await client.GetDirectoryClient(path).ExistsAsync())
            {
                var directoryParts = directoryPath.Split('/');
                foreach (var directoryPart in directoryParts)
                {
                    var searchItem = directoryPart;
                    var validPaths = MatchPathItemsCaseInsensitive(client, validDirectory, searchItem, true, log);

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
            if (client.GetFileClient(testFilePath).Exists())
                return testFilePath;

            var files = MatchPathItemsCaseInsensitive(client, validDirectory, filename, false, log);
            if (files.Count > 1)
                throw new Exception("Multiple paths matched with case insensitive compare.");
            return files.FirstOrDefault();
        }

        private IList<string> MatchPathItemsCaseInsensitive(DataLakeFileSystemClient client, string basePath, string searchItem, bool isDirectory, ILogger log)
        {
            var paths = client.GetPaths(basePath).ToList();
            return paths.Where(p => p.IsDirectory == isDirectory && Path.GetFileName(p.Name).Equals(searchItem, StringComparison.CurrentCultureIgnoreCase))
                         .Select(p => p.Name)
                         .ToList();

        }


        private async Task<IActionResult> GetItemsAsync(DataLakeFileSystemClient client, DataLakeConfig dataLakeConfig, DataLakeGetItemsConfig getItemsConfig, ILogger log)
        {
            var directory = getItemsConfig.IgnoreDirectoryCase ?
                                await CheckPathAsync(client, getItemsConfig.Directory, true, log) :
                                getItemsConfig.Directory;

            var paramsJsonFragment = GetParamsJsonFragment(dataLakeConfig, getItemsConfig);

            if (!client.GetDirectoryClient(directory).Exists())
                return new BadRequestObjectResult(JObject.Parse($"{{ {paramsJsonFragment}, \"error\": \"Directory '{directory} could not be found'\" }}"));

            var paths = client
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
                log.LogInformation($"Applying filter: paths.AsQueryable().Where(\"{dynamicLinqQuery}\", \"{filter.Value}\").ToList()");
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

            var IsEveryFilterValid = getItemsConfig.Filters.All(f => f.IsValid);
            var filesListJson = IsEveryFilterValid ?
                                     $"\"fileCount\": {paths.Count}," +
                                     $"\"files\": {JsonConvert.SerializeObject(paths, Formatting.Indented)}" :
                                     string.Empty;

            var resultJson = $"{{ {paramsJsonFragment}, {(getItemsConfig.IgnoreDirectoryCase && directory != getItemsConfig.Directory ? $"\"correctedFilePath\": \"{directory}\"," : string.Empty)} {filesListJson} }}";

            return IsEveryFilterValid ?
                (IActionResult)new OkObjectResult(JObject.Parse(resultJson)) :
                (IActionResult)new BadRequestObjectResult(JObject.Parse(resultJson));
        }

        
    }
    
       
    

}
