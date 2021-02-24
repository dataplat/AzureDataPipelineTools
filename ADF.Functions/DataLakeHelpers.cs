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

namespace Azure.Datafactory.Extensions.Functions
{
    public static partial class DataLakeHelpers
    {
        [FunctionName("DataLakeGetItems")]
        public static async Task<IActionResult> DataLakeGetItems(
            [HttpTrigger(AuthorizationLevel.Function, "get" /*, "post"*/, Route = null)] HttpRequest req,
            ILogger log)
        {
            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            log.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");

            try
            {
                var settings = DataLakeGetItemsConfig.ParseFromRequestBody(req, log);
                if (string.IsNullOrWhiteSpace(settings.AccountUri))
                    throw new ArgumentException($"Account Uri '{settings.AccountUri}' not found. Check the URI is correct.");

                var client = GetDataLakeClient(settings, log);
                if (client == null || !client.Exists())
                    throw new ArgumentException($"Container '{settings.Container}' not found in storage account '{settings.AccountUri}'. Check the names are correct, and that access is granted to the functions application service principal.");

                return await GetItemsAsync(client, settings, log);
            }
            catch (ArgumentException ex)
            {
                log.LogError(ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
            catch (Exception ex)
            {
                log.LogError(ex.ToString());
                return new BadRequestObjectResult("An error occurred, see the Azure Function logs for more details");
            }
        }



        [FunctionName("DataLakeCheckPathCase")]
        public static async Task<IActionResult> DataLakeCheckPathCase(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            log.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");

            try
            {
                var settings = DataLakeCheckPathCaseConfig.ParseFromRequestBody(req, log);
                if (string.IsNullOrWhiteSpace(settings.AccountUri))
                    throw new ArgumentException($"Account Uri '{settings.AccountUri}' not found. Check the URI is correct.");

                var client = GetDataLakeClient(settings, log);
                if (client == null || ! await client.ExistsAsync())
                    throw new ArgumentException($"Container '{settings.Container}' not found in storage account '{settings.AccountUri}'. Check the names are correct, and that access is granted to the functions application service principal.");

                var paramsJsonFragment = GetParamsJsonFragment(settings);
                var validatedPath = await CheckPathAsync(client, settings.Path, true, log);

                // If multiple files match, the function will throw and the catch block will return a BadRequestObjectResult
                // If the path could not be found as a directory, try for a file...
                validatedPath = validatedPath ?? await CheckPathAsync(client, settings.Path, false, log);

                var resultJson = "{" +
                                   $"{paramsJsonFragment}, \"validatedPath\":\"{validatedPath}\" " +
                                 "}";

                return validatedPath != null ?
                    (IActionResult) new OkObjectResult(JObject.Parse(resultJson)) :
                    (IActionResult) new NotFoundObjectResult(JObject.Parse(resultJson));
            }
            catch (ArgumentException ex)
            {
                log.LogError(ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
            catch (Exception ex)
            {
                log.LogError(ex.ToString());
                return new BadRequestObjectResult("An error occurred, see the Azure Function logs for more details");
            }
        }



        private static string GetParamsJsonFragment(DataLakeConfig settings)
        {
            return $"\"debugInfo\": {AssemblyHelpers.GetAssemblyVersionInfoJson()}," +
                   $"\"parameters\": {JsonConvert.SerializeObject(settings, Formatting.Indented, new JsonSerializerSettings { NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore })}";
        }


        private static DataLakeFileSystemClient GetDataLakeClient(DataLakeConfig settings, ILogger log)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            var credential = new DefaultAzureCredential();
            log.LogInformation($"Using credential Type: {credential.GetType().Name}");

            var client = new DataLakeFileSystemClient(new Uri(settings.BaseUrl), credential);
            if (!client.Exists())
                return null;

            return client;
        }

        
        private async static Task<string> CheckPathAsync(DataLakeFileSystemClient client, string path, bool isDirectory, ILogger log)
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

        private static IList<string> MatchPathItemsCaseInsensitive(DataLakeFileSystemClient client, string basePath, string searchItem, bool isDirectory, ILogger log)
        {
            var paths = client.GetPaths(basePath).ToList();
            return paths.Where(p => p.IsDirectory == isDirectory && Path.GetFileName(p.Name).Equals(searchItem, StringComparison.CurrentCultureIgnoreCase))
                         .Select(p => p.Name)
                         .ToList();

        }


        private static async Task<IActionResult> GetItemsAsync(DataLakeFileSystemClient client, DataLakeGetItemsConfig settings, ILogger log)
        {
            var directory = settings.IgnoreDirectoryCase ?
                                await CheckPathAsync(client, settings.Directory, true, log) :
                                settings.Directory;

            var paramsJsonFragment = GetParamsJsonFragment(settings);

            if (!client.GetDirectoryClient(directory).Exists())
                return new BadRequestObjectResult(JObject.Parse($"{{ {paramsJsonFragment}, \"error\": \"Directory '{directory} could not be found'\" }}"));

            var paths = client
                .GetPaths(path: directory ?? string.Empty, recursive: settings.Recursive)
                .Select(p => new DataLakeFile
                {
                    Name = Path.GetFileName(p.Name),
                    Directory = p.IsDirectory.GetValueOrDefault(false) ?
                                p.Name :
                                Path.GetDirectoryName(p.Name).Replace(Path.DirectorySeparatorChar, '/'),
                    FullPath = p.Name,
                    Url = Url.Combine(settings.BaseUrl, p.Name),
                    IsDirectory = p.IsDirectory.GetValueOrDefault(false),
                    ContentLength = p.ContentLength.GetValueOrDefault(0),
                    LastModified = p.LastModified.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
                })
                .ToList();

            // 1: Filter the results using dynamic LINQ
            foreach (var filter in settings.Filters.Where(f => f.IsValid))
            {
                var dynamicLinqQuery = filter.GetDynamicLinqString();
                string dynamicLinqQueryValue = filter.GetDynamicLinqValue();
                log.LogInformation($"Applying filter: paths.AsQueryable().Where(\"{dynamicLinqQuery}\", \"{filter.Value}\").ToList()");
                paths = paths.AsQueryable().Where(dynamicLinqQuery, dynamicLinqQueryValue).ToList();
            }

            // 2: Sort the results
            if (!string.IsNullOrWhiteSpace(settings.OrderByColumn))
            {
                paths = paths.AsQueryable()
                             .OrderBy(settings.OrderByColumn + (settings.OrderByDescending ? " descending" : string.Empty))
                             .ToList();
            }

            // 3: Do a top N if required
            if (settings.Limit > 0 && settings.Limit < paths.Count)
                paths = paths.Take(settings.Limit).ToList();



            // Output the results
            var versionAttribute = Attribute.GetCustomAttribute(Assembly.GetExecutingAssembly(), typeof(AssemblyInformationalVersionAttribute)) as AssemblyInformationalVersionAttribute;

            var IsEveryFilterValid = settings.Filters.All(f => f.IsValid);
            var filesListJson = IsEveryFilterValid ?
                                     $"\"fileCount\": {paths.Count}," +
                                     $"\"files\": {JsonConvert.SerializeObject(paths, Formatting.Indented)}" :
                                     string.Empty;

            var resultJson = $"{{ {paramsJsonFragment}, {(settings.IgnoreDirectoryCase && directory != settings.Directory ? $"\"correctedFilePath\": \"{directory}\"," : string.Empty)} {filesListJson} }}";

            return IsEveryFilterValid ?
                (IActionResult)new OkObjectResult(JObject.Parse(resultJson)) :
                (IActionResult)new BadRequestObjectResult(JObject.Parse(resultJson));
        }

        
    }
    
       
    

}
