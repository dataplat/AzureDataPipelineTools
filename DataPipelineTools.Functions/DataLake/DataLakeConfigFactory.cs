using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake
{
    public class DataLakeConfigFactory
    {
        private const string AccountUriParam = "accountUri";
        private const string ContainerParam = "container";
        private const string PathParam = "path";
        private const string DirectoryParam = "directory";
        private const string IgnoreDirectoryCaseParam = "ignoreDirectoryCase";
        private const string RecursiveParam = "recursive";
        private const string OrderByColumnParam = "orderBy";
        private const string OrderByDescendingParam = "orderByDesc";
        private const string LimitParam = "limit";

        private readonly ILogger _logger;
        public DataLakeConfigFactory(ILogger<DataLakeConfigFactory> logger)
        {
            _logger = logger;
        }

        public DataLakeConfig GetDataLakeConfig (HttpRequest req)
        {
            var config = new DataLakeConfig();
            //_logger.LogInformation($"req.GetQueryParameterDictionary(): {JsonConvert.SerializeObject(req.GetQueryParameterDictionary(), Formatting.Indented)}");

            var data = GetRequestData(req);
            config.AccountUri = req.Query[AccountUriParam] != StringValues.Empty ? (string)req.Query[AccountUriParam] : data?.accountUri;
            config.Container = req.Query[ContainerParam] != StringValues.Empty ? (string)req.Query[ContainerParam] : data?.container;

            return config;
        }

        public DataLakeCheckPathCaseConfig GetCheckPathCaseConfig (HttpRequest req)
        {
            var config = new DataLakeCheckPathCaseConfig();

            var data = GetRequestData(req);
            config.Path = req.Query[PathParam] != StringValues.Empty ? (string)req.Query[PathParam] : data?.directory;

            return config;
        }

        public DataLakeGetItemsConfig GetItemsConfig (HttpRequest req)
        {
            var config = new DataLakeGetItemsConfig();

            var data = GetRequestData(req);

            bool recursive;
            bool orderByDesc;
            bool ignoreDirectoryCase = true;
            int limit = 0;
            bool.TryParse(req.Query[RecursiveParam] != StringValues.Empty ? (string)req.Query[RecursiveParam] : data?.recursive, out recursive);
            bool.TryParse(req.Query[OrderByDescendingParam] != StringValues.Empty ? (string)req.Query[OrderByDescendingParam] : data?.orderByDesc, out orderByDesc);
            bool.TryParse(req.Query[IgnoreDirectoryCaseParam] != StringValues.Empty ? (string)req.Query[IgnoreDirectoryCaseParam] : data?.ignoreDirectoryCase, out ignoreDirectoryCase);
            int.TryParse(req.Query[LimitParam] != StringValues.Empty ? (string)req.Query[LimitParam] : data?.orderByDesc, out limit);

            string directory = req.Query[DirectoryParam] != StringValues.Empty || data?.directory  == null ? (string)req.Query[DirectoryParam] : data?.directory;
            config.Directory = string.IsNullOrWhiteSpace(directory?.TrimStart('/')) ? "/" : directory?.TrimStart('/');

            config.IgnoreDirectoryCase = ignoreDirectoryCase;
            config.Recursive = recursive;
            config.OrderByColumn = req.Query[OrderByColumnParam] != StringValues.Empty ? (string)req.Query[OrderByColumnParam] : data?.orderBy;
            config.OrderByDescending = orderByDesc;
            config.Limit = limit;

            config.Filters = ParseFilters(req);
            
            return config;
        }

        private IEnumerable<Filter<DataLakeItem>> ParseFilters(HttpRequest req)
        {
            var filters = req.Query.Keys
                            .Where(k => k.StartsWith("filter[") && k.EndsWith("]"))
                            // Clean up the column name by removing the filter[...] parts
                            //.Select(f => f[7..^1])
                            .SelectMany(k => req.Query[k].Select(v => FilterFactory<DataLakeItem>.Create(k[7..^1], v, _logger)))
                            .Where(f => f != null);

            return filters.ToArray();
        }

        private dynamic GetRequestData(HttpRequest req)
        {
            var task = new StreamReader(req.Body).ReadToEndAsync();
            //task.Wait(250);
            return JsonConvert.DeserializeObject(task.Result);
        }
    }
}
