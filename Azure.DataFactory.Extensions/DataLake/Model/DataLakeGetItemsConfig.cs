using Azure.Datafactory.Extensions.Functions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using System.Collections.Generic;
using System.Linq;

namespace Azure.Datafactory.Extensions.DataLake.Model
{

    public class DataLakeGetItemsConfig : DataLakeConfig
    {
        public string Directory { get; set; }
        public bool IgnoreDirectoryCase { get; set; }
        public bool Recursive { get; set; }
        public string OrderByColumn { get; set; }
        public bool OrderByDescending { get; set; }
        public int Limit { get; set; }
        public IEnumerable<Filter<DataLakeFile>> Filters { get; set; }

        private const string DirectoryParam = "directory";
        private const string IgnoreDirectoryCaseParam = "ignoreDirectoryCase";
        private const string RecursiveParam = "recursive";
        private const string OrderByColumnParam = "orderBy";
        private const string OrderByDescendingParam = "orderByDesc";
        private const string LimitParam = "limit";

        private DataLakeGetItemsConfig(HttpRequest req, ILogger logger) :
            base(req, logger)
        {
            bool recursive;
            bool orderByDesc;
            bool ignoreDirectoryCase = true;
            int limit = 0;
            bool.TryParse(req.Query[RecursiveParam] != StringValues.Empty ? (string)req.Query[RecursiveParam] : Data?.recursive, out recursive);
            bool.TryParse(req.Query[OrderByDescendingParam] != StringValues.Empty ? (string)req.Query[OrderByDescendingParam] : Data?.orderByDesc, out orderByDesc);
            bool.TryParse(req.Query[IgnoreDirectoryCaseParam] != StringValues.Empty ? (string)req.Query[IgnoreDirectoryCaseParam] : Data?.ignoreDirectoryCase, out ignoreDirectoryCase);
            int.TryParse(req.Query[LimitParam] != StringValues.Empty ? (string)req.Query[LimitParam] : Data?.orderByDesc, out limit);


            Directory = req.Query[DirectoryParam] != StringValues.Empty ? (string)req.Query[DirectoryParam] : Data?.directory;
            IgnoreDirectoryCase = ignoreDirectoryCase;
            Recursive = recursive;
            OrderByColumn = req.Query[OrderByColumnParam] != StringValues.Empty ? (string)req.Query[OrderByColumnParam] : Data?.orderBy;
            OrderByDescending = orderByDesc;
            Limit = limit;

            Filters = ParseFilters(req);
        }

        private IEnumerable<Filter<DataLakeFile>> ParseFilters(HttpRequest req)
        {
            var filters = req.Query.Keys
                            .Where(k => k.StartsWith("filter[") && k.EndsWith("]"))
                            .SelectMany(k => req.Query[k].Select(v => Filter<DataLakeFile>.ParseFilter(k, v, Logger)))
                            .Where(f => f != null);

            return filters;
        }

        public static new DataLakeGetItemsConfig ParseFromRequestBody(HttpRequest req, ILogger logger)
        {
            return new DataLakeGetItemsConfig(req, logger);
        }
    }
}