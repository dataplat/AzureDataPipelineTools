using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;
using System.IO;

namespace Azure.Datafactory.Extensions.Functions
{
    public class DataLakeConfig
    {
        public string AccountUri { get; set; }
        public string Container { get; set; }

        public string BaseUrl { get { return $"https://{AccountUri}.dfs.core.windows.net/{Container}"; } }

        private const string AccountUriParam = "accountUri";
        private const string ContainerParam = "container";

        protected ILogger Logger { get; private set; }
        protected dynamic Data { get; private set; }

        protected DataLakeConfig(HttpRequest req, ILogger logger)
        {
            Logger = logger;
            Logger?.LogInformation($"req.GetQueryParameterDictionary(): {JsonConvert.SerializeObject(req.GetQueryParameterDictionary(), Formatting.Indented)}");

            Data = GetRequestData(req);

            AccountUri = req.Query["accountUri"] != StringValues.Empty ? (string)req.Query["accountUri"] : Data?.accountUri;
            Container = req.Query["container"] != StringValues.Empty ? (string)req.Query["container"] : Data?.container;
        }

        public static DataLakeConfig ParseFromRequestBody(HttpRequest req, ILogger logger)
        {
            return new DataLakeConfig(req, logger);
        }

        private static dynamic GetRequestData(HttpRequest req)
        {
            var task = new StreamReader(req.Body).ReadToEndAsync();
            task.Wait(250);
            return JsonConvert.DeserializeObject(task.Result);
        }
    }
}