using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;

namespace Azure.Datafactory.Extensions.DataLake.Model
{
    public class DataLakeCheckPathCaseConfig : DataLakeConfig
    {
        public string Path { get; set; }

        private const string PathParam = "path";
        private DataLakeCheckPathCaseConfig(HttpRequest req, ILogger logger) :
            base(req, logger)
        {
            Path = req.Query[PathParam] != StringValues.Empty ? (string)req.Query[PathParam] : Data?.directory;
        }

        public static new DataLakeCheckPathCaseConfig ParseFromRequestBody(HttpRequest req, ILogger logger)
        {
            return new DataLakeCheckPathCaseConfig(req, logger);
        }
    }
}