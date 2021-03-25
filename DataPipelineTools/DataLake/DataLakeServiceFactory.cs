using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    // Using a factory so logging etc is injected vis IoC, but the client credentials can be specified each time
    public class DataLakeServiceFactory
    {
        private readonly ILogger _logger;
        public DataLakeServiceFactory(ILogger<DataLakeServiceFactory> logger)
        : this((ILogger) logger) { }

        public DataLakeServiceFactory(ILogger logger)
        {
            _logger = logger;
        }

        public DataLakeService CreateDataLakeService(DataLakeFileSystemClient client)
        {
            return new DataLakeService(_logger, client);
        }
    }
}
