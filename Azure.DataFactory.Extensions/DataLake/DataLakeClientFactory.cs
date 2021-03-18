using Azure.Datafactory.Extensions.DataLake.Model;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;

namespace Azure.Datafactory.Extensions.DataLake
{
    public class DataLakeClientFactory: IDataLakeClientFactory
    {
        private readonly ILogger _logger;
        public DataLakeClientFactory(ILogger<DataLakeClientFactory> logger)
        {
            _logger = logger;
        }

        public DataLakeFileSystemClient GetDataLakeClient(DataLakeConfig dataLakeConfig)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            var credential = new DefaultAzureCredential();
            _logger.LogInformation($"Using credential Type: {credential.GetType().Name}");

            var client = new DataLakeFileSystemClient(new Uri(dataLakeConfig.BaseUrl), credential);
            
            if (client == null || !client.Exists())
                throw new ArgumentException($"Container '{dataLakeConfig.Container}' not found in storage account '{dataLakeConfig.AccountUri}'. Check the names are correct, and that access is granted to the functions application service principal.");

            return client;
        }
    }
}
