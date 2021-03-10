using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Azure.Datafactory.Extensions.Functions
{
    public static class DataLakeClientFactory
    {

        public static DataLakeFileSystemClient GetDataLakeClient(DataLakeConfig settings, ILogger log)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            var credential = new DefaultAzureCredential();
            log.LogInformation($"Using credential Type: {credential.GetType().Name}");

            var client = new DataLakeFileSystemClient(new Uri(settings.BaseUrl), credential);
            
            if (client == null || !client.Exists())
                throw new ArgumentException($"Container '{settings.Container}' not found in storage account '{settings.AccountUri}'. Check the names are correct, and that access is granted to the functions application service principal.");

            return client;
        }
    }
}
