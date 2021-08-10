
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using Azure;
using Azure.Core;
using Azure.Storage;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public class DataLakeClientFactory : IDataLakeClientFactory
    {
        private readonly ILogger _logger;
        public DataLakeClientFactory(ILogger<DataLakeClientFactory> logger)
        {
            _logger = logger;
        }

        public DataLakeFileSystemClient GetDataLakeClient(IDataLakeConnectionConfig dataLakeConnectionConfig)
        {
            var client = GetClient(dataLakeConnectionConfig);

            if (client == null || !client.Exists())
                throw new ArgumentException($"Container '{dataLakeConnectionConfig.Container}' not found in storage account '{dataLakeConnectionConfig.Account}'. Check the names are correct, and that access is granted using the authentication method used.");

            return client;
        }

        private DataLakeFileSystemClient GetClient(IDataLakeConnectionConfig dataLakeConnectionConfig)
        {
            switch (dataLakeConnectionConfig)
            {
                case DataLakeFunctionsServicePrincipalConnectionConfig config:
                    return GetClient(config);

                case DataLakeUserServicePrincipalConnectionConfig config:
                    return GetClient(config);

                case DataLakeSasTokenConnectionConfig config:
                    return GetClient(config);

                case DataLakeAccountKeyConnectionConfig config:
                    return GetClient(config);
            }

            // This should never happen
            throw new NotImplementedException("Unknown Authentication Type");
        }


        private DataLakeFileSystemClient GetClient(DataLakeFunctionsServicePrincipalConnectionConfig dataLakeConnectionConfig)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            //
            // Note: The SharedTokenCacheCredential type is excluded as it seems to give auth errors
            var cred = new DefaultAzureCredential(new DefaultAzureCredentialOptions { ExcludeSharedTokenCacheCredential = true });
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");
            
            return new DataLakeFileSystemClient(new Uri(dataLakeConnectionConfig.BaseUrl), cred);
        }

        private DataLakeFileSystemClient GetClient(DataLakeUserServicePrincipalConnectionConfig dataLakeConnectionConfig)
        {
            // Work out how to create a token credential from a cleint id and secret. Do we need the tenant id?
            throw new NotImplementedException();
        }
        private DataLakeFileSystemClient GetClient(DataLakeSasTokenConnectionConfig dataLakeConnectionConfig)
        {
            // Required shared access signature, should be the uri and sas token combined
            //var cred = new AzureSasCredential(dataLakeConnectionConfig.SasToken);
            //_logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            //return new DataLakeFileSystemClient(new Uri(dataLakeConnectionConfig.BaseUrl), cred);

            throw new NotImplementedException();
        }
        private DataLakeFileSystemClient GetClient(DataLakeAccountKeyConnectionConfig dataLakeConnectionConfig)
        {
            var cred = new StorageSharedKeyCredential(dataLakeConnectionConfig.Account, dataLakeConnectionConfig.AccountKeySecret);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(dataLakeConnectionConfig.BaseUrl), cred);
        }
        }
}

