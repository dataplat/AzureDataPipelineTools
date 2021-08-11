
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using Azure;
using Azure.Identity;
using Azure.Storage;
using SqlCollaborative.Azure.DataPipelineTools.Common;

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


        private DataLakeFileSystemClient GetClient(DataLakeFunctionsServicePrincipalConnectionConfig connectionConfig)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            //
            // Note: The SharedTokenCacheCredential type is excluded as it seems to give auth errors
            var cred = AzureIdentityHelper.GetDefaultAzureCredential();
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");
            
            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);
        }

        private DataLakeFileSystemClient GetClient(DataLakeUserServicePrincipalConnectionConfig connectionConfig)
        {
            // If we have an Azure Key Vault reference, we get the actual secert from there
            var secret = string.IsNullOrWhiteSpace(connectionConfig.KeyVault)
                ? connectionConfig.ServicePrincipalClientSecret
                : KeyVaultHelpers.GetKeyVaultSecretValue(connectionConfig.KeyVault, connectionConfig.ServicePrincipalClientSecret);

            var cred = new ClientSecretCredential(AzureIdentityHelper.TenantId, connectionConfig.ServicePrincipalClientId, secret);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);
        }
        private DataLakeFileSystemClient GetClient(DataLakeSasTokenConnectionConfig connectionConfig)
        {
            // Required shared access signature, should be the uri and sas token combined
            var cred = new AzureSasCredential(connectionConfig.SasToken);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);

            throw new NotImplementedException();
        }
        private DataLakeFileSystemClient GetClient(DataLakeAccountKeyConnectionConfig connectionConfig)
        {
            var cred = new StorageSharedKeyCredential(connectionConfig.Account, connectionConfig.AccountKeySecret);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);
        }
        }
}

