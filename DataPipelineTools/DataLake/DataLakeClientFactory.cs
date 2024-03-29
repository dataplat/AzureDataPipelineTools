﻿
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using Azure;
using Azure.Identity;
using Azure.Storage;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using Microsoft.Extensions.Options;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public class DataLakeClientFactory : IDataLakeClientFactory
    {
        private readonly ILogger _logger;
        private readonly KeyVaultHelpers _keyVaultHelper;
        private readonly AzureIdentityHelper _azureIdentityHelper;

        public DataLakeClientFactory(ILogger<DataLakeClientFactory> logger, KeyVaultHelpers keyVaultHelper, AzureIdentityHelper azureIdentityHelper)
        {
            _logger = logger;
            _keyVaultHelper = keyVaultHelper;
            _azureIdentityHelper = azureIdentityHelper;
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
            // If we have an Azure Key Vault reference, we get the actual secret from there
            var secret = string.IsNullOrWhiteSpace(connectionConfig.KeyVault)
                ? connectionConfig.ServicePrincipalClientSecret
                : _keyVaultHelper.GetKeyVaultSecretValue(connectionConfig.KeyVault, connectionConfig.ServicePrincipalClientSecret);

            var cred = new ClientSecretCredential(_azureIdentityHelper.TenantId, connectionConfig.ServicePrincipalClientId, secret);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);
        }
        private DataLakeFileSystemClient GetClient(DataLakeSasTokenConnectionConfig connectionConfig)
        {
            // If we have an Azure Key Vault reference, we get the actual secret from there
            var secret = string.IsNullOrWhiteSpace(connectionConfig.KeyVault)
                ? connectionConfig.SasToken
                : _keyVaultHelper.GetKeyVaultSecretValue(connectionConfig.KeyVault, connectionConfig.SasToken);

            var cred = new AzureSasCredential(secret);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);

            throw new NotImplementedException();
        }
        private DataLakeFileSystemClient GetClient(DataLakeAccountKeyConnectionConfig connectionConfig)
        {
            // If we have an Azure Key Vault reference, we get the actual secret from there
            var secret = string.IsNullOrWhiteSpace(connectionConfig.KeyVault)
                ? connectionConfig.AccountKey
                : _keyVaultHelper.GetKeyVaultSecretValue(connectionConfig.KeyVault, connectionConfig.AccountKey);

            var cred = new StorageSharedKeyCredential(connectionConfig.Account, secret);
            _logger.LogInformation($"Using credential Type: {cred.GetType().Name}");

            return new DataLakeFileSystemClient(new Uri(connectionConfig.BaseUrl), cred);
        }
        }
}

