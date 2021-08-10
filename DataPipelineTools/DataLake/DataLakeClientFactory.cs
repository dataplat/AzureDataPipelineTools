using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;
using Azure.Core;
using Azure.Storage;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public class DataLakeClientFactory: IDataLakeClientFactory
    {
        private readonly ILogger _logger;
        public DataLakeClientFactory(ILogger<DataLakeClientFactory> logger)
        {
            _logger = logger;
        }

        public DataLakeFileSystemClient GetDataLakeClient(DataLakeConnectionConfig dataLakeConnectionConfig)
        {
            TokenCredential credential = GetCredential(dataLakeConnectionConfig);
            _logger.LogInformation($"Using credential Type: {credential.GetType().Name}");

            var client = new DataLakeFileSystemClient(new Uri(dataLakeConnectionConfig.BaseUrl), credential);

            if (client == null || !client.Exists())
                throw new ArgumentException($"Container '{dataLakeConnectionConfig.Container}' not found in storage account '{dataLakeConnectionConfig.Account}'. Check the names are correct, and that access is granted to the functions application service principal.");

            return client;
        }

        private TokenCredential GetCredential(DataLakeConnectionConfig dataLakeConnectionConfig)
        {
            switch (dataLakeConnectionConfig.AuthType)
            {
                case AuthType.FunctionsServicePrincipal:
                    return GetCredentialForFunctionsServicePrincipal(dataLakeConnectionConfig);

                case AuthType.UserServicePrincipal:
                    return GetCredentialForUserServicePrincipal(dataLakeConnectionConfig);

                case AuthType.SasToken:
                    return GetCredentialForSasToken(dataLakeConnectionConfig);

                case AuthType.AccountKey:
                    return GetCredentialForAccountKey(dataLakeConnectionConfig);

                // This should never happen
                default:
                    throw new NotImplementedException("Unknown Authentication Type");
            }
        }

        private TokenCredential GetCredentialForAccountKey(DataLakeConnectionConfig dataLakeConnectionConfig)
        {
            var cred = new StorageSharedKeyCredential(dataLakeConnectionConfig.Account,
                dataLakeConnectionConfig.AccountKeySecretKeyVault ??
                dataLakeConnectionConfig.AccountKeySecretPlaintext);

            //var client = new DataLakeFileSystemClient(new Uri(dataLakeConnectionConfig.BaseUrl),
            //    );
            throw new NotImplementedException();
        }

        private TokenCredential GetCredentialForSasToken(DataLakeConnectionConfig dataLakeConnectionConfig)
        {
            throw new NotImplementedException();
        }

        private TokenCredential GetCredentialForUserServicePrincipal(DataLakeConnectionConfig dataLakeConnectionConfig)
        {
            throw new NotImplementedException();
        }

        private TokenCredential GetCredentialForFunctionsServicePrincipal(DataLakeConnectionConfig dataLakeConnectionConfig)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            //
            // Note: The SharedTokenCacheCredential type is excluded as it seems to give auth errors
            return new DefaultAzureCredential(new DefaultAzureCredentialOptions { ExcludeSharedTokenCacheCredential = true });
        }
    }
}
