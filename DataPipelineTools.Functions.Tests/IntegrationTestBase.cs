using System;
using System.Runtime.InteropServices.ComTypes;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace DataPipelineTools.Functions.Tests
{
    /// <summary>
    /// Base class for functions tests. Exposes the 
    /// </summary>
    public abstract class IntegrationTestBase : TestBase
    {
        public IntegrationTestBase()
        {
            if (TestContext.Parameters.Count == 0)
                throw new ArgumentException("No setting file is configured for the integration tests.");
        }

        protected bool UseFunctionsEmulator
        {
            get
            {
                var result = false;
                bool.TryParse(TestContext.Parameters["UseFunctionsEmulator"], out result);
                return result;
            }
        }

        protected string FunctionsAppName => TestContext.Parameters["FunctionsAppName"];
        protected string FunctionsAppUrl => TestContext.Parameters["FunctionsAppUrl"];
        protected string StorageAccountName => TestContext.Parameters["StorageAccountName"];
        protected string StorageContainerName => TestContext.Parameters["StorageContainerName"];
        protected string KeyVaultName => TestContext.Parameters["KeyVaultName"];
        protected string ServicePrincipalName => TestContext.Parameters["ServicePrincipalName"];
        protected string ApplicationInsightsName => TestContext.Parameters["ApplicationInsightsName"];
        
        
        
        
        protected string FunctionsAppKey => TestContext.Parameters["FunctionsAppKey"] ?? GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretFunctionsAppKey"]);
        protected string ServicePrincipalSecretKey => TestContext.Parameters["ServicePrincipalSecretKey"] ?? GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretServicePrincipalSecretKey"]);
        protected string StorageContainerSasToken => TestContext.Parameters["StorageContainerSasToken"] ?? GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretStorageContainerSasToken"]);
        protected string StorageAccountAccessKey => TestContext.Parameters["StorageAccountAccessKey"] ?? GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretStorageAccountAccessKey"]);
        protected string ApplicationInsightsKey => TestContext.Parameters["ApplicationInsightsKey"] ?? GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretApplicationInsightsKey"]);


        [Test]
        public void Test_RunSettingsLoadedOk()
        {
            Assert.IsNotNull(UseFunctionsEmulator);
            Assert.IsNotNull(FunctionsAppName);
            Assert.IsNotNull(FunctionsAppUrl);
            Assert.IsNotNull(StorageAccountName);
            Assert.IsNotNull(StorageContainerName);
            Assert.IsNotNull(KeyVaultName);
            Assert.IsNotNull(ServicePrincipalName);
            Assert.IsNotNull(ApplicationInsightsName);
            Assert.IsNotNull(FunctionsAppKey);
            Assert.IsNotNull(ServicePrincipalSecretKey);
            Assert.IsNotNull(StorageContainerSasToken);
            Assert.IsNotNull(StorageAccountAccessKey);
            Assert.IsNotNull(ApplicationInsightsKey);

            // Check that the StorageContainerSasToken got unquoted correctly
            Assert.IsFalse(StorageContainerSasToken.Contains("&amp;"));
        }


        protected bool IsRunningOnCIServer
        {
            get
            {
                // Github Actions
                if (Environment.GetEnvironmentVariable("CI") != null)
                    return true;

                //Azure Devops
                if (Environment.GetEnvironmentVariable("TF_BUILD") != null)
                    return true;

                return false;
            }
        }

        protected void StartLocalFunctionsInstance()
        {
            throw new NotImplementedException();
        }

        protected string GetKeyVaultSecretValue(string secretName)
        {
            /* For some reason the DefaultAzureCredential (SharedTokenCacheCredential / VisualStudioCredential) returns a 403 trying to access the key vault, even when access policies are configured correctly
             * We either use one of the following to authenticate:
             *  - var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions{ ExcludeSharedTokenCacheCredential = true, ExcludeVisualStudioCredential = true});
             *  - var credential = new ChainedTokenCredential(new ManagedIdentityCredential(), new AzureCliCredential());
             *
             * See here for more info: https://docs.microsoft.com/en-us/answers/questions/74848/access-denied-to-first-party-service.html
             */
            var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions { ExcludeSharedTokenCacheCredential = true, ExcludeVisualStudioCredential = true });

            if (string.IsNullOrWhiteSpace(KeyVaultName))
                throw new ArgumentException("The run setting file does not have a value for 'KeyVaultName'");

            var keyVaultUri = $"https://{KeyVaultName}.vault.azure.net";
            var client = new SecretClient(new Uri(keyVaultUri), credential);
            var result = client.GetSecretAsync(secretName).Result;

            return result?.Value?.Value;
        }

        

    }
}
