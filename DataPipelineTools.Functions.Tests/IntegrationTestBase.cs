using System;
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


            var keyVaultUri = $"https://{TestContext.Parameters["KeyVaultName"]}.vault.azure.net";
            var client = new SecretClient(new Uri(keyVaultUri), credential);
            var result = client.GetSecretAsync(secretName).Result;

            return result?.Value?.Value;
        }

        

    }
}
