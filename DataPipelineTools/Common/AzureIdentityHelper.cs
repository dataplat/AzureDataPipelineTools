using System;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Options;

namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    public class AzureIdentityHelper
    {
        public AzureIdentityHelper(IOptions<AzureEnvironmentConfig> config)
        {
            Config = config.Value; 
        }

        private AzureEnvironmentConfig Config { get; set; }

        private bool IsRunningLocally =>
            string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID"));

        public string TenantId
        {
            get
            {
                var tenantId = Config?.TenantId ??
                       Environment.GetEnvironmentVariable("TENANT_ID");

                if (tenantId != null)
                    return tenantId;

                var environmentSpecificMessage =
                    $"{(IsRunningLocally ? "Set the value 'TenantId' under the section 'AzureEnvironmentConfig' in the secret.settings.json file, or s" : "S")} et the environment variable 'TENANT_ID' for the Azure Functions app.";
                throw new ApplicationException(
                    $"The application setting TenantId is not configured. {environmentSpecificMessage}");

            }
        }


        public static TokenCredential GetDefaultAzureCredential()
        {
            return GetDefaultAzureCredential(false);
        }

        public static TokenCredential GetDefaultAzureCredential(bool excludeVisualStudioCredential)
        {
            // This works as long as the account accessing (managed identity or visual studio user) has both of the following IAM permissions on the storage account:
            // - Reader
            // - Storage Blob Data Reader
            //
            // Note: The SharedTokenCacheCredential type is excluded as it seems to give auth errors




            /* For some reason I've been having issues with the DefaultAzureCredential for accessing data lake and key vault when debugging in VS
             *
             *  Data lake does not like the SharedTokenCacheCredential being included, so that is excluded by default.
             *
             * Azure Key Vault with DefaultAzureCredential (SharedTokenCacheCredential / VisualStudioCredential included) returns a 403 trying to access the key vault, even when access
             * policies are configured correctly. We can either use one of the following to authenticate:
             *  - var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions{ ExcludeSharedTokenCacheCredential = true, ExcludeVisualStudioCredential = true});
             *  - var credential = new ChainedTokenCredential(new ManagedIdentityCredential(), new AzureCliCredential());
             * It makes more sense to use the DefaultAzureCredential and exclude the VisualStudioCredential, as that is consistent with other services
             * See here for more info: https://docs.microsoft.com/en-us/answers/questions/74848/access-denied-to-first-party-service.html
             *
             * Note: Neither of these workarounds affects the functions running in Azure with their service principal credentials
             */
            return new DefaultAzureCredential(new DefaultAzureCredentialOptions
            {
                ExcludeSharedTokenCacheCredential = true, ExcludeVisualStudioCredential = excludeVisualStudioCredential
            });
        }
    }
}
