using System;
using Azure.Core;
using Azure.Identity;

namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    public static class AzureIdentityHelper
    {
        public static string TenantId =>
            Environment.GetEnvironmentVariable("TENANT_ID") ?? "00000000-0000-0000-0000-000000000000";

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
