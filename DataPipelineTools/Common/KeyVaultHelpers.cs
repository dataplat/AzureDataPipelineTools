﻿using Azure.Security.KeyVault.Secrets;
using System;
using System.Collections.Generic;
using System.Linq;
using Azure;
using Azure.Core;
using Microsoft.Extensions.Options;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;

namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    public class KeyVaultHelpers
    {
        /// <summary>
        /// Must have a constructor explicitly defined or the DI container does not create the object.
        /// </summary>
        public KeyVaultHelpers()
        {
        }

        public string GetKeyVaultSecretValue(string keyVaultName, string secretName)
        {
            var client = GetKeyVaultClient(keyVaultName);
            return GetKeyVaultSecretValue(client, keyVaultName, secretName);
        }

        public string GetKeyVaultSecretValue(SecretClient client, string keyVaultName, string secretName)
        {
            try
            {
                var result = client.GetSecretAsync(secretName).Result;

                return result?.Value?.Value;
            }
            catch (Exception ex)
            {
                throw new RequestFailedException(
                    $"The key vault {keyVaultName} is inaccessible or has been deleted. Check your run settings file.\n\nInner Exception Message:\n  {ex.Message.Split('\n').First()}");
            }
        }

        public IEnumerable<string> GetKeyVaultSecretNames(string keyVaultName)
        {
            var client = GetKeyVaultClient(keyVaultName);
            try
            {
                var results = client.GetPropertiesOfSecrets();
                return results.Select(x => x.Name);
            }
            catch (Exception ex)
            {
                throw new RequestFailedException(
                    $"The key vault {keyVaultName} is inaccessible or has been deleted. Check your run settings file.\n\nInner Exception Message:\n  {ex.Message.Split('\n').First()}");
            }
        }

        public SecretClient GetKeyVaultClient(string keyVaultName)
        {
            // Exclude VisualStudioCredentials from the options or it does not work when debugging locally. See the function comment for more info.
            var cred = AzureIdentityHelper.GetDefaultAzureCredential(true);
            return GetKeyVaultClient(cred, keyVaultName);

        }

        public SecretClient GetKeyVaultClient(TokenCredential cred, string keyVaultName)
        {
            if (string.IsNullOrWhiteSpace(keyVaultName))
                throw new ArgumentException("The value for parameter 'keyVaultName' cannot be a null, empty or whitespace string");

            var keyVaultUri = $"https://{keyVaultName}.vault.azure.net";
            return new SecretClient(new Uri(keyVaultUri), cred);
        }
    }
}
