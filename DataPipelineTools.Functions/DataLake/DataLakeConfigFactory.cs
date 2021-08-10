using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake
{
    public class DataLakeConfigFactory
    {
        // Data lake connection config
        public const string AccountParam = "account";
        public const string ContainerParam = "container";
        public const string ServicePrincipalClientIdParam = "spnClientId";
        public const string ServicePrincipalClientSecretPlaintextParam = "spnClientPlaintext";
        public const string ServicePrincipalClientSecretKeyVaultParam = "spnClient";
        public const string SasTokenPlaintextParam = "sasTokenPlaintext";
        public const string SasTokenKeyVaultParam = "sasToken";
        public const string AccountKeySecretPlaintextParam = "accountKeyPlaintext";
        public const string AccountKeySecretKeyVaultParam = "accountKey";
        public const string KeyVaultParam = "keyVault";

        // Query config
        public const string PathParam = "path";
        //public const string DirectoryParam = "directory";
        public const string IgnoreDirectoryCaseParam = "ignoreDirectoryCase";
        public const string RecursiveParam = "recursive";
        public const string OrderByColumnParam = "orderBy";
        public const string OrderByDescendingParam = "orderByDesc";
        public const string LimitParam = "limit";

        private readonly ILogger _logger;
        public DataLakeConfigFactory(ILogger<DataLakeConfigFactory> logger)
        {
            _logger = logger;
        }

        public DataLakeConnectionConfig GetDataLakeConnectionConfig (HttpRequest req)
        {
            var data = req.GetData();

            var config = new DataLakeConnectionConfig();

            config.Account = req.GetQueryParameter(AccountParam);
            config.Container = req.GetQueryParameter(ContainerParam);
            config.ServicePrincipalClientId = req.GetQueryParameter(ServicePrincipalClientIdParam);
            config.ServicePrincipalClientSecretPlaintext = req.GetQueryParameter(ServicePrincipalClientSecretPlaintextParam);
            config.ServicePrincipalClientSecretKeyVault = req.GetQueryParameter(ServicePrincipalClientSecretKeyVaultParam);
            config.SasTokenPlaintext = req.GetQueryParameter(SasTokenPlaintextParam);
            config.SasTokenKeyVault = req.GetQueryParameter(SasTokenKeyVaultParam);
            config.AccountKeySecretPlaintext = req.GetQueryParameter(AccountKeySecretPlaintextParam);
            config.AccountKeySecretKeyVault =req.GetQueryParameter(AccountKeySecretKeyVaultParam);
            config.KeyVault = req.GetQueryParameter(KeyVaultParam);

            ValidateDataLakeConfig(config);

            return config;
        }
        
        private void ValidateDataLakeConfig(DataLakeConnectionConfig connectionConfig)
        {
            if (string.IsNullOrWhiteSpace(connectionConfig.Account))
                throw new ArgumentException($"Mandatory parameter '{AccountParam}' was not provided.");

            if (string.IsNullOrWhiteSpace(connectionConfig.Container))
                throw new ArgumentException($"Mandatory parameter '{ContainerParam}' was not provided.");

            ValidateSingleAuthTypeSet(connectionConfig);

            if (connectionConfig.AuthType == AuthType.UserServicePrincipal)
                ValidateDataLakeConfigUserServicePrincipalAuth(connectionConfig);

            if (connectionConfig.AuthType == AuthType.SasToken)
                ValidateDataLakeConfigSasTokenAuth(connectionConfig);

            if (connectionConfig.AuthType == AuthType.AccountKey)
                ValidateDataLakeConfigAccountKeyAuth(connectionConfig);
        }


        private void ValidateSingleAuthTypeSet(DataLakeConnectionConfig connectionConfig)
        {
            var isUserServicePrincipal = !string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientId) ||
                                         !string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretKeyVault) ||
                                         !string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretPlaintext);

            var isSasToken = !string.IsNullOrWhiteSpace(connectionConfig.SasTokenKeyVault) ||
                             !string.IsNullOrWhiteSpace(connectionConfig.SasTokenPlaintext);

            var isAccountKey = !string.IsNullOrWhiteSpace(connectionConfig.AccountKeySecretKeyVault) ||
                               !string.IsNullOrWhiteSpace(connectionConfig.AccountKeySecretPlaintext);

            var authTypesCount = new[] {isUserServicePrincipal, isSasToken, isAccountKey}.Count(x => x);
            if (authTypesCount > 1)
                throw new ArgumentException(
                    "Authentication is misconfigured. Authentication params must be one of the following sets\n"
                    + "  - None (for authentication using the Azure Functions Service Principal)"
                    + $"  - {ServicePrincipalClientIdParam}, {ServicePrincipalClientSecretKeyVaultParam}, {KeyVaultParam}"
                    + $"  - {ServicePrincipalClientIdParam}, {ServicePrincipalClientSecretPlaintextParam}"
                    + $"  - {SasTokenKeyVaultParam}, {KeyVaultParam}"
                    + $"  - {SasTokenPlaintextParam}"
                    + $"  - {AccountKeySecretKeyVaultParam}, {KeyVaultParam}"
                    + $"  - {AccountKeySecretPlaintextParam}"
                );
        }



        private void ValidateDataLakeConfigUserServicePrincipalAuth(DataLakeConnectionConfig connectionConfig)
        {
            var genericMessage = $"To use a user defined service principal to connect to the storage account, provide the app/client id in the parameter {ServicePrincipalClientIdParam}, and either the "
                + $"name of an Azure Key Vault and the name of the secret that contains the app/client secret as parameters {KeyVaultParam} & {ServicePrincipalClientSecretKeyVaultParam}, or the app/client "
                + $" secret as plain text in the parameter {ServicePrincipalClientSecretPlaintextParam}. It is best practice to store the secrets in key vault.";

            if (string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientId))
                throw new ArgumentException($"Mandatory parameter '{ServicePrincipalClientIdParam}' was not provided.\n{genericMessage}");

            if (string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretKeyVault) && string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretPlaintext))
                throw new ArgumentException($"One of the parameters {ServicePrincipalClientSecretKeyVaultParam} or {ServicePrincipalClientSecretPlaintextParam} must be provided to use a user defined "
                                            + $"service principal for authentication.\n{genericMessage}");

            if (!string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretKeyVault) && !string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretPlaintext))
                throw new ArgumentException($"Only one of the parameters {ServicePrincipalClientSecretKeyVaultParam} or {ServicePrincipalClientSecretPlaintextParam} can be used.\n{genericMessage}");

            if (!string.IsNullOrWhiteSpace(connectionConfig.ServicePrincipalClientSecretKeyVault) && string.IsNullOrWhiteSpace(connectionConfig.KeyVault))
                throw new ArgumentException($"The parameter {ServicePrincipalClientSecretKeyVaultParam} requires the parameter {KeyVaultParam} to be configured too.\n{genericMessage}");
        }

        private void ValidateDataLakeConfigSasTokenAuth(DataLakeConnectionConfig connectionConfig)
        {
            var genericMessage = $"To use a SAS token to connect to the storage account, provide either the name of an Azure Key Vault and the name of the secret that contains the SAS token as parameters "
                + $"{KeyVaultParam} & {SasTokenKeyVaultParam}, or the app/client secret as plain text in the parameter {SasTokenPlaintextParam}. It is best practice to store the secrets in key vault.";

            if (!string.IsNullOrWhiteSpace(connectionConfig.SasTokenKeyVault) && !string.IsNullOrWhiteSpace(connectionConfig.SasTokenPlaintext))
                throw new ArgumentException($"Only one of the parameters {SasTokenKeyVaultParam} or {SasTokenPlaintextParam} can be used.\n{genericMessage}");

            if (!string.IsNullOrWhiteSpace(connectionConfig.SasTokenKeyVault) && string.IsNullOrWhiteSpace(connectionConfig.KeyVault))
                throw new ArgumentException($"The parameter {SasTokenKeyVaultParam} requires the parameter {KeyVaultParam} to be configured too.\n{genericMessage}");
        }

        private void ValidateDataLakeConfigAccountKeyAuth(DataLakeConnectionConfig connectionConfig)
        {
            var genericMessage = $"To use a storage account key to connect to the storage account, provide either the name of an Azure Key Vault and the name of the secret that contains the storage account key "
                + $"as parameters {KeyVaultParam} & {AccountKeySecretKeyVaultParam}, or the app/client secret as plain text in the parameter {AccountKeySecretPlaintextParam}. It is best practice to store the secrets in key vault.";

            if (!string.IsNullOrWhiteSpace(connectionConfig.AccountKeySecretKeyVault) && !string.IsNullOrWhiteSpace(connectionConfig.AccountKeySecretPlaintext))
                throw new ArgumentException($"Only one of the parameters {AccountKeySecretKeyVaultParam} or {AccountKeySecretPlaintextParam} can be used.\n{genericMessage}");

            if (!string.IsNullOrWhiteSpace(connectionConfig.AccountKeySecretKeyVault) && string.IsNullOrWhiteSpace(connectionConfig.KeyVault))
                throw new ArgumentException($"The parameter {AccountKeySecretKeyVaultParam} requires the parameter {KeyVaultParam} to be configured too.\n{genericMessage}");
        }


        public DataLakeCheckPathCaseConfig GetCheckPathCaseConfig (HttpRequest req)
        {
            var config = new DataLakeCheckPathCaseConfig();

            var data = req.GetData();
            config.Path = req.GetQueryParameter(PathParam);

            return config;
        }

        public DataLakeGetItemsConfig GetItemsConfig (HttpRequest req)
        {
            var config = new DataLakeGetItemsConfig();

            var data = req.GetData();

            bool recursive;
            bool orderByDesc;
            bool ignoreDirectoryCase = true;
            int limit = 0;
            bool.TryParse(req.GetQueryParameter(RecursiveParam), out recursive);
            bool.TryParse(req.GetQueryParameter(OrderByDescendingParam), out orderByDesc);
            bool.TryParse(req.GetQueryParameter(IgnoreDirectoryCaseParam), out ignoreDirectoryCase);
            int.TryParse(req.GetQueryParameter(LimitParam), out limit);

            string path = req.Query[PathParam] != StringValues.Empty || data?.directory  == null ? (string)req.Query[PathParam] : data?.path;
            config.Path = string.IsNullOrWhiteSpace(path?.TrimStart('/')) ? "/" : path?.TrimStart('/');

            config.IgnoreDirectoryCase = ignoreDirectoryCase;
            config.Recursive = recursive;
            config.OrderByColumn = req.Query[OrderByColumnParam] != StringValues.Empty ? (string)req.Query[OrderByColumnParam] : data?.orderBy;
            config.OrderByDescending = orderByDesc;
            config.Limit = limit;

            config.Filters = ParseFilters(req);

            return config;
        }

        

        private IEnumerable<Filter<DataLakeItem>> ParseFilters(HttpRequest req)
        {
            var filters = req.Query.Keys
                            .Where(k => k.StartsWith("filter[") && k.EndsWith("]"))
                            // Clean up the column name by removing the filter[...] parts
                            //.Select(f => f[7..^1])
                            .SelectMany(k => req.Query[k].Select(v => FilterFactory<DataLakeItem>.Create(k[7..^1], v, _logger)))
                            .Where(f => f != null);

            return filters.ToArray();
        }

        
    }

    public static class HttpRequestExtensions
    {
        public static string GetQueryParameter(this HttpRequest req, string parameterName)
        {
            var data = GetRequestDataDictionary(req);
            return (string) req.Query[parameterName] ??
                   data?[parameterName]?.ToString();
        }


        public static dynamic GetData(this HttpRequest req)
        {
            var task = new StreamReader(req.Body).ReadToEndAsync();
            return JsonConvert.DeserializeObject(task.Result);
        }

        private static Dictionary<string, object> GetRequestDataDictionary(HttpRequest req)
        {
            var task = new StreamReader(req.Body).ReadToEndAsync();


            // The body won't be populated unless it's a post, so this most likely fails trying to parse an invalid string. In that case just return null.
            try
            {
                return JObject.Parse(task.Result).ToObject<Dictionary<string, object>>();
            }
            catch
            {
                return null;
            }
        }
    }
}
