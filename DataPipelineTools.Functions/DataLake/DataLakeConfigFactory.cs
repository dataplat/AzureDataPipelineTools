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
        

        // Query config
        public const string PathParam = "path";
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



        #region Data lake connection config
        // Data lake connection config
        public const string AccountParam = "account";
        public const string ContainerParam = "container";
        public const string ServicePrincipalClientIdParam = "spnClientId";
        public const string ServicePrincipalClientSecretParam = "spnClient";
        public const string SasTokenParam = "sasToken";
        public const string AccountKeyParam = "accountKey";
        public const string KeyVaultParam = "keyVault";

        public IDataLakeConnectionConfig GetDataLakeConnectionConfig(HttpRequest req)
        {
            var parameters = GetParameters(req);
            ValidateParameters(parameters);
            return GetConfig(parameters);
        }


        private static Dictionary<string, QueryParameter> GetParameters(HttpRequest req)
        {
            return new Dictionary<string, QueryParameter>
            {
                { AccountParam, req.GetQueryParameter(AccountParam) },
                { ContainerParam, req.GetQueryParameter(ContainerParam) },
                { ServicePrincipalClientIdParam, req.GetQueryParameter(ServicePrincipalClientIdParam) },
                { ServicePrincipalClientSecretParam, req.GetQueryParameter(ServicePrincipalClientSecretParam)},
                { SasTokenParam, req.GetQueryParameter(SasTokenParam)},
                { AccountKeyParam, req.GetQueryParameter(AccountKeyParam)},
                { KeyVaultParam, req.GetQueryParameter(KeyVaultParam)}
            };


            //return req.Query.Keys.ToDictionary(k => k, req.GetQueryParameter);
        }


        private void ValidateParameters(IReadOnlyDictionary<string, QueryParameter> parameters)
        {
            if (!parameters[AccountParam].Exists || parameters[AccountParam].IsNullOrWhiteSpace)
                throw new ArgumentException($"Mandatory parameter '{AccountParam}' was not provided.");

            if (!parameters[ContainerParam].Exists || parameters[ContainerParam].IsNullOrWhiteSpace)
                throw new ArgumentException($"Mandatory parameter '{ContainerParam}' was not provided.");

            // We either need both params for a user defined service principal, or none
            var userServicePrincipalParams = new[] {parameters[ServicePrincipalClientIdParam], parameters[ServicePrincipalClientSecretParam]};
            if (userServicePrincipalParams.Count(x => x.Exists) == 1 || userServicePrincipalParams.Count(x => x.Exists && x.IsNullOrWhiteSpace) > 0)
                throw new ArgumentException($"To use a user defined service principal you must supply valid values for the the parameters '{ServicePrincipalClientIdParam}' and '{ServicePrincipalClientSecretParam}'");

            // We need zero or one auth types (if nothing is specified we use the functions app service principal)
            var secrets = new[] {parameters[ServicePrincipalClientSecretParam], parameters[SasTokenParam], parameters[AccountKeyParam]};
            if (secrets.Count(x => x.Exists) > 1)
                throw new ArgumentException(
                    "Authentication parameters are invalid. Authentication params must be one of the following sets\n"
                    + "  - None (for authentication using the Azure Functions Service Principal)\n"
                    + $"  - {ServicePrincipalClientIdParam}, {ServicePrincipalClientSecretParam}\n"
                    + $"  - {SasTokenParam}\n"
                    + $"  - {AccountKeyParam}\n"
                );

            // Check any sas token or account key that was passed is not null, empty or whitespace
            if (parameters[SasTokenParam].Exists && parameters[SasTokenParam].IsNullOrWhiteSpace)
                throw new ArgumentException($"Mandatory parameter '{SasTokenParam}' does not have a value.");
            if (parameters[AccountKeyParam].Exists && parameters[AccountKeyParam].IsNullOrWhiteSpace)
                throw new ArgumentException($"Mandatory parameter '{AccountKeyParam}' does not have a value.");


            // If secrets are specified without a ref to a Key Vault, log a warning
            if (secrets.Count(x => x.Exists) != 1 && !parameters[KeyVaultParam].Exists)
                _logger.LogWarning($"The authentication parameters are supplied, but a Azure Key Vault name is not. It is best practice to use Key Vault for storing secret values.");
        }

        private AuthType GetAuthType(IReadOnlyDictionary<string, QueryParameter> parameters)
        {
            if (parameters[ServicePrincipalClientIdParam].Exists)
                return AuthType.UserServicePrincipal;

            if ((parameters[SasTokenParam].Exists))
                return AuthType.SasToken;

            if (parameters[AccountKeyParam].Exists)
                return AuthType.AccountKey;

            return AuthType.FunctionsServicePrincipal;
        }

        private IDataLakeConnectionConfig GetConfig(IReadOnlyDictionary<string, QueryParameter> parameters)
        {
            var authType = GetAuthType(parameters);
            switch (authType)
            {
                case AuthType.FunctionsServicePrincipal:
                    return new DataLakeFunctionsServicePrincipalConnectionConfig
                    {
                        Account = parameters[AccountParam].Value,
                        Container = parameters[ContainerParam].Value
                    };

                case AuthType.UserServicePrincipal:
                    return new DataLakeUserServicePrincipalConnectionConfig()
                    {
                        Account = parameters[AccountParam].Value,
                        Container = parameters[ContainerParam].Value,
                        KeyVault = parameters[KeyVaultParam].Value,
                        ServicePrincipalClientId = parameters[ServicePrincipalClientIdParam].Value,
                        ServicePrincipalClientSecret = parameters[ServicePrincipalClientSecretParam].Value

                    };

                case AuthType.SasToken:
                    return new DataLakeSasTokenConnectionConfig()
                    {
                        Account = parameters[AccountParam].Value,
                        Container = parameters[ContainerParam].Value,
                        KeyVault = parameters[KeyVaultParam].Value,
                        SasToken = parameters[SasTokenParam].Value
                    };

                case AuthType.AccountKey:
                    return new DataLakeAccountKeyConnectionConfig()
                    {
                        Account = parameters[AccountParam].Value,
                        Container = parameters[ContainerParam].Value,
                        KeyVault = parameters[KeyVaultParam].Value,
                        AccountKey = parameters[AccountKeyParam].Value
                    };

                // Should never get here...
                default:
                    throw new NotImplementedException("Unknown authentication type");
            }
        }
        #endregion Data lake connection config





        public DataLakeCheckPathCaseConfig GetCheckPathCaseConfig (HttpRequest req)
        {
            var config = new DataLakeCheckPathCaseConfig();

            var data = req.GetData();
            config.Path = req.GetQueryParameter(PathParam).Value;

            return config;
        }

        public DataLakeGetItemsConfig GetItemsConfig (HttpRequest req)
        {
            var config = new DataLakeGetItemsConfig();
            var data = req.GetData();

            bool.TryParse(req.GetQueryParameter(RecursiveParam).Value, out bool recursive);
            bool.TryParse(req.GetQueryParameter(OrderByDescendingParam).Value, out bool orderByDesc);
            bool.TryParse(req.GetQueryParameter(IgnoreDirectoryCaseParam).Value, out bool ignoreDirectoryCase);
            int.TryParse(req.GetQueryParameter(LimitParam).Value, out int limit);

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
        public static QueryParameter GetQueryParameter(this HttpRequest req, string parameterName)
        {
            var data = GetRequestDataDictionary(req);

            return new QueryParameter
            {
                Name = parameterName,
                Exists = req.Query.TryGetValue(parameterName, out StringValues values),
                Value = values.Count > 0 ? values.ToString()?.Trim() : null
            };

            //return (string) req.Query[parameterName] ??
            //       data?[parameterName]?.ToString().Trim();
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


    public struct QueryParameter
    {
        public string Name { get; set; }
        public string Value { get; set; }
        public bool Exists { get; set; }
        public bool IsNullOrEmpty => string.IsNullOrEmpty(Value);
        public bool IsNullOrWhiteSpace => string.IsNullOrWhiteSpace(Value);

    }
}
