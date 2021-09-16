using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using System.Web;
using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.Functions.Common;

namespace DataPipelineTools.Functions.Tests.Integration
{
    /// <summary>
    /// Base class for functions integration tests. Exposes the run settings as properties, along with secrets from either the .runsettings file directly, or
    /// Azure Key Vault as specified by the .runsettings file.
    /// </summary>
    public abstract class IntegrationTestBase : TestBase
    {
        protected const string FunctionsAppKeyParam = "Code";
        protected readonly String TenantId;
        protected readonly KeyVaultHelpers KeyVaultHelpers;

        protected IntegrationTestBase()
        {
            if (TestContext.Parameters.Count == 0)
                throw new ArgumentException("No setting file is configured for the integration tests.");


            // Build a host using the same config for the real functions app so we can get to the settings and reuse the client for getting key vault secrets.
            // Based on ideas from here: https://saebamini.com/integration-testing-in-azure-functions-with-dependency-injection/
            var startup = new Startup();
            var host = new HostBuilder()
                .ConfigureAppConfiguration((a, b) =>
                {
                    var assemblyPath = Assembly.GetExecutingAssembly().Location;
                    string basePath = Path.GetDirectoryName(assemblyPath);
                    startup.ConfigureAppConfiguration(b, basePath);
                })
                .ConfigureWebJobs(startup.Configure)
                .Build();
            var _identityHelper = host.Services.GetRequiredService<AzureIdentityHelper>();
            TenantId = _identityHelper.TenantId;
            KeyVaultHelpers = host.Services.GetRequiredService<KeyVaultHelpers>();
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

        protected string FunctionsAppName =>
            UseFunctionsEmulator ? "localhost" : TestContext.Parameters["FunctionsAppName"];

        protected string FunctionsAppUrl => UseFunctionsEmulator
            ? "http://localhost:7071"
            : $"https://{TestContext.Parameters["FunctionsAppUrl"]}";

        protected string StorageAccountName => TestContext.Parameters["StorageAccountName"];
        protected string StorageContainerName => TestContext.Parameters["StorageContainerName"];
        protected string KeyVaultName => TestContext.Parameters["KeyVaultName"];
        protected string ServicePrincipalName => TestContext.Parameters["ServicePrincipalName"];
        protected string ApplicationInsightsName => TestContext.Parameters["ApplicationInsightsName"];

        protected string FunctionUri => $"{FunctionsAppUrl}/api/{FunctionRelativeUri}";
        protected abstract string FunctionRelativeUri { get; }


        // The properties that we get from Azure Key Vault are cached for reuse
        private string _functionsAppKey;
        protected string FunctionsAppKeyName => TestContext.Parameters["KeyVaultSecretFunctionsAppKey"];
        protected string ServicePrincipalSecretKeyName => TestContext.Parameters["KeyVaultSecretServicePrincipalSecretKey"];
        protected string StorageContainerSasTokenName => TestContext.Parameters["KeyVaultSecretStorageContainerSasToken"];
        protected string StorageAccountAccessKeyName => TestContext.Parameters["KeyVaultSecretStorageAccountAccessKey"];
        protected string ApplicationInsightsKeyName => TestContext.Parameters["KeyVaultSecretApplicationInsightsKey"];

        protected string FunctionsAppKey
        {
            get
            {
                if (_functionsAppKey == null)
                    _functionsAppKey = TestContext.Parameters["FunctionsAppKey"] ??
                                       KeyVaultHelpers.GetKeyVaultSecretValue(KeyVaultName, FunctionsAppKeyName);

                return _functionsAppKey;
            }
        }


        private string _servicePrincipalSecretKey;

        protected string ServicePrincipalSecretKey
        {
            get
            {
                if (_servicePrincipalSecretKey == null)
                    _servicePrincipalSecretKey = TestContext.Parameters["ServicePrincipalSecretKey"] ??
                                                 KeyVaultHelpers.GetKeyVaultSecretValue(KeyVaultName,
                                                     ServicePrincipalSecretKeyName);

                return _servicePrincipalSecretKey;
            }
        }


        private string _storageContainerSasToken;

        protected string StorageContainerSasToken
        {
            get
            {
                if (_storageContainerSasToken == null)
                    _storageContainerSasToken = TestContext.Parameters["StorageContainerSasToken"] ??
                                                KeyVaultHelpers.GetKeyVaultSecretValue(KeyVaultName,
                                                    StorageContainerSasTokenName);

                return _storageContainerSasToken;
            }
        }


        private string _storageAccountAccessKey;

        protected string StorageAccountAccessKey
        {
            get
            {
                if (_storageAccountAccessKey == null)
                    _storageAccountAccessKey = TestContext.Parameters["StorageAccountAccessKey"] ??
                                               KeyVaultHelpers.GetKeyVaultSecretValue(KeyVaultName,
                                                   StorageAccountAccessKeyName);

                return _storageAccountAccessKey;
            }
        }

        private string _applicationInsightsKey;

        protected string ApplicationInsightsKey
        {
            get
            {
                if (_applicationInsightsKey == null)
                    _applicationInsightsKey = TestContext.Parameters["ApplicationInsightsKey"] ??
                                              KeyVaultHelpers.GetKeyVaultSecretValue(KeyVaultName,
                                                  ApplicationInsightsKeyName);

                return _applicationInsightsKey;
            }
        }


        [Test]
        public void Given_RunSettingsFile_Should_LoadSettingsSuccessfully()
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

            // Check that the StorageContainerSasToken got unquoted correctly from the .runsettings XML file.
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

        /// <summary>
        /// Run a query against the URL specified by the property FunctionUri, using the <paramref name="parameters"/> list.
        /// You should use this overload when using a dictionary does not make sense, such as when a function can take multiple parameters with the same key.
        /// </summary>
        /// <param name="parameters">A dictionary of parameters to use when calling the function</param>
        /// <returns></returns>
        protected async Task<HttpResponseMessage> RunQueryFromParameters(Dictionary<string, string> parameters)
        {
            return await RunQueryFromParameters(parameters.ToList());
        }

        /// <summary>
        /// Run a query against the URL specified by the property FunctionUri, using the <paramref name="parameters"/> dictionary.
        /// </summary>
        /// <param name="parameters">A dictionary of parameters to use when calling the function</param>
        /// <returns></returns>
        protected async Task<HttpResponseMessage> RunQueryFromParameters(List<KeyValuePair<string, string>> parameters)
        {
            using var client = new HttpClient();
            var queryParams = HttpUtility.ParseQueryString(string.Empty);
            foreach (var (name, value) in parameters)
                queryParams[name] = value;

            // If we are hitting an actual functions instance, include the app key to authenticate against it
            if (!UseFunctionsEmulator)
                queryParams[FunctionsAppKeyParam] = FunctionsAppKey;

            var urlBuilder = new UriBuilder(FunctionUri)
            {
                Query = queryParams.ToString() ?? string.Empty
            };
            var queryUrl = urlBuilder.ToString();

            // If we are running locally, log the URL to help with debugging
            if (!IsRunningOnCIServer)
                Logger.LogInformation($"Query URL: {queryUrl}");

            return await client.GetAsync(queryUrl);
        }


        private static Random random = new Random();
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }


        protected void LogContent(HttpResponseMessage response)
        {
            // Only log the response in debug sessions, no point having all that info for a CI run
#if DEBUG
            var content = response?.Content?.ReadAsStringAsync()?.Result;
            if (string.IsNullOrWhiteSpace(content))
                return;

            try
            {

                var json = JObject.Parse(content).ToString();
                Logger.LogInformation($"Content:\n{json}");
            }
            catch
            {
                Logger.LogInformation($"Content:\n  '{content}'");
            }
#endif
        }


        protected dynamic GetResultsObject(HttpResponseMessage response)
        {
            var content = response.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject(content);
        }
    }
}