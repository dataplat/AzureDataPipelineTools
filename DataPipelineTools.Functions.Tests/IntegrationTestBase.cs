using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SearchOption = System.IO.SearchOption;

namespace DataPipelineTools.Functions.Tests
{
    /// <summary>
    /// Base class for functions integration tests. Exposes the run settings as properties, along with secrets from either the .runsettings file directly, or
    /// Azure Key Vault as specified by the .runsettings file.
    /// </summary>
    public abstract class IntegrationTestBase : TestBase
    {
        protected const string FunctionsAppKeyParam = "Code";

        protected IntegrationTestBase()
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

        protected abstract string FunctionUri { get; }


        // The properties that we get from Azure Key Vault are cached for reuse
        private string _functionsAppKey;

        protected string FunctionsAppKeyName => TestContext.Parameters["KeyVaultSecretFunctionsAppKey"];

        protected string ServicePrincipalSecretKeyName =>
            TestContext.Parameters["KeyVaultSecretServicePrincipalSecretKey"];

        protected string StorageContainerSasTokenName =>
            TestContext.Parameters["KeyVaultSecretStorageContainerSasToken"];

        protected string StorageAccountAccessKeyname => TestContext.Parameters["KeyVaultSecretStorageAccountAccessKey"];
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
                                                   StorageAccountAccessKeyname);

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



        protected async Task<HttpResponseMessage> RunQueryFromParameters(Dictionary<string, string> parameters)
        {
            using var client = new HttpClient();
            var queryParams = HttpUtility.ParseQueryString(string.Empty);
            foreach (var (name, value) in parameters)
                queryParams[name] = value;

            // If we are hitting an actual functions instance, include the app key to authenticate against it
            if (!IsEmulatorRunning)
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




        protected void LogContent(HttpResponseMessage response)
        {
            // Only log the response in debug sessions, no point having all that info for a CI run
#if DEBUG
            var content = response.Content.ReadAsStringAsync().Result;
            var json = JObject.Parse(content).ToString();
            Logger.LogInformation($"Content:\n {json}");
#endif
        }


        protected dynamic GetResultsObject(HttpResponseMessage response)
        {
            var content = response.Content.ReadAsStringAsync().Result;
            return JsonConvert.DeserializeObject(content);
        }



        #region Azure Functions Local Host

        // We use one time setup and teardown to generate a single instance of the emulator across all classes that implement this base class

        private static object _functionsProcessLock = new object();

        [OneTimeSetUp]
        public void StartFunctionsEmulator()
        {
            // If the run settings is referencing secrets via key vault, make sure we can connect
            if (TestContext.Parameters.Names.Any(x =>
                x.StartsWith("KeyVaultSecret") && !string.IsNullOrWhiteSpace(TestContext.Parameters[x].ToString())))
                KeyVaultHelpers.GetKeyVaultSecretNames(KeyVaultName);


            // Start the local functions emulator if required. Use a lock so that multiple test classes inheriting from this base class share a
            // functions emulator instance
            lock (_functionsProcessLock)
            {
                if (UseFunctionsEmulator)
                {
                    if (InstanceCount == 0)
                        StartFunctionsEmulatorInternal();

                    InstanceCount++;
                }
            }
        }

        [OneTimeTearDown]
        public void StopFunctionsEmulator()
        {
            // Once the last instance finishes, stop the local emulator instance if we're using it.
            lock (_functionsProcessLock)
            {
                if (UseFunctionsEmulator)
                {
                    InstanceCount--;

                    if (InstanceCount == 0)
                        StopFunctionsEmulatorInternal();
                }
            }
        }


        protected static bool IsEmulatorRunning => LocalFunctionsHostProcess != null;
        private static Process LocalFunctionsHostProcess { get; set; }
        private static int InstanceCount { get; set; }



        private void StartFunctionsEmulatorInternal()
        {
            if (IsEmulatorRunning)
                return;

            string appData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            string toolsPath = Path.Join(appData, "AzureFunctionsTools", "Releases");

            var toolsVersions = Directory.GetFiles(toolsPath, "func.exe", SearchOption.AllDirectories);
            var latestToolsVersion = toolsVersions.OrderBy(x => x).FirstOrDefault();

            if (latestToolsVersion == null)
                throw new FileNotFoundException(
                    "The Azure Functions Core tools are not installed. Run the functions app locally to install the tools.");

            const string args = "host start";
            string binDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            binDir = binDir.Replace("DataPipelineTools.Functions.Tests", "DataPipelineTools.Functions");

            binDir =
                @"C:\Users\Niall\src\sqlcollaborative\AzureDataPipelineTools\DataPipelineTools.Functions\bin\Debug";

            ProcessStartInfo hostProcess = new ProcessStartInfo
            {
                FileName = latestToolsVersion,
                Arguments = args,
                WorkingDirectory = binDir,
                CreateNoWindow = false,
                WindowStyle = ProcessWindowStyle.Normal
            };

            LocalFunctionsHostProcess = Process.Start(hostProcess);

            // Sleep for 5 seconds to allow the emulated functions app to start
            Thread.Sleep(5000);
        }

        private void StopFunctionsEmulatorInternal()
        {
            if (!IsEmulatorRunning)
                return;

            LocalFunctionsHostProcess.Kill();
            LocalFunctionsHostProcess.WaitForExit();
            LocalFunctionsHostProcess.Dispose();
        }

        #endregion Azure Functions Local Host
    }
}