using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using DataPipelineTools.Tests.Common;
using Flurl.Util;
using Microsoft.VisualStudio.TestPlatform.ObjectModel;
using NUnit.Framework;
using SearchOption = System.IO.SearchOption;

namespace DataPipelineTools.Functions.Tests
{
    /// <summary>
    /// Base class for functions integration tests. Exposes the run settings as properties, along with secrets from either the .runsettings file directly, or
    /// Azure Key Vault as specified by the .runsettings file.
    /// </summary>
    public abstract class IntegrationTestBase : TestBase
    {
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

        protected string FunctionsAppName => UseFunctionsEmulator ? "localhost" : TestContext.Parameters["FunctionsAppName"];
        protected string FunctionsAppUrl => UseFunctionsEmulator ? "http://localhost:7071" : $"https://{TestContext.Parameters["FunctionsAppUrl"]}";
        protected string StorageAccountName => TestContext.Parameters["StorageAccountName"];
        protected string StorageContainerName => TestContext.Parameters["StorageContainerName"];
        protected string KeyVaultName => TestContext.Parameters["KeyVaultName"];
        protected string ServicePrincipalName => TestContext.Parameters["ServicePrincipalName"];
        protected string ApplicationInsightsName => TestContext.Parameters["ApplicationInsightsName"];

        protected abstract string FunctionUri { get; }


        // The properties that we get from Azure Key Vault are cached for reuse
        private string _functionsAppKey;
        protected string FunctionsAppKey
        {
            get
            {
                if (_functionsAppKey == null)
                    _functionsAppKey = TestContext.Parameters["FunctionsAppKey"] ??
                                       GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretFunctionsAppKey"]);

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
                       GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretServicePrincipalSecretKey"]);

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
                       GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretStorageContainerSasToken"]);

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
                       GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretStorageAccountAccessKey"]);

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
                       GetKeyVaultSecretValue(TestContext.Parameters["KeyVaultSecretApplicationInsightsKey"]);

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
        
        protected string GetKeyVaultSecretValue(string secretName)
        {
            var client = GetKeyVaultClient();

            try
            {
                var result = client.GetSecretAsync(secretName).Result;

                return result?.Value?.Value;
            }
            catch (Exception ex)
            {
                throw new SettingsException(
                    $"The key vault {KeyVaultName} is inaccessible or has been deleted. Check your run settings file.\n\nInner Exception Message:\n  {ex.Message.Split('\n').First()}");
            }
        }


        protected IEnumerable<string> GetKeyVaultSecretNames()
        {
            var client = GetKeyVaultClient();
            try
            {
                var results = client.GetPropertiesOfSecrets();
                return results.Select(x => x.Name);
            }
            catch (Exception ex)
            {
                throw new SettingsException(
                    $"The key vault {KeyVaultName} is inaccessible or has been deleted. Check your run settings file.\n\nInner Exception Message:\n  {ex.Message.Split('\n').First()}");
            }
        }

        private SecretClient GetKeyVaultClient()
        {
            /* For some reason the DefaultAzureCredential (SharedTokenCacheCredential / VisualStudioCredential) returns a 403 trying to access the key vault, even when access policies are configured correctly
             * We either use one of the following to authenticate:
             *  - var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions{ ExcludeSharedTokenCacheCredential = true, ExcludeVisualStudioCredential = true});
             *  - var credential = new ChainedTokenCredential(new ManagedIdentityCredential(), new AzureCliCredential());
             *
             * See here for more info: https://docs.microsoft.com/en-us/answers/questions/74848/access-denied-to-first-party-service.html
             */
            var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
                {ExcludeSharedTokenCacheCredential = true, ExcludeVisualStudioCredential = true});

            if (string.IsNullOrWhiteSpace(KeyVaultName))
                throw new ArgumentException("The run setting file does not have a value for 'KeyVaultName'");

            var keyVaultUri = $"https://{KeyVaultName}.vault.azure.net";
            return new SecretClient(new Uri(keyVaultUri), credential);

        }





        #region Azure Functions Local Host
        // We use one time setup and teardown to generate a single instance of the emulator across all classes that implement this base class

        private static object _functionsProcessLock = new object();

        [OneTimeSetUp]
        public void StartFunctionsEmulator()
        {
            // If the run settings is referencing secrets via key vault, make sure we can connect
            if (TestContext.Parameters.Names.Any(x => x.StartsWith("KeyVaultSecret") && !string.IsNullOrWhiteSpace(TestContext.Parameters[x].ToString())))
                GetKeyVaultSecretNames();
            

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
                throw new FileNotFoundException("The Azure Functions Core tools are not installed. Run the functions app locally to install the tools.");

            const string args = "host start";
            string binDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            binDir = binDir.Replace("DataPipelineTools.Functions.Tests", "DataPipelineTools.Functions");

            binDir = @"C:\Users\Niall\src\sqlcollaborative\AzureDataPipelineTools\DataPipelineTools.Functions\bin\Debug";

            ProcessStartInfo hostProcess = new ProcessStartInfo
            {
                FileName = latestToolsVersion,
                Arguments = args,
                WorkingDirectory = binDir,
                CreateNoWindow =  false,
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
