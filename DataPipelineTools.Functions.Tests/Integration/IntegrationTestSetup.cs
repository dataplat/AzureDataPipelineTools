using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using NUnit.Framework.Internal;

namespace DataPipelineTools.Functions.Tests.Integration
{
    /// <summary>
    /// We use one time setup and teardown to generate a single instance of the emulator across all classes in the base test project namespace.
    /// </summary>
    [SetUpFixture]
    public class IntegrationTestSetup
    {
        public bool UseFunctionsEmulator
        {
            get
            {
                bool.TryParse(TestContext.Parameters["UseFunctionsEmulator"], out var result);
                return result;
            }
        }

        protected string KeyVaultName => TestContext.Parameters["KeyVaultName"];
        private static Process LocalFunctionsHostProcess { get; set; }

        public static IEnumerable<ITest> GetDescendants(ITest test)
            => test.Tests.Concat(test.Tests.SelectMany(GetDescendants));


        [OneTimeSetUp]
        public void RunBeforeAnyTests()
        {
            if (TestContext.Parameters.Count == 0)
                throw new ArgumentException("No setting file is configured for the integration tests.");
            
            if (UseFunctionsEmulator)
                StartFunctionsEmulator();
        }

        [OneTimeTearDown]
        public void RunAfterAnyTests()
        {
            // Once the tests finish, stop the local emulator instance if we're using it.
            if (LocalFunctionsHostProcess is { HasExited: false })
            {
                LocalFunctionsHostProcess.Kill();
                LocalFunctionsHostProcess.WaitForExit();
            }

            LocalFunctionsHostProcess?.Dispose();
        }

        private void StartFunctionsEmulator()
        {
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
            
            // The relative path is something like 'DataPipelineTools.Functions.Tests\bin\Debug\netcoreapp3.1', but the func.exe
            // invocation does not like the 'netcoreapp3.1' bit on the end, so strip it off
            binDir = Path.GetDirectoryName(binDir);

            ProcessStartInfo hostProcess = new ProcessStartInfo
            {
                FileName = latestToolsVersion,
                Arguments = args,
                WorkingDirectory = binDir,
                CreateNoWindow = false,
                WindowStyle = ProcessWindowStyle.Normal,
                UseShellExecute = false


            };

            LocalFunctionsHostProcess = Process.Start(hostProcess);

            // Sleep for 5 seconds to allow the emulated functions app to start
            Thread.Sleep(5000);

            // Throw if the functions app fails to start, rather than each test giving a error that the host is unreachable
            if (LocalFunctionsHostProcess?.HasExited == true)
                throw new Exception( $"Functions host failed to start");
        }
        
        
    }
}
