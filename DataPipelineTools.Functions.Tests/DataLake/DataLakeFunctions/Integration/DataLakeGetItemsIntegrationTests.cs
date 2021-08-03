using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace DataPipelineTools.Functions.Tests.DataLake.DataLakeFunctions.Integration
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    public class DataLakeGetItemsIntegrationTests: IntegrationTestBase
    {
        protected string FunctionUri => $"{FunctionsAppUrl}/api/DataLakeGetItems";

        [SetUp]
        public void Setup()
        {
            Logger.LogInformation($"Running tests in { (IsRunningOnCIServer ? "CI" : "local") } environment using Functions App '{FunctionsAppUrl}'");
            Logger.LogInformation($"TestContext.Parameters.Count: { TestContext.Parameters.Count }");
        }

        [Test]
        public async Task Test_FunctionIsRunnable()
        {
            using (var client = new HttpClient())
            {
                var queryParams = HttpUtility.ParseQueryString(string.Empty);
                queryParams["AccountUri"] = this.StorageAccountName;
                queryParams["container"] = this.StorageContainerName;

                if (!IsEmulatorRunning)
                    queryParams["code"] = this.FunctionsAppKey;

                var urlBuilder = new UriBuilder(FunctionUri)
                {
                    Query = queryParams.ToString() ?? string.Empty
                };
                var queryUrl = urlBuilder.ToString();

                if (!IsRunningOnCIServer)
                    Logger.LogInformation($"Query URL: {queryUrl}");
                
                var result = await client.GetAsync(queryUrl);
                var content = result.Content.ReadAsStringAsync().Result;

                Logger.LogInformation($"Content: {content}");

                Assert.AreEqual(HttpStatusCode.OK, result.StatusCode);
            }
        }
    }
}