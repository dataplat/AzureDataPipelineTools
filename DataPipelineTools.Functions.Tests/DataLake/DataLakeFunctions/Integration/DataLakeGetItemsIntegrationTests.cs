using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace DataPipelineTools.Functions.Tests.DataLake.DataLakeFunctions.Integration
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    [Parallelizable(ParallelScope.Children)]
    public class DataLakeGetItemsIntegrationTests: IntegrationTestBase
    {
        protected override string FunctionUri => $"{FunctionsAppUrl}/api/DataLakeGetItems";

        [SetUp]
        public void Setup()
        {
            Logger.LogInformation($"Running tests in { (IsRunningOnCIServer ? "CI" : "local") } environment using Functions App '{FunctionsAppUrl}'");
            Logger.LogInformation($"TestContext.Parameters.Count: { TestContext.Parameters.Count }");

            Logger.LogInformation($"Key Vault Name: { KeyVaultName }");
        }

        [Test]
        public async Task Test_FunctionIsRunnable_With_FunctionsServicePrincipalAuth()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.FunctionsServicePrincipal, (AuthType)results.authType);
        }

        [Test]
        public async Task Test_FunctionIsRunnable_With_UserServicePrincipalAuthAndPlainTextKey()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, ServicePrincipalName},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey},
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.UserServicePrincipal, (AuthType)results.authType);
        }

        [Test]
        public async Task Test_FunctionIsRunnable_With_UserServicePrincipalAuthAndKeyVaultKeyName()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, ServicePrincipalName},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKeyName},
                {DataLakeConfigFactory.KeyVaultParam, KeyVaultName}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.UserServicePrincipal, (AuthType)results.authType);
        }

    }
}