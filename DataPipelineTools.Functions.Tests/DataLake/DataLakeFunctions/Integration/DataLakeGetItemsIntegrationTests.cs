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
    public class DataLakeGetItemsIntegrationTests : IntegrationTestBase
    {
        protected override string FunctionUri => $"{FunctionsAppUrl}/api/DataLakeGetItems";

        [SetUp]
        public void Setup()
        {
            Logger.LogInformation(
                $"Running tests in {(IsRunningOnCIServer ? "CI" : "local")} environment using Functions App '{FunctionsAppUrl}'");
            Logger.LogInformation($"TestContext.Parameters.Count: {TestContext.Parameters.Count}");

            Logger.LogInformation($"Key Vault Name: {KeyVaultName}");
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
            Assert.AreEqual(AuthType.FunctionsServicePrincipal, (AuthType) results.authType);
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
            Assert.AreEqual(AuthType.UserServicePrincipal, (AuthType) results.authType);
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
            Assert.AreEqual(AuthType.UserServicePrincipal, (AuthType) results.authType);
        }


        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Test_FunctionReturnsError_With_UserServicePrincipalAuthAndEmptyClientId(string clientId)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, clientId},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Test_FunctionReturnsError_With_UserServicePrincipalAuthAndEmptyClientSecret(
            string clientSecret)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, ServicePrincipalName},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, clientSecret}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }


        [Test]
        public async Task Test_FunctionReturnsError_With_UserServicePrincipalAuthAndMissingClientId()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task Test_FunctionReturnsError_With_UserServicePrincipalAuthAndMissingClientSecret()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, ServicePrincipalName}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task Test_FunctionReturnsError_With_InvalidUserServicePrincipalAuthAndMissingClientSecret()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, Guid.NewGuid().ToString()},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }










        [Test]
        public async Task Test_FunctionIsRunnable_With_SasTokenAuthAndPlainTextKey()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.SasTokenParam, StorageContainerSasToken}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.SasToken, (AuthType) results.authType);
        }

        [Test]
        public async Task Test_FunctionIsRunnable_With_SasTokenAuthAuthAndKeyVaultKeyName()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.SasTokenParam, StorageContainerSasTokenName},
                {DataLakeConfigFactory.KeyVaultParam, KeyVaultName}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.SasToken, (AuthType) results.authType);
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Test_FunctionReturnsError_With_SasTokenAuthAndEmptySasToken(string sasToken)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.SasTokenParam, sasToken}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

        [Test]
        public async Task Test_FunctionReturnsError_With_SasTokenAuthAndInvalidSasToken()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.SasTokenParam, RandomString(20)}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }









        [Test]
        public async Task Test_FunctionIsRunnable_With_AccountKeyAuthAndPlainTextKey()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.AccountKeyParam, StorageAccountAccessKey}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.AccountKey, (AuthType) results.authType);
        }

        [Test]
        public async Task Test_FunctionIsRunnable_With_AccountKeyAuthAuthAndKeyVaultKeyName()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.AccountKeyParam, StorageAccountAccessKeyName},
                {DataLakeConfigFactory.KeyVaultParam, KeyVaultName}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.AreEqual(AuthType.AccountKey, (AuthType) results.authType);
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Test_FunctionReturnsError_With_AccountKeyAuthAndEmptyAccountKeyAuth(string accountKey)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.AccountKeyParam, accountKey}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }


        [Test]
        public async Task Test_FunctionReturnsError_With_AccountKeyAuthAndInvalidAccountKeyAuth()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.AccountKeyParam, RandomString(20)}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }

















        [Test]
        [TestCase(false, false, false, HttpStatusCode.OK)] // Functions Service Principal Auth
        [TestCase(true, false, false, HttpStatusCode.OK)] // User Service Principal Auth
        [TestCase(false, true, false, HttpStatusCode.OK)] // Sas Token Auth
        [TestCase(false, false, true, HttpStatusCode.OK)] // Account Key Auth
        [TestCase(true, true, false, HttpStatusCode.BadRequest)]
        [TestCase(true, false, true, HttpStatusCode.BadRequest)]
        [TestCase(false, true, true, HttpStatusCode.BadRequest)]
        [TestCase(true, true, true, HttpStatusCode.BadRequest)]
        public async Task Test_FunctionReturnsError_When_MultipleAuthTypesUsed(bool useUserServicePrincipal,
            bool useSasToken, bool useAccountKey, HttpStatusCode expectedResponse)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                //{DataLakeConfigFactory.AccountKeyParam, RandomString(20)}
            };
            if (useUserServicePrincipal)
            {
                parameters.Add(DataLakeConfigFactory.ServicePrincipalClientIdParam, ServicePrincipalName);
                parameters.Add(DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey);
            }

            if (useSasToken)
                parameters.Add(DataLakeConfigFactory.SasTokenParam, StorageContainerSasToken);
            if (useAccountKey)
                parameters.Add(DataLakeConfigFactory.AccountKeyParam, StorageAccountAccessKey);

            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(expectedResponse, response.StatusCode);

            // If the response was ok, check the correct auth type got used
            if (response.StatusCode == HttpStatusCode.OK)
            {
                var expectedAuthType = AuthType.FunctionsServicePrincipal;
                if (useUserServicePrincipal)
                    expectedAuthType = AuthType.UserServicePrincipal;
                else if (useSasToken)
                    expectedAuthType = AuthType.SasToken;
                else if (useAccountKey)
                    expectedAuthType = AuthType.AccountKey;

                // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
                dynamic results = GetResultsObject(response);
                Assert.AreEqual(expectedAuthType, (AuthType) results.authType);
            }
        }



        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Test_FunctionReturnsError_With_EmptyAccountName(string accountName)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, accountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }


        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Test_FunctionReturnsError_With_EmptyContainerName(string containerName)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, containerName}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }
    }
}