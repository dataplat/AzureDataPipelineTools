using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake;

namespace DataPipelineTools.Functions.Tests.Integration.DataLake
{
    /// <summary>
    /// Base class for integration tests against data lake functions. The test here test the authentication, so are re-usable over
    /// all data lake functions.
    /// </summary>
    public abstract class DataLakeIntegrationTestBase : IntegrationTestBase
    {
        [SetUp]
        public void Setup()
        {
            Logger.LogInformation(
                $"Running tests in {(IsRunningOnCIServer ? "CI" : "local")} environment using Functions App '{FunctionsAppUrl}'");
            Logger.LogInformation($"TestContext.Parameters.Count: {TestContext.Parameters.Count}");

            Logger.LogInformation($"Key Vault Name: {KeyVaultName}");
        }




        [Test]
        public async Task Test_FunctionReturnsError_With_MissingAccountParam()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.IsNotNull(results.error);
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.AccountParamIsMandatory,results.error.ToString());
        }

        [Test]
        public async Task Test_FunctionReturnsError_With_MissingContainerParam()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.IsNotNull(results.error);
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.ContainerParamIsMandatory, results.error.ToString());
        }

        [Test]
        public async Task Test_FunctionIsRunnable_With_FunctionsServicePrincipalAuth()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.KeyVaultParam, KeyVaultName},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.IsNotNull(results.error);
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.UserDefinedServicePrincipalParamsMissing, results.error.ToString());
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
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, clientSecret},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.IsNotNull(results.error);
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.UserDefinedServicePrincipalParamsMissing, results.error.ToString());
        }


        [Test]
        public async Task Test_FunctionReturnsError_With_UserServicePrincipalAuthAndMissingClientId()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.ServicePrincipalClientIdParam, ServicePrincipalName},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.ServicePrincipalClientSecretParam, ServicePrincipalSecretKey},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.SasTokenParam, StorageContainerSasToken},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.KeyVaultParam, KeyVaultName},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.SasTokenParam, sasToken},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.IsNotNull(results.error);
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.SasTokenParamMustHaveValue, results.error.ToString());
        }

        [Test]
        public async Task Test_FunctionReturnsError_With_SasTokenAuthAndInvalidSasToken()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.SasTokenParam, RandomString(20)},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.AccountKeyParam, StorageAccountAccessKey},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.KeyVaultParam, KeyVaultName},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.AccountKeyParam, accountKey},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);
            Assert.IsNotNull(results.error);
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.AccountKeyParamMustHaveValue, results.error.ToString());
        }


        [Test]
        public async Task Test_FunctionReturnsError_With_AccountKeyAuthAndInvalidAccountKeyAuth()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.AccountKeyParam, RandomString(20)},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.PathParam, "/"}
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

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

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

                
                Assert.AreEqual(expectedAuthType, (AuthType) results.authType);
            }
            // If it was a bad request, check the error message is set correctly
            else if (response.StatusCode == HttpStatusCode.BadRequest)
            {
                Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.MultipleAuthTypesUsed, results.error.ToString());
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
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"}
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
                {DataLakeConfigFactory.ContainerParam, containerName},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);
        }
    }
}