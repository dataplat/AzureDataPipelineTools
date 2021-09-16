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
        public async Task Given_AccountParamIsMissing_Should_ReturnError()
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
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.AccountParamIsMandatory,results.error?.ToString());
        }

        [Test]
        public async Task Given_ContainerParamIsMissing_Should_ReturnError()
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
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.ContainerParamIsMandatory, results.error?.ToString());
        }

        [Test]
        public async Task Given_NoAuthParameters_Should_RunSuccessfully_WithFunctionsServicePrincipalAuth()
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
        public async Task Given_UserServicePrincipalClientIdAndSecret_Should_RunSuccessfully_WithUserServicePrincipalAuth()
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
        public async Task Given_UserServicePrincipalClientIdAndKeyVaultSecretName_Should_RunSuccessfully_WithUserServicePrincipalAuth()
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
        public async Task Given_NullOrEmptyUserServicePrincipalClientId_Should_ReturnError(string clientId)
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
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.UserDefinedServicePrincipalParamsMissing, results.error?.ToString());
        }

        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Given_NullOrEmptyUserServicePrincipalSecret_Should_ReturnError(
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
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.UserDefinedServicePrincipalParamsMissing, results.error?.ToString());
        }


        [Test]
        public async Task Given_UserServicePrincipalClientIdIsMissing_Should_ReturnError()
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
        public async Task Given_UserServicePrincipalSecretIsMissing_Should_ReturnError()
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
        public async Task Given_UserServicePrincipalClientIdDoesNotExist_Should_ReturnError()
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
        public async Task Given_SasToken_Should_RunSuccessfully_WithSasTokenAuth()
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
        public async Task Given_SasTokenKeyVaultSecretName_Should_RunSuccessfully_WithSasTokenAuth()
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
        public async Task Given_NullOrEmptySasToken_Should_ReturnError(string sasToken)
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
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.SasTokenParamMustHaveValue, results.error?.ToString());
        }

        [Test]
        public async Task Given_InvalidSasToken_Should_ReturnError()
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
        public async Task Given_AccountKey_Should_RunSuccessfully_WithAccountKeyAuth()
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
        public async Task Given_AccountKeyKeyVaultSecretName_Should_RunSuccessfully_WithAccountKeyAuth()
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
        public async Task Given_NullOrEmptyAccountKey_Should_ReturnError(string accountKey)
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
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.AccountKeyParamMustHaveValue, results.error?.ToString());
        }


        [Test]
        public async Task Given_InvalidAccountKey_Should_ReturnError()
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
        [TestCase(true, true, false, HttpStatusCode.BadRequest)]
        [TestCase(true, false, true, HttpStatusCode.BadRequest)]
        [TestCase(false, true, true, HttpStatusCode.BadRequest)]
        [TestCase(true, true, true, HttpStatusCode.BadRequest)]
        public async Task Given_ParametersForMultipleAuthType_Should_ReturnError(bool useUserServicePrincipal,
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
            
            // Check the error message is set correctly
            Assert.AreEqual(DataLakeConfigFactory.ErrorMessage.MultipleAuthTypesUsed, results.error?.ToString());
        }



        [Test]
        [TestCase(null)]
        [TestCase("")]
        [TestCase("  ")]
        public async Task Given_NullOrEmptyAccountName_Should_ReturnError(string accountName)
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
        public async Task Given_NullOrEmptyContainerName_Should_ReturnError(string containerName)
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