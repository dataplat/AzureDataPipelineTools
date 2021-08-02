using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace DataPipelineTools.Functions.Tests.DataLake.DataLakeFunctions.Integration
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    public class DataLakeGetItemsIntegrationTests: IntegrationTestBase
    {


        [SetUp]
        public void Setup()
        {
            Logger.LogInformation($"Running tests in { (IsRunningOnCIServer ? "CI" : "local") } environment");
            Logger.LogInformation($"TestContext.Parameters.Count: { TestContext.Parameters.Count }");
        }

        [Test]
        public void Test_KeyVaultSecretsAreCorrect()
        {
            var servicePrincipalSecretPlainText = TestContext.Parameters["ServicePrincipalSecret"];
            var servicePrincipalSecretKeyVault = GetKeyVaultSecretValue(TestContext.Parameters["ServicePrincipalSecretKeyName"]);

            Assert.AreEqual(servicePrincipalSecretPlainText, servicePrincipalSecretKeyVault);
            //Assert.Fail("Integration tests not implemented yet.");
        }
    }
}