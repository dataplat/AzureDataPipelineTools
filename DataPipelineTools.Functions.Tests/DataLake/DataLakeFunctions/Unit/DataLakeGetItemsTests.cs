//using DataPipelineTools.Tests.Common;
//using Microsoft.Extensions.Logging;
//using NUnit.Framework;

//namespace DataPipelineTools.Functions.Tests.DataLake.DataLakeFunctions.Unit
//{
//    [TestFixture]
//    [Category(nameof(TestType.IntegrationTest))]
//    public class DataLakeGetItemsTests: IntegrationTestBase
//    {
//        [SetUp]
//        public void Setup()
//        {
//            Logger.LogInformation($"Running tests in { (IsRunningOnCIServer ? "CI": "local") } environment");
//        }

//        [Test]
//        public void Test_KeyVaultSecretsAreCorrect()
//        {   
//            var servicePrincipalSecretPlainText = TestContext.Parameters["ServicePrincipalSecret"];
//            var servicePrincipalSecretKeyVault = GetKeyVaultSecretValue(TestContext.Parameters["ServicePrincipalSecretKeyName"]);

//            Assert.AreEqual(servicePrincipalSecretPlainText, servicePrincipalSecretKeyVault);
//            //Assert.Fail("Integration tests not implemented yet.");
//        }
//    }
//}