using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace DataPipelineTools.Functions.Tests.DataLake.DataLakeFunctions.Unit
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    public class DataLakeGetItemsTests: IntegrationTestBase
    {
        [SetUp]
        public void Setup()
        {
            
        }

        [Test]
        public void Test1()
        {
            Logger.LogInformation($"Running tests in { (IsRunningOnCIServer ? "CI": "local") } environment");


            Logger.LogError("Integration tests not implemented yet.");
            Assert.Fail();
        }
    }
}