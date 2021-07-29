using DataPipelineTools.Tests;
using DataPipelineTools.Tests.Common;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace DataPipelineTools.Functions.Tests.DataLake.DataLakeFunctions
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    public class DataLakeGetItemsTests: TestBase
    {
        [SetUp]
        public void Setup()
        {
            
        }

        [Test]
        public void Test1()
        {
            base.Logger.LogError("Integration tests not implemented yet.");
            Assert.Fail();
        }
    }
}