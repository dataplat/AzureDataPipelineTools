using DataPipelineTools.Tests.Common;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace DataPipelineTools.Tests.DataLake.Model
{
    [TestFixture]
    [Category(nameof(TestType.UnitTest))]
    public class DataLakeConfigTests
    {
        private const string AccountUri = "mydatalake";
        private const string ContainerName = "mycontainer";
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void BaseUrl_ShouldContain_AccountUri()
        {
            var config = new DataLakeConfig { AccountUri = AccountUri, Container = ContainerName };

            Assert.IsTrue(config.BaseUrl.StartsWith($"https://{AccountUri}"));
        }

        [Test]
        public void BaseUrl_ShouldContain_ContainerName()
        {
            var config = new DataLakeConfig { AccountUri = AccountUri, Container = ContainerName };

            Assert.IsTrue(config.BaseUrl.EndsWith(ContainerName));
        }
    }
}