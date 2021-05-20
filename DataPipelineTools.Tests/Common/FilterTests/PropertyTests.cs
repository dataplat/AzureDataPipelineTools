using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;

namespace DataPipelineTools.Tests.Common.FilterTests
{
    [TestFixture]
    public class PropertyTests : TestBase
    {
        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void Given_IsValid_IsDefault_Should_Return_False()
        {
            var filter = new Filter<TestPoco>();

            Assert.That(filter.IsValid, Is.EqualTo(false));
        }


        [Test]
        public void Given_ErrorMessage_IsDefault_Should_Return_Null()
        {
            var filter = new Filter<TestPoco>();

            Assert.That(filter.ErrorMessage, Is.EqualTo(null));
        }
    }
}