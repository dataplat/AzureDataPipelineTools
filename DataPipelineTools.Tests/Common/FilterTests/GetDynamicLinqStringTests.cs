using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;

namespace DataPipelineTools.Tests.Common.FilterTests
{
    [TestFixture]
    [Category(nameof(TestType.UnitTest))]
    public class GetDynamicLinqStringTests : TestBase
    {
        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void Given_Name_And_OpertatorIsNotLike_Should_Return_SimpleDynamicLinqString()
        {
            var filter = new Filter<TestPoco>
            {
                PropertyName = "Name",
                Operator = "eq"
            };

            var result = filter.GetDynamicLinqString();


            Assert.That(result, Is.EqualTo("Name eq @0"));
        }

        [Test]
        public void Given_Name_And_OpertatorIsLike_Should_Return_DynamicLinqStringUsingRegexMatchFunction()
        {
            var filter = new Filter<TestPoco>
            {
                PropertyName = "Name",
                Operator = "like"
            };

            var result = filter.GetDynamicLinqString();

            Assert.That(result, Is.EqualTo("DynamicLinqUtils.IsRegexMatch(Name, @0)"));
        }
    }
}