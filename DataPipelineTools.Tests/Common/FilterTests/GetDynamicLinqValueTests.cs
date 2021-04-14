using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;

namespace DataPipelineTools.Tests.Common.FilterTests
{
    [TestFixture]
    public class GetDynamicLinqValueTests : TestBase
    {
        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void Given_Value_And_OpertatorIsNotLike_Should_Return_Value()
        {
            var filter = new Filter<TestPoco>
            {
                Operator = "eq",
                Value = "Someone"
            };

            var result = filter.GetDynamicLinqValue();


            Assert.That(result, Is.EqualTo(filter.Value));
        }

        [Test]
        public void Given_ValueWithoutWildcards_And_OpertatorIsLike_Should_Return_Value()
        {
            var filter = new Filter<TestPoco>
            {
                Operator = "like",
                Value = "Someone"
            };

            var result = filter.GetDynamicLinqValue();


            Assert.That(result, Is.EqualTo(filter.Value));
        }

        [Test]
        public void Given_ValueWithWildcards_And_OpertatorIsLike_Should_Return_ValueWithWildcardsReplacedForRegexWildcard()
        {
            var filter = new Filter<TestPoco>
            {
                Operator = "like",
                Value = "Some*ne"
            };

            var result = filter.GetDynamicLinqValue();


            Assert.That(result, Is.EqualTo("Some.+ne"));
        }
    }
}