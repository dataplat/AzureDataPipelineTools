using System;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;

namespace DataPipelineTools.Tests.Common.FilterTests
{
    [TestFixture]
    [Category(nameof(TestType.UnitTest))]
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
                Value = "Someone",
                PropertyType = typeof(string),
                IsValid = true
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
                Value = "Someone",
                PropertyType = typeof(string),
                IsValid = true
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
                Value = "Some*ne",
                PropertyType = typeof(string),
                IsValid = true
            };

            var result = filter.GetDynamicLinqValue();


            Assert.That(result, Is.EqualTo("Some.*ne"));
        }

        [Test]
        public void Given_DateTimeValue_Should_Return_ValueOfTypeDateTime()
        {
            var date = "2021-01-01T00:00:00";
            var filter = new Filter<TestPoco>
            {
                Operator = "like",
                Value = date,
                PropertyType = typeof(DateTime),
                IsValid = true
            };

            var result = filter.GetDynamicLinqValue();


            Assert.That(result, Is.EqualTo(DateTime.Parse(date)));
        }



        [Test]
        public void Given_DateTimeOffsetValue_Should_Return_ValueOfTypeDateTimeOffset()
        {
            var date = "2021-01-01T00:00:00+06:00";
            var filter = new Filter<TestPoco>
            {
                Operator = "like",
                Value = date,
                PropertyType = typeof(DateTimeOffset),
                IsValid = true
            };

            var result = filter.GetDynamicLinqValue();


            Assert.That(result, Is.EqualTo(DateTimeOffset.Parse(date)));
        }



        [Test]
        public void Given_InvalidFilter_Should_Return_Null()
        {
            var filter = new Filter<TestPoco>
            {
                Operator = "like",
                Value = "2021-01-01T00:00:00",
                PropertyType = typeof(DateTime),
                IsValid = false
            };

            var result = filter.GetDynamicLinqValue();

            Assert.That(result, Is.Null);
        }

    }
}