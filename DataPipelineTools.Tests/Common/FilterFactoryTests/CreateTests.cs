using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Castle.Core.Logging;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;

namespace DataPipelineTools.Tests.Common.FilterFactoryTests
{
    [TestFixture]
    public class CreateTests : TestBase<FilterFactory<TestPoco>>
    {
        [SetUp]
        public void Setup()
        {
            MockLogger.Reset();
            SetupConsoleLogging();
        }
        
        #region Given_ColumNameIsNotValidProperty
        static string[] InvalidColumnNames = { "Some missing property", null, "" };
        
        [TestCaseSource(nameof(InvalidColumnNames))]
        public void Given_ColumNameIsNotValidProperty_Should_ReturnFilterWithIsValidEqualsFalse(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, string.Empty, Logger);

            Assert.That(result.IsValid, Is.EqualTo(false));
        }

        [TestCaseSource(nameof(InvalidColumnNames))]
        public void Given_ColumNameIsNotValidProperty_Should_ReturnFilterWithErrorMessageValue(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, string.Empty, Logger);

            Assert.That(result.ErrorMessage, Is.Not.Null);
        }

        [TestCaseSource(nameof(InvalidColumnNames))]
        public void Given_ColumNameIsNotValidProperty_Should_LogWarningOnce(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, string.Empty, Logger);

            MockLogger.VerifyLogging(LogLevel.Warning, Times.Once());
        }
        #endregion Given_ColumNameIsNotValidProperty



        #region Given_OperatorIsNotValid
        static string[] InvalidOperators = { "eq:", "=5", null, "" };
        
        [TestCaseSource(nameof(InvalidOperators))]
        public void Given_OperatorIsNotValid_Should_ReturnFilterWithIsValidEqualsFalse(string filter)
        {
            var result = FilterFactory<TestPoco>.Create(nameof(TestPoco.StringProp), filter, Logger);

            Assert.That(result.IsValid, Is.EqualTo(false));
        }

        [TestCaseSource(nameof(InvalidOperators))]
        public void Given_OperatorIsNotValid_Should_ReturnFilterWithErrorMessageValue(string filter)
        {
            var result = FilterFactory<TestPoco>.Create(nameof(TestPoco.StringProp), filter, Logger);

            Assert.That(result.ErrorMessage, Is.Not.Null);
        }

        [TestCaseSource(nameof(InvalidOperators))]
        public void Given_OperatorIsNotValid_Should_LogWarningOnce(string filter)
        {
            var result = FilterFactory<TestPoco>.Create(nameof(TestPoco.StringProp), filter, Logger);

            MockLogger.VerifyLogging(LogLevel.Warning, Times.Once());
        }
        #endregion Given_OperatorIsNotValid



        #region Given_ValueDoesNotCastToNamedColumnType
        static readonly string[] ValidNonStringColumnTypes =
        {
            nameof(TestPoco.BoolProp),
            nameof(TestPoco.Int16Prop),
            nameof(TestPoco.IntProp),
            nameof(TestPoco.Int64Prop),
            nameof(TestPoco.DoubleProp),
            nameof(TestPoco.DecimalProp),
            nameof(TestPoco.DateTimeProp)
        };
        private const string ValueDoesNotCastToNamedColumnTypeValue = "eq:abc";

        [TestCaseSource(nameof(ValidNonStringColumnTypes))]
        public void Given_ValueDoesNotCastToNamedColumnType_Should_ReturnFilterWithIsValidEqualsFalse(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, ValueDoesNotCastToNamedColumnTypeValue, Logger);

            Assert.That(result.IsValid, Is.EqualTo(false));
        }

        [TestCaseSource(nameof(InvalidOperators))]
        public void Given_ValueDoesNotCastToNamedColumnType_Should_ReturnFilterWithErrorMessageValue(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, ValueDoesNotCastToNamedColumnTypeValue, Logger);

            Assert.That(result.ErrorMessage, Is.Not.Null);
        }

        [TestCaseSource(nameof(InvalidOperators))]
        public void Given_ValueDoesNotCastToNamedColumnType_Should_LogWarningOnce(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, ValueDoesNotCastToNamedColumnTypeValue, Logger);

            MockLogger.VerifyLogging(LogLevel.Warning, Times.Once());
        }
        #endregion Given_ValueDoesNotCastToNamedColumnType

        
        #region Given_ValidColumnNameAndFilter
        private static readonly Dictionary<string, string> ValidColumnNamesAndValues = new Dictionary<string, string> {
            {nameof(TestPoco.StringProp), "hello"},
            {nameof(TestPoco.BoolProp), "true"},
            {nameof(TestPoco.Int16Prop), "42"},
            {nameof(TestPoco.IntProp), "42"},
            {nameof(TestPoco.Int64Prop), "42"},
            {nameof(TestPoco.DoubleProp), "42.1"},
            {nameof(TestPoco.DecimalProp), "42.1"},
            {nameof(TestPoco.DateTimeProp), "2021-01-01T12:00:00"},
            {nameof(TestPoco.DateTimeOffsetProp), "2021-01-01T12:00:00"}
        };
        private static readonly string[] SimpleFilterTypes = {"eq", "ne", "lt", "gt", "le", "ge" };

        [Combinatorial]
        public void Given_ValidColumnNameAndFilter_Should_ReturnFilterWithIsValidEqualsTrue(
            [ValueSource(nameof(ValidColumnNamesAndValues))] KeyValuePair<string, string> propertyName,
            [ValueSource(nameof(SimpleFilterTypes))] string filter
            )
        {
            var result = FilterFactory<TestPoco>.Create(propertyName.Key, $"{filter}:{propertyName.Value}", Logger);

            Assert.That(result.IsValid, Is.EqualTo(true));
        }

        [Combinatorial]
        public void Given_ValidColumnNameAndFilter_Should_ReturnFilterErrorMessageIsNull(
            [ValueSource(nameof(ValidColumnNamesAndValues))] KeyValuePair<string, string> propertyName,
            [ValueSource(nameof(SimpleFilterTypes))] string filter
        )
        {
            var result = FilterFactory<TestPoco>.Create(propertyName.Key, $"{filter}:{propertyName.Value}", Logger);

            Assert.That(result.ErrorMessage, Is.Null);
        }

        [Combinatorial]
        public void Given_ValidColumnNameAndFilter_Should_LogZeroWarnings(
            [ValueSource(nameof(ValidColumnNamesAndValues))] KeyValuePair<string, string> propertyName,
            [ValueSource(nameof(SimpleFilterTypes))] string filter
        )
        {
            var result = FilterFactory<TestPoco>.Create(propertyName.Key, $"{filter}:{propertyName.Value}", Logger);

            MockLogger.VerifyLogging(LogLevel.Warning, Times.Never());
        }
        #endregion Given_ValidColumnNameAndFilter


        #region Given_LikeFilter
        [Test]
        public void Given_LikeFilterWithStringColumn_Should_ReturnFilterWithIsValidEqualsTrue()
        {
            var result = FilterFactory<TestPoco>.Create(nameof(TestPoco.StringProp), $"like:hello*", Logger);

            Assert.That(result.IsValid, Is.EqualTo(true));
        }

        [Test]
        public void Given_LikeFilterWithStringColumn_Should_ReturnFilterErrorMessageIsNull()
        {
            var result = FilterFactory<TestPoco>.Create(nameof(TestPoco.StringProp), $"like:hello*", Logger);

            Assert.That(result.ErrorMessage, Is.Null);
        }

        [Test]
        public void Given_LikeFilterWithStringColumn_Should_LogZeroWarnings()
        {
            var result = FilterFactory<TestPoco>.Create(nameof(TestPoco.StringProp), $"like:hello*", Logger);

            MockLogger.VerifyLogging(LogLevel.Warning, Times.Never());
        }

        [TestCaseSource(nameof(ValidNonStringColumnTypes))]
        public void Given_LikeFilterWithNonStringColumn_Should_ReturnFilterWithIsValidEqualsFalse(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, $"like:hello*", Logger);

            Assert.That(result.IsValid, Is.EqualTo(false));
        }

        [TestCaseSource(nameof(ValidNonStringColumnTypes))]
        public void Given_LikeFilterWithNonStringColumn_Should_ReturnFilterWithErrorMessageValue(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, $"like:hello*", Logger);

            Assert.That(result.ErrorMessage, Is.Not.Null);
        }

        [TestCaseSource(nameof(ValidNonStringColumnTypes))]
        public void Given_LikeFilterWithNonStringColumn_Should_LogWarningOnce(string propertyName)
        {
            var result = FilterFactory<TestPoco>.Create(propertyName, $"like:hello*", Logger);

            MockLogger.VerifyLogging(LogLevel.Warning, Times.Once());
        }
        #endregion Given_LikeFilter
    }
}