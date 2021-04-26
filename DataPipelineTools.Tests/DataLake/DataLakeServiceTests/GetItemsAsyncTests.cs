using System;
using System.Collections.Generic;
using System.Linq;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace DataPipelineTools.Tests.DataLake.DataLakeServiceTests
{
    [TestFixture]
    public class GetItemsAsyncTests : DataLakeTestBase
    {

        protected readonly DataLakeService Sut;
        private DataLakeConfig DatalakeConfig => new DataLakeConfig();

        public GetItemsAsyncTests()
        {
            // Use the factory to inject the mock logger to get the mock client...
            var factory = new DataLakeServiceFactory(MockLogger.Object);
            Sut = factory.CreateDataLakeService(MockFileSystemClient.Object);
        }

        [SetUp]
        public void Setup()
        {
            // Reset the logger for each test, and add a setup to the moq to write log entries to the console so they are captured
            // as additional output in the test result
            MockLogger.Reset();
            SetupConsoleLogging();
        }

        [Test]
        public void Given_ValidDirectoryPath_Should_ReturnContents()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Directory = "raw/api/feb"
            };

            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result;

            Assert.That(result.Count, Is.EqualTo(2));
        }

        private static DataLakeGetItemsConfig[] DirectoryPathWithIncorrectCase =
            {
                new DataLakeGetItemsConfig {Directory = "RAW/api/feb"},
                new DataLakeGetItemsConfig {Directory = "raw/API/feb"},
                new DataLakeGetItemsConfig {Directory = "raw/api/FEB"}
            };
        [TestCaseSource(nameof(DirectoryPathWithIncorrectCase))]
        public void Given_DirectoryPathWithIncorrectCase_Should_ReturnContentsForCorrectedPath(DataLakeGetItemsConfig itemsConfig)
        {
            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(1));
            Assert.That(result.Files.Count, Is.EqualTo(1));
            Assert.That(result.Files.Count(x => x.FullPath == "raw/api/feb/delta_extract_3.json"), Is.EqualTo(1));
        }


        [Test]
        public void Given_InvalidDirectoryPath_Should_ThrowException()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Directory = "some/invalid/path"
            };

            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConfig, itemsConfig));
        }
        
        [Test]
        public void Given_DirectoryPathWithIncorrectCase_When_MatchesMultipleDirectories_Should_ThrowException()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Directory = "RAW/api/JAN"
            };

            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConfig, itemsConfig));
        }



        private static object[] LimitNRecords =
        {
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", Limit = 1}, 1},
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", Limit = 3}, 3},
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", Limit = 5}, 5},
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", Limit = 10}, 5} // There's 5 files for that folder in the test csv
        };
        [TestCaseSource(nameof(LimitNRecords))]
        public void Given_LimitNRecords_Should_ReturnNRecords(DataLakeGetItemsConfig itemsConfig, int expectedResultCount)
        {
            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(expectedResultCount));
        }


        private static object[] OrderByColumn =
        {
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", OrderByColumn = "ContentLength" }, new []{10, 20, 30, 40, 50}}, // Default is ascending order
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = false }, new []{10, 20, 30, 40, 50}},
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = true }, new []{50, 40, 30, 20, 10}}
        };
        [TestCaseSource(nameof(OrderByColumn))]
        public void Given_OrderBy_Should_ReturnRecordsOrderedBySpecifiedColumnWithDirectionSpecifiedByOrderByDescendingFlag(DataLakeGetItemsConfig itemsConfig, int[] expectedContentLengths)
        {
            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(expectedContentLengths.Length));
            for (int i = 0; i < 5; i++)
                Assert.That(result.Files[i].ContentLength, Is.EqualTo(expectedContentLengths[i]));
        }


        private static object[] LimitNRecordsAndOrderByColumn =
        {
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", OrderByColumn = "ContentLength" }, new []{10, 20, 30}}, // Default is ascending order
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = false }, new []{10, 20, 30, 40}},
            new object[] {new DataLakeGetItemsConfig {Directory = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = true }, new []{50, 40}}
        };
        [TestCaseSource(nameof(LimitNRecordsAndOrderByColumn))]
        public void Given_LimitNRecordAndOrderBy_Should_ReturnTopNRecordsOrderedBySpecifiedColumnWithDirectionSpecifiedByOrderByDescendingFlag(DataLakeGetItemsConfig itemsConfig, int[] expectedContentLengths)
        {
            itemsConfig.Limit = expectedContentLengths.Length;

            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(expectedContentLengths.Length));
            for (int i = 0; i < expectedContentLengths.Length; i++)
                Assert.That(result.Files[i].ContentLength, Is.EqualTo(expectedContentLengths[i]));
        }



        [Test]
        public void Given_RecursiveFlagIsTrue_Should_ReturnContentRecursively()
        {
            var itemsConfig = new DataLakeGetItemsConfig {Directory = "raw/database", Recursive = true };
            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(5));
        }

        [Test]
        public void Given_RecursiveFlagIsFalse_Should_ReturnDirectoryContentsOnly()
        {
            var itemsConfig = new DataLakeGetItemsConfig { Directory = "raw/database", Recursive = false };
            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(2));
            Assert.That(result.Files.All(x => x.Directory == itemsConfig.Directory), Is.True);
        }




        //Filters = new[] {FilterFactory<DataLakeItem>.Create("Name", "like:*1*", MockLogger.Object)}
        //[Test]
        //public void Given_ValidDirectoryPath_Should_ReturnDirectoryPath()
        //{
        //    var testPath = "raw/database";
        //    var resultPath = Sut.CheckPathAsync(testPath, true).Result;

        //    Assert.That(resultPath, Is.EqualTo(testPath));
        //}

    }

    public class GetItemsResponse
    {
        public int FileCount { get; set; }
        public string CorrectedFilePath { get; set; }
        public List<DataLakeItem> Files { get; set; }
    }
}