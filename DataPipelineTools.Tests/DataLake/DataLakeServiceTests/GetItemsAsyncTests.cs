using System.Collections.Generic;
using System.Linq;
using DataPipelineTools.Tests.Common;
using Moq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace DataPipelineTools.Tests.DataLake.DataLakeServiceTests
{
    [TestFixture]
    [Category(nameof(TestType.UnitTest))]
    public class GetItemsAsyncTests : DataLakeTestBase
    {

        protected readonly DataLakeService Sut;
        private IDataLakeConnectionConfig DatalakeConnectionConfig => new DataLakeUserServicePrincipalConnectionConfig();

        public GetItemsAsyncTests()
        {
            // Use the factory to inject the mock logger to get the mock client...
            var factory = new DataLakeServiceFactory(Logger);
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
        public void Given_ValidDirectoryPath_Should_ReturnContentsAsJson()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Path = "raw/api/feb"
            };

            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result;

            Assert.That(result.Count, Is.EqualTo(2));
        }

        private static DataLakeGetItemsConfig[] DirectoryPathWithIncorrectCase =
            {
                new DataLakeGetItemsConfig {Path = "RAW/api/feb"},
                new DataLakeGetItemsConfig {Path = "raw/API/feb"},
                new DataLakeGetItemsConfig {Path = "raw/api/FEB"}
            };
        [TestCaseSource(nameof(DirectoryPathWithIncorrectCase))]
        public void Given_DirectoryPathWithIncorrectCase_Should_ReturnContentsForCorrectedPath(DataLakeGetItemsConfig itemsConfig)
        {
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(1));
            Assert.That(result.Files.Count, Is.EqualTo(1));
            Assert.That(result.Files, Has.All.Property(nameof(DataLakeItem.FullPath)).EqualTo("raw/api/feb/delta_extract_3.json"));
        }


        [Test]
        public void Given_InvalidDirectoryPath_Should_ThrowException()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Path = "some/invalid/path"
            };

            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig));
        }
        
        [Test]
        public void Given_DirectoryPathWithIncorrectCase_When_MatchesMultipleDirectories_Should_ThrowException()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Path = "RAW/api/JAN"
            };

            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig));
        }



        private static object[] LimitNRecords =
        {
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", Limit = 1}, 1},
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", Limit = 3}, 3},
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", Limit = 5}, 5},
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", Limit = 10}, 5} // There's 5 files for that folder in the test csv
        };
        [TestCaseSource(nameof(LimitNRecords))]
        public void Given_LimitNRecords_Should_ReturnNRecords(DataLakeGetItemsConfig itemsConfig, int expectedResultCount)
        {
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(expectedResultCount));
            Assert.That(result.Files, Has.Count.EqualTo(expectedResultCount));
        }


        private static object[] OrderByColumn =
        {
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", OrderByColumn = "ContentLength" }, new []{10, 20, 30, 40, 50}}, // Default is ascending order
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = false }, new []{10, 20, 30, 40, 50}},
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = true }, new []{50, 40, 30, 20, 10}}
        };
        [TestCaseSource(nameof(OrderByColumn))]
        public void Given_OrderBy_Should_ReturnRecordsOrderedBySpecifiedColumnWithDirectionSpecifiedByOrderByDescendingFlag(DataLakeGetItemsConfig itemsConfig, int[] expectedContentLengths)
        {
            var expectedFileCount = expectedContentLengths.Length;

            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(expectedFileCount));
            Assert.That(result.Files, Has.Count.EqualTo(expectedFileCount));
            Assert.That(result.Files.Select(item => item.ContentLength), Is.EquivalentTo(expectedContentLengths));
        }


        private static object[] LimitNRecordsAndOrderByColumn =
        {
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", OrderByColumn = "ContentLength" }, new []{10, 20, 30}}, // Default is ascending order
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = false }, new []{10, 20, 30, 40}},
            new object[] {new DataLakeGetItemsConfig {Path = "raw/api/jan", OrderByColumn = "ContentLength", OrderByDescending = true }, new []{50, 40}}
        };
        [TestCaseSource(nameof(LimitNRecordsAndOrderByColumn))]
        public void Given_LimitNRecordAndOrderBy_Should_ReturnTopNRecordsOrderedBySpecifiedColumnWithDirectionSpecifiedByOrderByDescendingFlag(DataLakeGetItemsConfig itemsConfig, int[] expectedContentLengths)
        {
            var expectedFileCount = expectedContentLengths.Length;
            itemsConfig.Limit = expectedFileCount;

            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(expectedFileCount));
            Assert.That(result.Files, Has.Count.EqualTo(expectedFileCount));
            Assert.That(result.Files.Select(item => item.ContentLength), Is.EquivalentTo(expectedContentLengths));
        }

        [Test]
        public void Given_RecursiveFlagIsTrue_Should_ReturnContentRecursively()
        {
            var itemsConfig = new DataLakeGetItemsConfig {Path = "raw/database", Recursive = true };
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(5));
            Assert.That(result.Files, Has.Count.EqualTo(5));
        }

        [Test]
        public void Given_RecursiveFlagIsFalse_Should_ReturnDirectoryContentsOnly()
        {
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw/database", Recursive = false };
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(2));
            Assert.That(result.Files, Has.Count.EqualTo(2));
            Assert.That(result.Files, Has.All.Property(nameof(DataLakeItem.Directory)).EqualTo(itemsConfig.Path));
        }

        [Test]
        public void Given_DirectoryPathWithIncorrectCaseAndIgnoreDirectoryCaseIsFalse_Should_Throw()
        {
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw/DATABASE", IgnoreDirectoryCase = false };
            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig));
        }

        [Test]
        public void Given_DirectoryPathWithIncorrectCaseAndIgnoreDirectoryCaseTrue_Should_ReturnContents()
        {
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw/DATABASE", IgnoreDirectoryCase = true };
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.GreaterThan(0));
            Assert.That(result.Files, Has.Count.GreaterThan(0));
        }

        [Test]
        public void Given_DirectoryPathWithIncorrectCaseAndIgnoreDirectoryCaseIsTrue_WhenMatchesOneDirectoryPath_Should_ReturnCorrectedPath()
        {
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw/DATABASE", IgnoreDirectoryCase = true };
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.CorrectedFilePath, Is.EqualTo("raw/database"));
        }

        [Test]
        public void Given_DirectoryPathWithIncorrectCaseAndIgnoreDirectoryCaseIsTrue_WhenMatcheMultipleDirectoryPaths_Should_Throw()
        {
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw/ApI", IgnoreDirectoryCase = true };
            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig));
        }

        

        private static object[][] Filters = 
        {
            new object[] { "raw/api/jan", nameof(DataLakeItem.Name), "eq:delta_extract_4.json", 1},
            new object[] { "raw/api/jan", nameof(DataLakeItem.Name), "ne:delta_extract_4.json", 4},
            new object[] { "raw/api/jan", nameof(DataLakeItem.ContentLength), "gt:40", 1},
            new object[] { "raw/api/jan", nameof(DataLakeItem.ContentLength), "ge:40", 2},
            new object[] { "raw/api/jan", nameof(DataLakeItem.ContentLength), "lt:20", 1},
            new object[] { "raw/api/jan", nameof(DataLakeItem.ContentLength), "le:20", 2},
            new object[] { "raw/api", nameof(DataLakeItem.Directory), "like:*feb*", 1},
            new object[] { "raw/api", nameof(DataLakeItem.FullPath), "like:delta_extract_[3-5].json", 4},
            new object[] { "raw/api", nameof(DataLakeItem.Url), "like:.+raw\\/api.+", 8}, // 6 files + 2 directories
            new object[] { "raw/api", nameof(DataLakeItem.IsDirectory), "eq:true", 2},
            new object[] { "raw/api", nameof(DataLakeItem.IsDirectory), "eq:false", 6},
            new object[] { "raw/api", nameof(DataLakeItem.LastModified), "ge:2021-01-04T14:00:00", 2},
        };
        [TestCaseSource(nameof(Filters))]
        public void Given_Filter_Should_ReturnRecordsMatchingFilter(string directory, string filterProperty, string filterExpression, int expectedFileCount)
        {
            // Have to build the filter here rather than pass it in using TestCaseSource as the logger is not static
            var filter = FilterFactory<DataLakeItem>.Create(filterProperty, filterExpression, Logger);
            var itemsConfig = new DataLakeGetItemsConfig {Path = directory, Filters = new[] {filter}};
            
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();
            
            Assert.That(result.FileCount, Is.EqualTo(expectedFileCount));
            Assert.That(result.Files, Has.Count.EqualTo(expectedFileCount));
        }

        private static object[][] InvalidFilters =
        {
            new object[] { "raw/api/jan", nameof(DataLakeItem.Name), "neq:delta_extract_4.json"}, // Invalid operator
            new object[] { "raw/api/jan", nameof(DataLakeItem.ContentLength), "eq:delta_extract_4.json"}, // Invalid value type
            new object[] { "raw/api/jan", "Some invalid property", "ne:delta_extract_4.json"},
            new object[] { "raw/api/jan", string.Empty, "ne:delta_extract_4.json"},
            new object[] { "raw/api/jan", null, "ne:delta_extract_4.json"},
            new object[] { "raw/api/jan", nameof(DataLakeItem.Name), string.Empty},
            new object[] { "raw/api/jan", nameof(DataLakeItem.Name), null},

        };
        [TestCaseSource(nameof(InvalidFilters))]
        public void Given_InvalidFilter_Should_Throw(string directory, string filterProperty, string filterExpression)
        {
            // Have to build the filter here rather than pass it in using TestCaseSource as the logger is not static
            var filter = FilterFactory<DataLakeItem>.Create(filterProperty, filterExpression, Logger);
            var itemsConfig = new DataLakeGetItemsConfig { Path = directory, Filters = new[] { filter } };

            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig));
        }

        [TestCaseSource(nameof(InvalidFilters))]
        public void Given_MultipleFilters_WhenSomeAreInvalid_Should_Throw(string directory, string filterProperty, string filterExpression)
        {
            var filters = new[]
            {
                FilterFactory<DataLakeItem>.Create(nameof(DataLakeItem.FullPath), "like:*jan*", Logger),
                FilterFactory<DataLakeItem>.Create(nameof(DataLakeItem.IsDirectory), "eq:5", Logger),
            };
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw", Filters = filters };

            Assert.CatchAsync(() => Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig));
        }

        [Test]
        public void Given_MultipleValidFiltersOfDifferentTypes_Should_ReturnFilteredResults()
        {
            var filters = new[]
            {
                FilterFactory<DataLakeItem>.Create(nameof(DataLakeItem.FullPath), "like:*jan*", Logger),
                FilterFactory<DataLakeItem>.Create(nameof(DataLakeItem.IsDirectory), "eq:true", Logger),
            };
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw", Filters = filters};
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(3));
            Assert.That(result.Files, Has.Count.EqualTo(3));

            Assert.That(result.Files, Has.All.Property(nameof(DataLakeItem.IsDirectory)).True);
        }


        [Test]
        public void Given_MultipleValidFiltersOfSameType_Should_ReturnFilteredResults()
        {
            var filters = new[]
            {
                FilterFactory<DataLakeItem>.Create(nameof(DataLakeItem.FullPath), "like:*jan*", Logger),
                FilterFactory<DataLakeItem>.Create(nameof(DataLakeItem.FullPath), "like:*delta", Logger),
            };
            var itemsConfig = new DataLakeGetItemsConfig { Path = "raw", Filters = filters };
            var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

            Assert.That(result.FileCount, Is.EqualTo(6));
            Assert.That(result.Files, Has.Count.EqualTo(6));

            Assert.That(result.Files, Has.All.Property(nameof(DataLakeItem.FullPath)).Contains("jan"));
            Assert.That(result.Files, Has.All.Property(nameof(DataLakeItem.FullPath)).Contains("delta"));
        }


        //[Test]
        //public void Given__Should_Return()
        //{
        //    var itemsConfig = new DataLakeGetItemsConfig { Path = "raw/database", Recursive = false };
        //    var result = Sut.GetItemsAsync(DatalakeConnectionConfig, itemsConfig).Result.ToObject<GetItemsResponse>();

        //    Assert.That(result.FileCount, Is.EqualTo(2));
        //    Assert.That(result.Files.All(x => x.Path == itemsConfig.Path), Is.True);
        //}
    }

    public class GetItemsResponse
    {
        public int FileCount { get; set; }
        public string CorrectedFilePath { get; set; }
        public List<DataLakeItem> Files { get; set; }
    }
}