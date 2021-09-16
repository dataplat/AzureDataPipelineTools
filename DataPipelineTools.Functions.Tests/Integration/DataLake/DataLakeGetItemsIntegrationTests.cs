using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using DataPipelineTools.Tests.Common;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake;

namespace DataPipelineTools.Functions.Tests.Integration.DataLake
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    [Parallelizable(ParallelScope.Children)]
    public class DataLakeGetItemsIntegrationTests : DataLakeIntegrationTestBase
    {
        protected override string FunctionRelativeUri => "DataLake/GetItems";

        private const int ItemsInRootFolder = 2;
        private const int TotalDirectories = 6;
        private const int TotalFiles = 6;
        private int TotalItems => TotalDirectories + TotalFiles;

        [Test]
        public async Task
            Given_FolderPath_With_WrongCaseAndSingleMatch_AndParamIgnoreDirectoryCaseParamIsTrue_Should_ReturnFileList()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "TestData/TESTFOLDER1"}, // Real Path is 'TestData/TestFolder1'
                {DataLakeConfigFactory.IgnoreDirectoryCaseParam, true.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);


            Assert.IsNull(results.error);
            Assert.AreEqual(ItemsInRootFolder, (int) results.fileCount);
        }

        [Test]
        public async Task
            Given_FolderPath_With_WrongCaseAndSingleMatch_AndParamIgnoreDirectoryCaseParamIsFalse_Should_ReturnError()
        {
            const string folder = "TestData/TESTFOLDER1"; // Real Path is 'TestData/TestFolder1'

            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, folder},
                {DataLakeConfigFactory.IgnoreDirectoryCaseParam, false.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(string.Format(DataLakeService.ErrorMessage.PathNotFound, folder),
                results.error?.ToString());
        }

        [Test]
        public async Task
            Given_FolderPath_With_WrongCaseAndMultipleMatches_AndParamIgnoreDirectoryCaseParamIsTrue_Should_ReturnError()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "testdata"}, // Real Path is 'TestData' or 'TESTDATA'
                {DataLakeConfigFactory.IgnoreDirectoryCaseParam, true.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);


            Assert.IsNotNull(results.error?.ToString());
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(10)]
        [Test]
        public async Task
            Given_RootPath_AndParamLimitIsGreaterThanZero_Should_ReturnCorrectNumberOfResults(int limit)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.LimitParam, limit.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            var expectedItems = (new[] {ItemsInRootFolder, limit}).Min();
            Assert.AreEqual(expectedItems, (int) results.fileCount);
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        [TestCase(-10)]
        public async Task
            Given_RootPath_AndParamLimitIsZeroOrNegative_Should_ReturnTwoResults(int limit)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.LimitParam, limit.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(ItemsInRootFolder, (int) results.fileCount);
        }

        [Test]
        public async Task
            Given_RootPath_AndParamLimitIsOne_AndOrderByParamIsName_AndOrderByDescParamIsTrue_Should_ReturnOneDirectory()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.LimitParam, 1.ToString()},
                {DataLakeConfigFactory.OrderByColumnParam, "Name"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(1, (int) results.fileCount);
            Assert.AreEqual("TestData", results.files?[0]?.Name.ToString());
            Assert.AreEqual(true, (bool) results.files?[0]?.IsDirectory);
        }

        [Test]
        public async Task
            Given_RootPath_AndParamLimitIsOne_AndOrderByParamIsName_Should_ReturnFirstDirectory()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.LimitParam, 1.ToString()},
                {DataLakeConfigFactory.OrderByColumnParam, "Name"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(1, (int) results.fileCount);
            Assert.AreEqual("TestData", results.files?[0]?.Name.ToString());
            Assert.AreEqual(true, (bool) results.files?[0]?.IsDirectory);
        }

        [Test]
        public async Task
            Given_RootPath_AndParamLimitIsOne_AndOrderByParamIsName_AndOrderByDescParamIsTrue_Should_ReturnLastDirectory()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.LimitParam, 1.ToString()},
                {DataLakeConfigFactory.OrderByColumnParam, "Name"},
                {DataLakeConfigFactory.OrderByDescendingParam, true.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(1, (int) results.fileCount);
            Assert.AreEqual("TESTDATA", results.files?[0]?.Name.ToString());
            Assert.AreEqual(true, (bool) results.files?[0]?.IsDirectory);
        }


        [Test]
        public async Task
            Given_RootPath_AndRecursiveParamIsTrue_Should_ReturnTwelveResults()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.RecursiveParam, true.ToString()}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(TotalItems, (int) results.fileCount);
        }



        [Test]
        public async Task
            Given_RootPath_AndRecursiveParamIsFalse_Should_ReturnTwoDirectories()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(ItemsInRootFolder, (int) results.fileCount);

            var allItemsAreDirectories = (results.files as JArray)?.All(x => (bool) x["IsDirectory"]);
            Assert.AreEqual(true, allItemsAreDirectories);
        }

        [Test]
        [TestCase(true, TotalDirectories)]
        [TestCase(false, TotalFiles)]
        public async Task
            Given_RootPath_AndRecursiveParamIsTrue_AndFilterOnIsDirectoryEqualsTrue_Should_ReturnTwoResults(
                bool isDirectory, int expectedFileCount)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.RecursiveParam, true.ToString()},
                {"filter[IsDirectory]", $"eq:{isDirectory}"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(expectedFileCount, (int) results.fileCount);
            var allItemsAreDirectories = (results.files as JArray)?.All(x => (bool) x["IsDirectory"] == isDirectory);
            Assert.AreEqual(true, allItemsAreDirectories);
        }

        [Test]
        [TestCase("lt", "2020-01-01 00:00:00", 0)]
        [TestCase("le", "2999-01-01 00:00:00", TotalFiles)]
        [TestCase("gt", "2020-01-01 00:00:00", TotalFiles)]
        [TestCase("ge", "2999-01-01 00:00:00", 0)]
        [TestCase("eq", "2020-09-01 00:00:00", 0)]
        [TestCase("ne", "2020-09-01 00:00:00", TotalFiles)]
        public async Task
            Given_RootPath_AndRecursiveParamIsTrue_AndFilterOnLastModified_Should_ReturnNResults(string matchOperator,
                string lastModified, int expectedFileCount)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.RecursiveParam, true.ToString()},
                {"filter[IsDirectory]", $"eq:{false}"},
                {"filter[LastModified]", $"{matchOperator}:{lastModified}"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(expectedFileCount, (int) results.fileCount);
        }




        // ******************************************************************************************************
        // **** This is currently not supported. See issue no 4
        // ******************************************************************************************************
        //[Test]
        //[TestCase(100, 1100, 1, "TestDoc2.txt")]
        //[TestCase(6000, 7000, 1, "TestDoc1.txt")]
        //[TestCase(2800, 3800, 2, "TestDoc3.txt")]
        //[TestCase(15500, 16500, 2, "TestDoc4.txt")]
        //public async Task
        //Given_RootPath_AndRecursiveParamIsTrue_AndFilterOnContentLengthBetweenValues_Should_ReturnNResults(int contentLengthMin, int contentLengthMax, int expectedFileCount, string expectedFileName)
        //{
        //    var parameters = new List<KeyValuePair<string, string>>()
        //    {
        //        new KeyValuePair<string, string>(DataLakeConfigFactory.AccountParam, StorageAccountName),
        //        new KeyValuePair<string, string>(DataLakeConfigFactory.ContainerParam, StorageContainerName),
        //        new KeyValuePair<string, string>(DataLakeConfigFactory.PathParam, "/"),
        //        new KeyValuePair<string, string>(DataLakeConfigFactory.RecursiveParam, true.ToString()),
        //        new KeyValuePair<string, string>("filter[IsDirectory]",$"eq:{false}"),
        //        new KeyValuePair<string, string>("filter[ContentLength]",$"ge:{contentLengthMin}"),
        //        new KeyValuePair<string, string>("filter[ContentLength]",$"le:{contentLengthMax}")
        //    };
        //    var response = await RunQueryFromParameters(parameters);
        //    LogContent(response);
        //    Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

        //    // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
        //    dynamic results = GetResultsObject(response);

        //    Assert.AreEqual(expectedFileCount, (int)results.fileCount);

        //    var allItemsAreNamedAsExpected = (results.files as JArray)?.All(x => x["Name"].ToString() == expectedFileName);
        //    Assert.AreEqual(true, allItemsAreNamedAsExpected);
        //}

        [Test]
        [TestCase("filter[Name]", "eq", "TestDoc1.txt", "TestDoc3.txt", 2)]
        [TestCase("filter[ContentLength]", "ge", 10000, 1, TotalFiles)]
        [TestCase("filter[ContentLength]", "ge", 1, 10000, 2)]
        public async Task
            Given_TheSameParameterTwice_Should_ReturnResultsBasedOnLastParameterValue(
                string parameter, string compareOperator, object firstValue, object secondValue, int expectedFileCount)
        {
            var parameters = new List<KeyValuePair<string, string>>()
            {
                new KeyValuePair<string, string>(DataLakeConfigFactory.AccountParam, StorageAccountName),
                new KeyValuePair<string, string>(DataLakeConfigFactory.ContainerParam, StorageContainerName),
                new KeyValuePair<string, string>(DataLakeConfigFactory.PathParam, "/"),
                new KeyValuePair<string, string>(DataLakeConfigFactory.RecursiveParam, true.ToString()),
                new KeyValuePair<string, string>(parameter, $"{compareOperator}:{firstValue}"),
                new KeyValuePair<string, string>(parameter, $"{compareOperator}:{secondValue}")
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(expectedFileCount, (int) results.fileCount);
        }




        [Test]
        public async Task Given_MultipleFiltersOnDifferentProperties_Should_ReturnResultsFromAllFiltersApplies()
        {
            var parameters = new Dictionary<string, string>()
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.RecursiveParam, true.ToString()},
                {"filter[IsDirectory]", $"eq:{false}"},
                {"filter[Directory]", "like:*Case*"},
                {"filter[Name]", "like:*3.txt"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(2, (int)results.fileCount);
        }


        [Test]
        public async Task Given_FilterOnInvalidProperty_Should_ReturnError()
        {
            var parameters = new Dictionary<string, string>()
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/"},
                {DataLakeConfigFactory.RecursiveParam, true.ToString()},
                {"filter[SomeInvalidProperty]", $"eq:SomeValue"},
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            // TODO: Make the error useful to the client
            Assert.IsNotNull(results.error?.ToString());
            Assert.Warn("TODO: Make the error message useful to the client");
        }

        [Test]
        public async Task Given_PathIsEmptyFolder_Should_ReturnZeroResults()
        {
            var parameters = new Dictionary<string, string>()
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "TestData/TestFolder2"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(0, (int)results.fileCount);
        }


        [Test]
        public async Task InvalidPath_Should_ReturnError()
        {
            var parameters = new Dictionary<string, string>()
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "SomeInvalidPath"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.IsNotNull(results.error?.ToString());
        }
    }
}