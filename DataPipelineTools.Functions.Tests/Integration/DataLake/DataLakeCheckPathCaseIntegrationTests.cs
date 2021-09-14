using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using DataPipelineTools.Tests.Common;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake;

namespace DataPipelineTools.Functions.Tests.Integration.DataLake
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    [Parallelizable(ParallelScope.Children)]
    public class DataLakeCheckPathCaseIntegrationTests : DataLakeIntegrationTestBase
    {
        protected override string FunctionRelativeUri => "DataLake/CheckPathCase";


        [Test]
        // Test all combos of leading slashes
        [TestCase("TestData/TestFolder1/TestDoc1.txt")]
        [TestCase("/TestData/TestFolder1/TestDoc1.txt")]
        public async Task Given_ValidFilePath_Should_ReturnSamePath(string testPath)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, testPath}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);


            Assert.IsNull(results.error);
            Assert.AreEqual(testPath.Trim('/'), results.validatedPath?.ToString());
        }

        [Test]
        // Test all combos of leading/trailing slashes
        [TestCase("TestData/TestFolder1/")]
        [TestCase("TestData/TestFolder1")]
        [TestCase("/TestData/TestFolder1/")]
        [TestCase("/TestData/TestFolder1")]
        public async Task Given_ValidFolderPath_Should_ReturnSamePath(string testPath)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, testPath}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);


            Assert.IsNull(results.error);
            Assert.AreEqual(testPath.Trim('/'), results.validatedPath?.ToString());
        }

        [Test]
        public async Task Given_NonExistentPath_Should_ReturnBadRequest()
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, "/SomeMadeUpPath"}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);


            Assert.AreEqual(DataLakeFunctions.PathNotFoundErrorMessage,  results.error?.ToString());
            Assert.IsNull(results.validatedPath?.ToString());
        }

        [Test]
        [TestCase("TESTDATA/TestFolder1/")]
        [TestCase("TestData/TESTFOLDER1")]
        [TestCase("/TESTDATA/TestFolder1/")]
        [TestCase("/TestData/TESTFOLDER1")]
        public async Task Given_FolderPath_With_WrongCaseAndSingleMatch_Should_ReturnValidPath(string testPath)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, testPath}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual("TestData/TestFolder1", results.validatedPath?.ToString());
        }

        [Test]
        [TestCase("TESTDATA/TestFolder1/TestDoc1.txt")]
        [TestCase("TestData/TESTFOLDER1/TestDoc1.txt")]
        [TestCase("TestData/TestFolder1/TESTDOC1.txt")]
        [TestCase("/TESTDATA/TestFolder1/TestDoc1.txt")]
        [TestCase("/TestData/TESTFOLDER1/TestDoc1.txt")]
        [TestCase("/TestData/TestFolder1/TESTDOC1.txt")]
        public async Task Given_FilePath_With_WrongCaseAndSingleMatch_Should_ReturnValidPath(string testPath)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, testPath}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual("TestData/TestFolder1/TestDoc1.txt", results.validatedPath?.ToString());
        }

        [Test]
        [TestCase("TestData/TESTFOLDERCASEDUPES/TestDoc3.txt")]
        [TestCase("TestData/TESTFOLDERCASEDUPES/TestDoc4.txt")]
        public async Task Given_FilePath_With_WrongCaseAndMultipleMatches_Should_ReturnError(string testPath)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, testPath}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(DataLakeService.ErrorMessage.MultipleFileMatchesWithCaseInsensitiveCompare, results.error?.ToString());
        }

        [Test]
        [TestCase("TestData/TESTFOLDERCASEDUPES")]
        [TestCase("TestData/TESTFOLDERCASEDUPES/")]
        [TestCase("/TestData/TESTFOLDERCASEDUPES")]
        [TestCase("testdata/TestFolderCaseDupes")]
        [TestCase("testdata/TestFolderCaseDupes/")]
        [TestCase("/testdata/TestFolderCaseDupes")]
        public async Task Given_FolderPath_With_WrongCaseAndMultipleMatches_Should_ReturnError(string testPath)
        {
            var parameters = new Dictionary<string, string>
            {
                {DataLakeConfigFactory.AccountParam, StorageAccountName},
                {DataLakeConfigFactory.ContainerParam, StorageContainerName},
                {DataLakeConfigFactory.PathParam, testPath}
            };
            var response = await RunQueryFromParameters(parameters);
            LogContent(response);
            Assert.AreEqual(HttpStatusCode.BadRequest, response.StatusCode);

            // Check response details. Its important to cast the actual or we test against JToken from the dynamic results
            dynamic results = GetResultsObject(response);

            Assert.AreEqual(DataLakeService.ErrorMessage.MultipleDirectoryMatchesWithCaseInsensitiveCompare, results.error?.ToString());
        }
    }
}