using System;
using System.Collections.Generic;
using System.Linq;
using Moq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
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

        [Test]
        public void Given_DirectoryPathWithIncorrectCase_Should_ReturnContentsForCorrectedPath()
        {
            var itemsConfig = new DataLakeGetItemsConfig
            {
                Directory = "raw/aPi/feb"
            };


            var r = Sut.CheckPathAsync("raw/Api/jan/delta_extract_1.json", false).Result;

            var result = Sut.GetItemsAsync(DatalakeConfig, itemsConfig).Result;

            Assert.That(result.ContainsKey("fileCount"), Is.True);
            Assert.That(result.ContainsKey("files"), Is.True);

            Assert.That( (int)result.Property("fileCount").Value, Is.EqualTo(2) );

            var s = result.ToObject<GetItemsResponse>();
            var responseObject = JsonConvert.DeserializeObject<GetItemsResponse>(result.ToString());

            var files = result.Property("files");
            var itemsInfo = JsonConvert.DeserializeObject<List<DataLakeItem>>(files.ToString());
            Assert.That(itemsInfo, Is.EqualTo(2));
            //Assert.That(itemsInfo.Count(x => x.FullPath == "raw/api/feb"), Is.True);
            //Assert.That(itemsInfo.Count(x => x.FullPath == "raw/api/feb/delta_extract_3.json"), Is.True);
        }

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
        public int fileCount { get; set; }
        public string correctedFilePath { get; set; }
        public List<DataLakeItem> files { get; set; }
    }
}