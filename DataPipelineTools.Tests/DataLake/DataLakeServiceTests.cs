using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core.Extensions;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Logging;
using Moq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace DataPipelineTools.Tests.DataLake
{
    [TestFixture]
    public class DataLakeServiceTests: TestBase
    {
        private const string AccountUri = "mydatalake";
        private const string ContainerName = "mycontainer";

        private readonly IEnumerable<PathItem> TestData;
        private readonly Mock<DataLakeFileSystemClient> mockFileSystemClient;

        //private readonly Mock<DataLakeDirectoryClient> mockDirectoryClient;
        //private readonly Mock<DataLakeFileClient> mockFileClient;

        private readonly Mock<ILogger<DataLakeServiceFactory>> mockLogger;
       
        private readonly DataLakeService Sut;

        public DataLakeServiceTests()
        {
            // Get test data to mock the file system
            TestData = GetTestData();

            // Mock the logger to test where it is called
            mockLogger = new Mock<ILogger<DataLakeServiceFactory>>();

            // Mock the file system client
            mockFileSystemClient = BuildMockDataLakeFileSystemClient();

            // Use the factory to inject the mock logger to get the mock client...
            var factory = new DataLakeServiceFactory(mockLogger.Object);
            Sut = factory.CreateDataLakeService(mockFileSystemClient.Object);
        }

        private Mock<DataLakeFileSystemClient> BuildMockDataLakeFileSystemClient()
        {
            var mockFileSystemClient = new Mock<DataLakeFileSystemClient>();
            mockFileSystemClient.SetupGet(x => x.Name).Returns(ContainerName);
            mockFileSystemClient.SetupGet(x => x.AccountName).Returns(AccountUri);

            mockFileSystemClient
                .Setup(x => x.GetDirectoryClient(It.IsAny<String>()))
                .Returns<string>(BuildMockDataLakeDirectoryClient<DataLakeDirectoryClient>);
            mockFileSystemClient
                .Setup(x => x.GetFileClient(It.IsAny<String>()))
                .Returns<string>(BuildMockDataLakeDirectoryClient<DataLakeFileClient>);

            mockFileSystemClient
                .Setup(x => x.GetPaths(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns((string p, bool r) =>
                {
                    var items = TestData
                        // Include all files starting with the test path
                        .Where(x => x.Name.StartsWith(p))
                        // Still include them if the recursive flag is set, otherwise check if the relative path after the search path contains 
                        // directory separator to exclude sub dirs
                        .Where(x => r || !x.Name.Substring(p.Length).Contains('/'))
                        .ToList()
                        .AsReadOnly();

                    var page = Page<PathItem>.FromValues(items, null, Mock.Of<Response>());
                    return Pageable<PathItem>.FromPages(new[] { page });
                });

            return mockFileSystemClient;
        }

        private T BuildMockDataLakeDirectoryClient<T>(string directoryName) where T: DataLakePathClient
        {
            var mockDirectoryClient = new Mock<T>();
            mockDirectoryClient.SetupGet(x => x.FileSystemName).Returns(ContainerName);
            mockDirectoryClient.SetupGet(x => x.AccountName).Returns(AccountUri);
            mockDirectoryClient.SetupGet(x => x.Name).Returns(directoryName);

            var directoryNameExists = TestData.Any(i => i.Name == directoryName);
            mockDirectoryClient
                .Setup(x => x.ExistsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => Response.FromValue(directoryNameExists, new Mock<Response>().Object));

            mockDirectoryClient
                .Setup(x => x.Exists(It.IsAny<CancellationToken>()))
                .Returns(() => Response.FromValue(directoryNameExists, new Mock<Response>().Object));

            return mockDirectoryClient.Object;
        }

        private IEnumerable<PathItem> GetTestData()
        {
            return GetTestData(",", properties =>
            {
                return DataLakeModelFactory.PathItem(
                    properties[nameof(PathItem.Name)],
                    Convert.ToBoolean(properties[nameof(PathItem.IsDirectory)]),
                    Convert.ToDateTime(properties[nameof(PathItem.LastModified)]),
                    ETag.All,
                    Convert.ToInt32(properties[nameof(PathItem.ContentLength)]), 
                    null,
                    null,
                    null
                    );
            }).ToArray();
        }

//private IEnumerable<DataLakeItem> GetTestData()
//        { 
//            return GetTestData(",", properties =>
//            {
//                return new DataLakeItem()
//                {
//                    Directory = properties[nameof(DataLakeItem.Directory)],
//                    Name = properties[nameof(DataLakeItem.Name)],
//                    IsDirectory = Convert.ToBoolean(properties[nameof(DataLakeItem.IsDirectory)]),
//                    ContentLength = Convert.ToInt32(properties[nameof(DataLakeItem.ContentLength)]),
//                    LastModified = Convert.ToDateTime(properties[nameof(DataLakeItem.LastModified)])
//                };
//            }).ToArray();
//        }

        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void CheckPathAsync_ShouldReturn_()
        {
            
        }

        
    }
}