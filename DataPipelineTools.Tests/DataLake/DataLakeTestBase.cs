using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Logging;
using Moq;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;

namespace DataPipelineTools.Tests.DataLake
{
    /// <summary>
    /// Base class for tests that need to mock the DataLakeFileSystemClient classes
    /// </summary>
    public class DataLakeTestBase : TestBase
    {
        protected const string AccountUri = "mydatalake";
        protected const string ContainerName = "mycontainer";

        protected readonly IEnumerable<PathItem> TestData;

        protected readonly Mock<DataLakeFileSystemClient> MockFileSystemClient;
        protected readonly Mock<ILogger<DataLakeServiceFactory>> MockLogger;



        public DataLakeTestBase()
        {
            // Get test data to mock the file system
            TestData = GetTestData();

            // Mock the logger to test where it is called
            MockLogger = new Mock<ILogger<DataLakeServiceFactory>>();

            // Mock the file system client
            MockFileSystemClient = BuildMockDataLakeFileSystemClient();
        }

        protected Mock<DataLakeFileSystemClient> BuildMockDataLakeFileSystemClient()
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
                .Returns((string path, bool recursive, bool userPrinciaplName, CancellationToken token) =>
                {
                    var items = TestData
                        // Include all files starting with the test path
                        .Where(x => x.Name.StartsWith(path ?? string.Empty) && path != null)
                        // Still include them if the recursive flag is set, otherwise check if the relative path after the search path contains 
                        // directory separator to exclude sub dirs
                        .Where(x => recursive || !x.Name.Substring(path.Length).Contains('/'))
                        .ToList()
                        .AsReadOnly();

                    var page = Page<PathItem>.FromValues(items, null, Mock.Of<Response>());
                    return Pageable<PathItem>.FromPages(new[] { page });
                });

            return mockFileSystemClient;
        }

        protected T BuildMockDataLakeDirectoryClient<T>(string directoryName) where T : DataLakePathClient
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

        protected IEnumerable<PathItem> GetTestData()
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
    }
}
