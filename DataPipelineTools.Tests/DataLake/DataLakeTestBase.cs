using System;
using System.Collections.Generic;
using System.IO;
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
    public class DataLakeTestBase : TestBase<DataLakeServiceFactory>
    {
        protected const string AccountUri = "mydatalake";
        protected const string ContainerName = "mycontainer";

        protected readonly IEnumerable<PathItem> TestData;

        protected readonly Mock<DataLakeFileSystemClient> MockFileSystemClient;


        public DataLakeTestBase()
        {
            // Get test data to mock the file system
            TestData = GetTestData();

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
                    var pathLength = 1 + (path?.Length ?? 0);
                    var items = TestData
                        // Include all files starting with the test path, or root paths if the test path is null
                        .Where(x => x.Name.StartsWith(path ?? string.Empty) && x.Name != path)
                        // Still include them if the recursive flag is set, otherwise check if the relative path after the search path contains 
                        // directory separator to exclude sub dirs
                        .Where(x => recursive || !x.Name.Substring(pathLength > x.Name.Length ? x.Name.Length : pathLength).Contains('/'))
                        .ToList()
                        .AsReadOnly();

                    var page = Page<PathItem>.FromValues(items, null, Mock.Of<Response>());
                    return Pageable<PathItem>.FromPages(new[] { page });
                });

            return mockFileSystemClient;
        }

        protected T BuildMockDataLakeDirectoryClient<T>(string path) where T : DataLakePathClient
        {
            var mockDataLakePathClient = new Mock<T>();
            mockDataLakePathClient.SetupGet(x => x.FileSystemName).Returns(ContainerName);
            mockDataLakePathClient.SetupGet(x => x.AccountName).Returns(AccountUri);
            mockDataLakePathClient.SetupGet(x => x.Name).Returns(path);

            var pathExists = TestData.Any(i => i.Name == path);
            mockDataLakePathClient
                .Setup(x => x.ExistsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => Response.FromValue(pathExists, new Mock<Response>().Object));

            mockDataLakePathClient
                .Setup(x => x.Exists(It.IsAny<CancellationToken>()))
                .Returns(() => Response.FromValue(pathExists, new Mock<Response>().Object));

            return mockDataLakePathClient.Object;
        }

        protected IEnumerable<PathItem> GetTestData()
        {
            var files = GetTestData(",", properties =>
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
            }).ToList();

            // Build the directories by expanding the paths in the file to add all required levels of the directory structure
            var directories = files.SelectMany(f => ExpandAllParentPaths(f.Name))
                .Distinct()
                .Select(d =>
                {
                    var lastModified = files.Where(f => f.Name.StartsWith(d))
                        .Select(f => f.LastModified)
                        .Min();

                    return DataLakeModelFactory.PathItem(
                        d,
                        true,
                        lastModified,
                        ETag.All,
                        0,
                        null,
                        null,
                        null
                    );
                })
                // Remove dupes if the directory was listed in the source file as an empty dir
                .Where(d => !files.Exists(f => f.Name == d.Name))
                .ToList();

            files.AddRange(directories);
            return files.OrderBy(x => x.Name).ToList();
        }

        private IEnumerable<string> ExpandAllParentPaths(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return new string[0];

            // Directory separators are changed when using Path.GetDirectoryName(...)
            var parent = Path.GetDirectoryName(path).Replace('\\', '/');
            var grandParents = ExpandAllParentPaths(parent);

            return grandParents.Append(parent).Where(s => !string.IsNullOrWhiteSpace(s));
        }
    }
}
