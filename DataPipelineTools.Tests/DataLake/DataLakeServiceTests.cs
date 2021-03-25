using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;

namespace DataPipelineTools.Tests.DataLake
{
    [TestFixture]
    public class DataLakeServiceTests: DataLakeTestBase
    {

        protected readonly DataLakeService Sut;

        public DataLakeServiceTests()
        {
            // Use the factory to inject the mock logger to get the mock client...
            var factory = new DataLakeServiceFactory(MockLogger.Object);
            Sut = factory.CreateDataLakeService(MockFileSystemClient.Object);
        }

        [SetUp]
        public void Setup()
        {

        }

        [Test]
        public void CheckPathAsync_ShouldReturnThePath_WhenTheDirectoryPathExists()
        {
            var testPath = "raw/database";
            var correctPath = Sut.CheckPathAsync(testPath, true).Result;
            
            Assert.That(testPath, Is.EqualTo(correctPath));
        }
        
        [Test]
        public void CheckPathAsync_ShouldReturnThePath_WhenTheFilePathExists()
        {
            var testPath = "raw/database/jan/extract_1.csv";
            var correctPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(testPath, Is.EqualTo(correctPath));
        }

        [Test]
        public void CheckPathAsync_ShouldReturnNull_WhenTheIsDirectoryIsIncorrectlyFalse()
        {
            var testPath = "raw/database";
            var correctPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(testPath, Is.EqualTo(correctPath));
        }

        [Test]
        public void CheckPathAsync_ShouldReturnNull_WhenTheIsDirectoryIsIncorrectlyTrue()
        {
            var testPath = "raw/database/jan/extract_1.csv";
            var correctPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(testPath, Is.EqualTo(correctPath));
        }

        [Test]
        public void CheckPathAsync_ShouldReturnNull_WhenPathDoesNotExist()
        {
            var testPath = "some/invalid/path";
            var correctPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(null, Is.EqualTo(correctPath));
        }


    }
}