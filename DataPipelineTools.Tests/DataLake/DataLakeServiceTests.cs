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
        public void CheckPathAsync_Given_ValidDirectoryPath_Should_ReturnDirectoryPath()
        {
            var testPath = "raw/database";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;
            
            Assert.That(testPath, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_DirectoryPathWithIncorrectCase_Should_ReturnCorrectedDirectoryPath()
        {
            var testPath = "raw/DATABASE";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That("raw/database", Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_ValidFilePath_Should_ReturnValidPath()
        {
            var testPath = "raw/database/jan/extract_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(testPath, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_FilePathWithIncorrectCase_Should_ReturnCorrectedFilePath()
        {
            var testPath = "raw/DATABASE/jan/extract_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That("raw/database/jan/extract_1.csv", Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_ValidDirectoryPath_And_IsDirectoryFlagIsFalse_Should_ReturnNull()
        {
            var testPath = "raw/database";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(testPath, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_ValidFilePath_And_IsDirectoryFlagIsTrue_Should_ReturnNull()
        {
            var testPath = "raw/database/jan/extract_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(testPath, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_InvalidDirectoryPath_Should_ReturnNull()
        {
            var testPath = "some/invalid/path";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(null, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_InvalidFilePath_Should_ReturnNull()
        {
            var testPath = "some/invalid/path.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(null, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_DirectoryPathWithIncorrectCase_When_MatchesMultiplePaths_Should_Throw()
        {
            var testPath = "raw/aPi";
            
            Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, true));
        }

        [Test]
        public void CheckPathAsync_Given_FilePathWithIncorrectCase_When_MatchesMultiplePaths_Should_Throw()
        {
            var testPath = "raw/aPi/jan/delta_extract_1.json";

            Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, true));
        }



        //[Test]
        //public void CheckPathAsync_Given__Should_()
        //{
        //    var testPath = "some/invalid/path";
        //    var resultPath = Sut.CheckPathAsync(testPath, true).Result;

        //    Assert.That(null, Is.EqualTo(resultPath));
        //}

    }
}