using System;
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
        public void CheckPathAsync_Given_FilePathWithIncorrectFilenameCase_Should_ReturnCorrectedFilePath()
        {
            var testPath = "raw/database/jan/eXTRact_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That("raw/database/jan/extract_1.csv", Is.EqualTo(resultPath));
        }


        [Test]
        public void CheckPathAsync_Given_FilePathWithIncorrectDirectoryCase_Should_ReturnCorrectedFilePath()
        {
            var testPath = "raw/database/JAN/extract_1.csv";
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
        public void CheckPathAsync_Given_NullPath_Should_Return_EmptyString()
        {
            string testPath = null;
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(string.Empty, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_EmptyStringPath_Should_Return_EmptyString()
        {
            var testPath = string.Empty;
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(string.Empty, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_WhitespacePath_Should_Return_EmptyString()
        {
            var testPath = "   \t\r\n   ";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(string.Empty, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_ForwardSlashPath_Should_Return_EmptyString()
        {
            var testPath = "/";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(string.Empty, Is.EqualTo(resultPath));
        }

        [Test]
        public void CheckPathAsync_Given_DirectoryPathWithIncorrectCase_Should_ThrowWhenMultipleDirectoriesMatch()
        {
            var testPath = "RaW/api/jan";

            var exception = Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, true));
            Assert.That(exception, Is.TypeOf(typeof(Exception)));
        }

        [Test]
        public void CheckPathAsync_Given_FilePathWithIncorrectCase_Should_ThrowWhenMultipleDirectoriesMatch()
        {
            var testPath = "RaW/api/jan/delta_extract_1.json";

            var exception = Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, false));
            Assert.That(exception, Is.TypeOf(typeof(Exception)));
        }

        [Test]
        public void CheckPathAsync_Given_FilePathWithIncorrectCase_Should_ThrowWhenMultipleFilesMatch()
        {
            var testPath = "raw/database/feb/Extract_2.csv";

            var exception = Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, false));
            Assert.That(exception, Is.TypeOf(typeof(Exception)));
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