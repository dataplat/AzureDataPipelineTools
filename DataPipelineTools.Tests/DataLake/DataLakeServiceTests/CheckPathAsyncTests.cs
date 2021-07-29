using System;
using DataPipelineTools.Tests.Common;
using Moq;
using NUnit.Framework;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;

namespace DataPipelineTools.Tests.DataLake.DataLakeServiceTests
{
    [TestFixture]
    [Category(nameof(TestType.UnitTest))]
    public class CheckPathAsyncTests: DataLakeTestBase
    {

        protected readonly DataLakeService Sut;

        public CheckPathAsyncTests()
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
        public void Given_ValidDirectoryPath_Should_ReturnDirectoryPath()
        {
            var testPath = "raw/database";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;
            
            Assert.That(resultPath, Is.EqualTo(testPath));
        }

        [TestCase("raw/database/JAN")]
        [TestCase("raw/DataBase/jan")]
        [TestCase("RAW/database/jan")]
        public void Given_DirectoryPathWithIncorrectCase_Should_ReturnCorrectedDirectoryPath(string testPath)
        {
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo("raw/database/jan"));
        }

        [Test]
        public void Given_ValidFilePath_Should_ReturnValidPath()
        {
            var testPath = "raw/database/jan/extract_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(resultPath, Is.EqualTo(testPath));
        }

        [Test]
        public void Given_FilePathWithIncorrectFilenameCase_Should_ReturnCorrectedFilePath()
        {
            var testPath = "raw/database/jan/eXTRact_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(resultPath, Is.EqualTo("raw/database/jan/extract_1.csv"));
        }


        [TestCase("raw/database/JAN/extract_1.csv")]
        [TestCase("raw/DataBase/jan/extract_1.csv")]
        [TestCase("RAW/database/jan/extract_1.csv")]
        public void Given_FilePathWithIncorrectDirectoryCase_Should_ReturnCorrectedFilePath(string testPath)
        {
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(resultPath, Is.EqualTo("raw/database/jan/extract_1.csv"));
        }

        [Test]
        public void Given_ValidDirectoryPath_And_IsDirectoryFlagIsFalse_Should_ReturnNull()
        {
            var testPath = "raw/database";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(resultPath, Is.EqualTo(testPath));
        }

        [Test]
        public void Given_ValidFilePath_And_IsDirectoryFlagIsTrue_Should_ReturnNull()
        {
            var testPath = "raw/database/jan/extract_1.csv";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo(testPath));
        }

        [Test]
        public void Given_InvalidDirectoryPath_Should_ReturnNull()
        {
            var testPath = "some/invalid/path";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo(null));
        }

        [Test]
        public void Given_InvalidFilePath_Should_ReturnNull()
        {
            var testPath = "some/invalid/path.csv";
            var resultPath = Sut.CheckPathAsync(testPath, false).Result;

            Assert.That(resultPath, Is.EqualTo(null));
        }

        [TestCase("raw/aPi")]
        [TestCase("RAW/api")]
        public void Given_DirectoryPathWithIncorrectCase_When_MatchesMultiplePaths_Should_Throw(string testPath)
        {
            Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, true));
        }
        
        [Test]
        public void Given_NullPath_Should_Return_EmptyString()
        {
            string testPath = null;
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo(string.Empty));
        }

        [Test]
        public void Given_EmptyStringPath_Should_Return_EmptyString()
        {
            var testPath = string.Empty;
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo(string.Empty));
        }

        [Test]
        public void Given_WhitespacePath_Should_Return_EmptyString()
        {
            var testPath = "   \t\r\n   ";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo(string.Empty));
        }

        [Test]
        public void Given_ForwardSlashPath_Should_Return_EmptyString()
        {
            var testPath = "/";
            var resultPath = Sut.CheckPathAsync(testPath, true).Result;

            Assert.That(resultPath, Is.EqualTo(string.Empty));
        }

        [TestCase("RaW/api/jan")]
        [TestCase("raw/ApI/jan")]
        [TestCase("raw/api/JaN")]
        public void Given_DirectoryPathWithIncorrectCase_Should_ThrowWhenMultipleDirectoriesMatch(string testPath)
        {
            var exception = Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, true));
            Assert.That(exception, Is.TypeOf(typeof(Exception)));
        }

        [TestCase("RaW/api/jan/delta_extract_1.json")]
        [TestCase("raw/ApI/jan/delta_extract_1.json")]
        [TestCase("raw/api/JaN/delta_extract_1.json")]
        [TestCase("raw/api/jan/delta_EXTRACT_1.json")]
        [TestCase("raw/DataBase/feb/extract_2.csv")]
        [TestCase("raw/database/feb/Extract_2.csv")]
        public void Given_PathWithIncorrectCase_Should_ThrowWhenMultipleFilesMatch(string testPath)
        {
            var exception = Assert.CatchAsync(() => Sut.CheckPathAsync(testPath, false));
            Assert.That(exception, Is.TypeOf(typeof(Exception)));
        }
    }
}