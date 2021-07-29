using System;
using Microsoft.Extensions.Logging;
using Moq;

namespace DataPipelineTools.Tests.Common
{
    // Based once code / ideas here: https://adamstorr.azurewebsites.net/blog/mocking-ilogger-with-moq
    public static class MockILoggerExtensions
    {
        public static Mock<ILogger<T>> VerifyLogging<T>(this Mock<ILogger<T>> logger, LogLevel expectedLogLevel = LogLevel.Debug, Times? times = null)
        {
            return logger.VerifyLogging( null, false, expectedLogLevel, times);
        }

        public static Mock<ILogger<T>> VerifyLogging<T>(this Mock<ILogger<T>> logger, string expectedMessage, bool verifyExpectedMessage = true, LogLevel expectedLogLevel = LogLevel.Debug, Times? times = null)
        {
            times ??= Times.Once();

            Func<object, Type, bool> state = verifyExpectedMessage ? 
                (v, t) => v.ToString()?.CompareTo(expectedMessage) == 0 : 
                state = (v, t) => true;

            logger.Verify(
                x => x.Log(
                    It.Is<LogLevel>(l => l == expectedLogLevel),
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => state(v, t)),
                    It.IsAny<Exception>(),
                    It.Is<Func<It.IsAnyType, Exception, string>>((v, t) => true)), (Times)times);

            return logger;
        }
    }
}
