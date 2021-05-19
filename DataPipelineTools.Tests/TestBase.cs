﻿using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Logging;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;

namespace DataPipelineTools.Tests
{
    public abstract class TestBase<T>: TestBase
    {
        protected ILogger<T> Logger => MockLogger.Object;
        protected readonly Mock<ILogger<T>> MockLogger;

        internal TestBase()
        {
            // Mock the logger to test where it is called
            MockLogger = new Mock<ILogger<T>>();
        }

        protected void SetupConsoleLogging()
        {
            MockLogger.Setup(
                    l => l.Log(
                        It.IsAny<LogLevel>(),
                        It.IsAny<EventId>(),
                        It.IsAny<It.IsAnyType>(),
                        It.IsAny<Exception>(),
                        (Func<It.IsAnyType, Exception, string>)It.IsAny<object>()))
                .Callback((IInvocation invocation) =>
                {
                    var logLevel = (LogLevel)invocation.Arguments[0];
                    var eventId = (EventId)invocation.Arguments[1];
                    var state = (IReadOnlyCollection<KeyValuePair<string, object>>)invocation.Arguments[2];
                    var exception = invocation.Arguments[3] as Exception;
                    var formatter = invocation.Arguments[4] as Delegate;
                    var formatterStr = formatter.DynamicInvoke(state, exception);
                    Console.WriteLine($"{logLevel} - {eventId.Id} - Testing - {formatterStr}");
                }).Verifiable();
        }
    }

    public abstract class TestBase
    {
        protected IEnumerable<T> GetTestData<T>(string delimiter, Func<Dictionary<string, string>, T> conversionFunc)
        {
            var thisAssembly = Assembly.GetExecutingAssembly();
            var assemblyPath = thisAssembly.Location;
            var assemblyName = thisAssembly.GetName().Name;
            var nameSpace = GetType().Namespace;

            var testDataRelativePath = nameSpace.Replace(assemblyName, "").Replace(".", Path.DirectorySeparatorChar.ToString()).TrimStart(Path.DirectorySeparatorChar);
            var testDataPath = Path.Combine(Path.GetDirectoryName(assemblyPath), testDataRelativePath, $"{GetType().Name}_Data_{typeof(T).Name}.csv");

            using (var fs = new FileStream(testDataPath, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                using (StreamReader sr = new StreamReader(fs))
                {
                    // The first line is the headers, so grab the values as keys
                    var keys = sr.ReadLine().Split(delimiter);

                    // Build a dictionary of the properties, as pass it to the parser function
                    while (!sr.EndOfStream)
                    {
                        var dict = new Dictionary<string, string>();
                        var values = sr.ReadLine().Split(delimiter);
                        for (int i = 0; i < keys.Length && i < values.Length; i++)
                        {
                            dict.Add(keys[i], values[i]);
                        }

                        yield return conversionFunc(dict);
                    }
                }
            }
        }
    }
}
