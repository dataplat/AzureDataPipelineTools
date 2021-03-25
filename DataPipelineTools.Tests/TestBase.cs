using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using DataPipelineTools.Tests.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace DataPipelineTools.Tests
{
    public abstract class TestBase
    {
        //protected abstract T ParseCsv(string csvLine);

        protected IEnumerable<T> GetTestData<T>(string delimiter, Func<Dictionary<string, string>, T> conversionFunc)
        {
            var thisAssembly = Assembly.GetExecutingAssembly();
            var assemblyPath = thisAssembly.Location;
            var assemblyName = thisAssembly.GetName().Name;
            var nameSpace = GetType().Namespace;

            var testDataRelativePath = nameSpace.Replace(assemblyName, "").Replace(".", "\\").TrimStart('\\');
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
