using DataPipelineTools.Tests.Common;
using NUnit.Framework;

namespace DataPipelineTools.Functions.Tests.Integration.DataLake
{
    [TestFixture]
    [Category(nameof(TestType.IntegrationTest))]
    [Parallelizable(ParallelScope.Children)]
    public class DataLakeGetItemsIntegrationTests : DataLakeIntegrationTestBase
    {
        protected override string FunctionUri => $"{FunctionsAppUrl}/api/DataLake/GetItems";

        
    }
}