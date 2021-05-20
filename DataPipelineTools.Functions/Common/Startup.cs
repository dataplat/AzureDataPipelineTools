using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.Functions.Common;
using SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake;

[assembly: FunctionsStartup(typeof(Startup))]    
namespace SqlCollaborative.Azure.DataPipelineTools.Functions.Common
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            //  ** Registers the ILogger instance **
            builder.Services.AddLogging();

            builder.Services.AddTransient(typeof(DataLakeConfigFactory));
            builder.Services.AddTransient<IDataLakeClientFactory, DataLakeClientFactory>();
            builder.Services.AddTransient(typeof(DataLakeServiceFactory));
        }
    }
}