using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

[assembly: FunctionsStartup(typeof(SqlCollaborative.Azure.DataPipelineTools.Functions.Startup))]    
namespace SqlCollaborative.Azure.DataPipelineTools.Functions
{
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            //  ** Registers the ILogger instance **
            builder.Services.AddLogging();

            builder.Services.AddTransient(typeof(DataLakeConfigFactory));
            builder.Services.AddTransient<IDataLakeClientFactory, DataLakeClientFactory>();
            builder.Services.AddTransient(typeof(DataLakeControllerFactory));
        }
    }
}