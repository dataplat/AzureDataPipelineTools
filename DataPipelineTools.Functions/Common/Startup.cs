using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.Functions.Common;
using SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake;

[assembly: FunctionsStartup(typeof(Startup))]    
namespace SqlCollaborative.Azure.DataPipelineTools.Functions.Common
{
    public class Startup : FunctionsStartup
    {
        /// <summary>
        /// Configure application settings for the Azure Functions Application using local.settings.json, secrets.settings.json and environment variables.
        /// </summary>
        /// <param name="builder">IFunctionsConfigurationBuilder used to configure the application.</param>
        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
            base.ConfigureAppConfiguration(builder);

            var basePath = builder.GetContext().ApplicationRootPath;
            ConfigureAppConfiguration(builder.ConfigurationBuilder, basePath);
        }

        /// <summary>
        /// Configure application settings for the Azure WebJobs using local.settings.json, secrets.settings.json and environment variables.
        /// </summary>
        /// <param name="builder">IConfigurationBuilder  used to configure the application.</param>
        /// <param name="basePath">The root folder for the application. The local.settings.json, secrets.settings.json files should be in this location.</param>
        
        // Add in this override allows creating a WebJobs configuration from the unit tests so we can get a correctly configured instance of the classes
        // that help us access key vault.
        internal void ConfigureAppConfiguration(IConfigurationBuilder builder, string basePath)
        {
            var config = builder
                .SetBasePath(basePath)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("secrets.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();
        }

        /// <summary>
        /// Configure the DI container for the Azure Functions Application
        /// </summary>
        /// <param name="builder">IFunctionsHostBuilder</param>
        public override void Configure(IFunctionsHostBuilder builder)
        {
            //  ** Registers the ILogger instance **
            builder.Services.AddLogging();

            builder.Services.AddTransient(typeof(DataLakeConfigFactory));
            builder.Services.AddTransient<IDataLakeClientFactory, DataLakeClientFactory>();
            builder.Services.AddTransient(typeof(DataLakeServiceFactory));
            
            builder.Services.AddSingleton(typeof(AzureIdentityHelper));
            builder.Services.AddSingleton(typeof(KeyVaultHelpers));

            builder.Services.AddOptions<AzureEnvironmentConfig>().Configure<IConfiguration>((settings, configuration) =>
                {
                    var section = configuration.GetSection("AzureEnvironmentConfig");
                    section.Bind(settings);
                }
            );


        }
    }

}