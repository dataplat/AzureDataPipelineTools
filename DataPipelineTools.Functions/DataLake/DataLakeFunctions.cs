using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using SqlCollaborative.Azure.DataPipelineTools.DataLake;
using SqlCollaborative.Azure.DataPipelineTools.Functions.Common;

namespace SqlCollaborative.Azure.DataPipelineTools.Functions.DataLake
{
    public partial class DataLakeFunctions: FunctionsBase
    {
        private readonly DataLakeConfigFactory _configFactory;
        private readonly IDataLakeClientFactory _clientFactory;
        private readonly DataLakeServiceFactory _serviceFactory;
        public DataLakeFunctions(ILogger<DataLakeFunctions> logger, DataLakeConfigFactory configFactory, IDataLakeClientFactory clientFactory, DataLakeServiceFactory serviceFactory):
            base(logger)
        {
            _configFactory = configFactory;
            _clientFactory = clientFactory;
            _serviceFactory = serviceFactory;
        }




        [FunctionName("DataLakeGetItems")]
        public async Task<IActionResult> DataLakeGetItems(
            [HttpTrigger(AuthorizationLevel.Function, "get" /*, "post"*/, Route = null)] HttpRequest req)
        {
            req.GetQueryParameterDictionary();

            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            _logger.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");
            try
            {
                var dataLakeConfig = _configFactory.GetDataLakeConfig(req);
                var getItemsConfig = _configFactory.GetItemsConfig(req);

                if (string.IsNullOrWhiteSpace(dataLakeConfig.AccountUri))
                    throw new ArgumentException($"Parameter 'accountUri' with value '{dataLakeConfig.AccountUri}' not found. Check the URI is correct.");

                if (getItemsConfig.Directory == null)
                    throw new ArgumentException($"Parameter 'directory' is required.");

                var client = _clientFactory.GetDataLakeClient(dataLakeConfig);
                var controller = _serviceFactory.CreateDataLakeService(client);
                
                var responseJson = GetTemplateResponse(dataLakeConfig, getItemsConfig);
                var items = await controller.GetItemsAsync(dataLakeConfig, getItemsConfig);
                foreach (var item in items)
                    responseJson.Add(item.Key, item.Value);

                return (IActionResult)new OkObjectResult(responseJson);
            }
            catch (ArgumentException ex)
            {
                _logger.LogError(ex, ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message); // The simple message goes in the trace, but the full exception details are in the exception logging in Application Insights
                return new BadRequestObjectResult("An error occurred, see the Azure Function logs for more details");
            }
        }



        [FunctionName("DataLakeCheckPathCase")]
        public async Task<IActionResult> DataLakeCheckPathCase(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            _logger.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");

            try
            {
                var dataLakeConfig = _configFactory.GetDataLakeConfig(req);
                var getItemsConfig = _configFactory.GetCheckPathCaseConfig(req);

                if (string.IsNullOrWhiteSpace(dataLakeConfig.AccountUri))
                    throw new ArgumentException($"Parameter 'accountUri' with value '{dataLakeConfig.AccountUri}' not found. Check the URI is correct.");

                if (getItemsConfig.Path == null)
                    throw new ArgumentException($"Parameter 'path' is required.");

                var client = _clientFactory.GetDataLakeClient(dataLakeConfig);
                var dataLakeService = _serviceFactory.CreateDataLakeService(client);

                var validatedPath = await dataLakeService.CheckPathAsync(getItemsConfig.Path, true);

                // If multiple files match, the function will throw and the catch block will return a BadRequestObjectResult
                // If the path could not be found as a directory, try for a file...
                validatedPath ??= await dataLakeService.CheckPathAsync(getItemsConfig.Path, false);

                var responseJson = GetTemplateResponse(dataLakeConfig, getItemsConfig);
                responseJson.Add("validatedPath", validatedPath);

                return validatedPath != null ?
                    (IActionResult)new OkObjectResult(responseJson):
                    (IActionResult)new NotFoundObjectResult(responseJson);
            }
            catch (ArgumentException ex)
            {
                _logger.LogError(ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message); // The simple message goes in the trace, but the full exception details are in the exception logging in Application Insights
                return new BadRequestObjectResult("An error occurred, see the Azure Function logs for more details");
            }
        }
        
    }
}
