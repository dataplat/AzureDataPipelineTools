using System;
using System.IO;
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
    public class DataLakeFunctions: FunctionsBase
    {
        private readonly DataLakeConfigFactory _configFactory;
        private readonly IDataLakeClientFactory _clientFactory;
        private readonly DataLakeServiceFactory _serviceFactory;

        public static readonly string PathNotFoundErrorMessage =
            "Path could not be found, or the authentication method used does not have access to the path.";
        public DataLakeFunctions(ILogger<DataLakeFunctions> logger, DataLakeConfigFactory configFactory, IDataLakeClientFactory clientFactory, DataLakeServiceFactory serviceFactory):
            base(logger)
        {
            _configFactory = configFactory;
            _clientFactory = clientFactory;
            _serviceFactory = serviceFactory;
        }

        [FunctionName("DataLake-GetItems")]
        public async Task<IActionResult> DataLakeGetItems(
            [HttpTrigger(AuthorizationLevel.Function, "get" /*, "post"*/, Route = "DataLake/GetItems")] HttpRequest req, ExecutionContext context)
        {
            req.GetQueryParameterDictionary();

            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            _logger.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");
            try
            {
                var dataLakeConfig = _configFactory.GetDataLakeConnectionConfig(req);
                var getItemsConfig = _configFactory.GetItemsConfig(req);

                var client = _clientFactory.GetDataLakeClient(dataLakeConfig);
                var controller = _serviceFactory.CreateDataLakeService(client);
                
                var responseJson = GetTemplateResponse(dataLakeConfig, getItemsConfig, context);
                var items = await controller.GetItemsAsync(dataLakeConfig, getItemsConfig);
                foreach (var item in items)
                    responseJson.Add(item.Key, item.Value);

                return (IActionResult)new OkObjectResult(responseJson);
            }
            catch (Exception ex) when (ExceptionTypeReturnsDetailedError(ex))
            {
                _logger.LogError(ex, ex.Message);
                return new BadRequestObjectResult($"{{\n  \"invocationId\":\"{context.InvocationId}\",\n  \"error\": \"{ex.Message}\"\n}}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message); // The simple message goes in the trace, but the full exception details are in the exception logging in Application Insights
                return new BadRequestObjectResult($"{{\n  \"invocationId\":\"{context.InvocationId}\",\n  \"error\": \"An error occurred, see the Azure Function logs for more details\"\n}}");
            }
        }



        [FunctionName("DataLake-CheckPathCase")]
        public async Task<IActionResult> DataLakeCheckPathCase(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "DataLake/CheckPathCase")] HttpRequest req, ExecutionContext context)
        {
            var userAgentKey = req.Headers.Keys.FirstOrDefault(k => k.ToLower() == "user-agent" || k.ToLower() == "useragent");
            _logger.LogInformation($"C# HTTP trigger function processed a request [User Agent: { (userAgentKey == null ? "Unknown" : req.Headers[userAgentKey].ToString()) }].");

            try
            {
                var dataLakeConfig = _configFactory.GetDataLakeConnectionConfig(req);
                var getItemsConfig = _configFactory.GetCheckPathCaseConfig(req);

                if (string.IsNullOrWhiteSpace(dataLakeConfig.Account))
                    throw new ArgumentException($"Parameter 'accountUri' with value '{dataLakeConfig.Account}' not found. Check the URI is correct.");

                if (getItemsConfig.Path == null)
                    throw new ArgumentException($"Parameter 'path' is required.");

                var client = _clientFactory.GetDataLakeClient(dataLakeConfig);
                var dataLakeService = _serviceFactory.CreateDataLakeService(client);

                var validatedPath = await dataLakeService.CheckPathAsync(getItemsConfig.Path, true);

                // If multiple files match, the function will throw and the catch block will return a BadRequestObjectResult
                // If the path could not be found as a directory, try for a file...
                validatedPath ??= await dataLakeService.CheckPathAsync(getItemsConfig.Path, false);

                var responseJson = GetTemplateResponse(dataLakeConfig, getItemsConfig, context);


                if (validatedPath != null)
                {
                    responseJson.Add("validatedPath", validatedPath);
                    return (IActionResult) new OkObjectResult(responseJson);
                }

                responseJson.Add("error", PathNotFoundErrorMessage);
                return (IActionResult)new BadRequestObjectResult(responseJson);
            }
            catch (Exception ex) when (ExceptionTypeReturnsDetailedError(ex))
            {
                _logger.LogError(ex.Message);
                return new BadRequestObjectResult($"{{\n  \"invocationId\":\"{context.InvocationId}\",\n  \"error\": \"{ex.Message}\"\n}}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message); // The simple message goes in the trace, but the full exception details are in the exception logging in Application Insights
                return new BadRequestObjectResult($"{{\n  \"invocationId\":\"{context.InvocationId}\",\n  \"error\": \"An error occurred, see the Azure Function logs for more details\"\n}}");
            }
        }

        private bool ExceptionTypeReturnsDetailedError(Exception ex)
        {
            return ex is ArgumentException ||
                   ex is MultipleMatchesException  ||
                   ex is DirectoryNotFoundException;
        }
    }
}
