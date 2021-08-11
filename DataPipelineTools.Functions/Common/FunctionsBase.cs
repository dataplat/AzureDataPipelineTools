using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using SqlCollaborative.Azure.DataPipelineTools.Common;
using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;

namespace SqlCollaborative.Azure.DataPipelineTools.Functions.Common
{
    public abstract class FunctionsBase
    {
        protected readonly ILogger _logger;

        public FunctionsBase(ILogger<FunctionsBase> logger)
        {
            _logger = logger;
        }

        protected JObject GetTemplateResponse(IDataLakeConnectionConfig dataLakeConnectionConfig, object parameters)
        {
            var assemblyInfo = AssemblyHelpers.GetAssemblyVersionInfoJson();

            var responseJson = new JObject();
            if (assemblyInfo.HasValues)
                responseJson.Add("debugInfo", assemblyInfo);

            if (dataLakeConnectionConfig.BaseUrl != null)
                responseJson.Add("storageContainerUrl", dataLakeConnectionConfig.BaseUrl);

            if (dataLakeConnectionConfig is DataLakeUserServicePrincipalConnectionConfig config)
                responseJson.Add("clientId", config.ServicePrincipalClientId);

            responseJson.Add("authType", dataLakeConnectionConfig.AuthType.ToString());

            var parametersJson = JObject.FromObject(parameters);
            responseJson.Add("parameters", parametersJson);

            return responseJson;
        }
    }
}
