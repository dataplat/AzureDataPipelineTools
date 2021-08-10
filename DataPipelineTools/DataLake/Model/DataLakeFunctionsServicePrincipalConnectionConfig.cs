using System;
using Azure.Storage.Files.DataLake;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public class DataLakeFunctionsServicePrincipalConnectionConfig: IDataLakeConnectionConfig
    {
        public string Account { get; set; }
        public string Container { get; set; }
        public string BaseUrl { get { return $"https://{Account}.dfs.core.windows.net/{Container}"; } }
    }
}