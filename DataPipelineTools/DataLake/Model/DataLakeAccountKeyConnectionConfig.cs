using System;
using Azure.Storage.Files.DataLake;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public class DataLakeAccountKeyConnectionConfig : IDataLakeConnectionConfig
    {
        public string Account { get; set; }
        public string Container { get; set; }
        public string AccountKeySecret { get; set; }
        public string BaseUrl
        {
            get { return $"https://{Account}.dfs.core.windows.net/{Container}"; }
        }
        public string KeyVault { get; set; }
    }
}