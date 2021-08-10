using Azure.Storage.Files.DataLake;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public interface IDataLakeConnectionConfig
    {
        public string Account { get; set; }
        public string Container { get; set; }
        public string BaseUrl { get ; }
    }
}