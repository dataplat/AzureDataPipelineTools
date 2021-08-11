using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Storage.Files.DataLake;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public interface IDataLakeClientFactory
    {
        public DataLakeFileSystemClient GetDataLakeClient(IDataLakeConnectionConfig dataLakeConnectionConfig);
    }
}
