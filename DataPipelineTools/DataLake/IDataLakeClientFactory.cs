using SqlCollaborative.Azure.DataPipelineTools.DataLake.Model;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake
{
    public interface IDataLakeClientFactory
    {
        public DataLakeFileSystemClient GetDataLakeClient(IDataLakeConnectionConfig dataLakeConnectionConfig);
    }
}
