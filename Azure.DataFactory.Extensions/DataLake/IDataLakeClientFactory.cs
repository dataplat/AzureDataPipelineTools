using Azure.Datafactory.Extensions.DataLake.Model;
using Azure.Identity;
using Azure.Storage.Files.DataLake;
using Microsoft.Extensions.Logging;
using System;

namespace Azure.Datafactory.Extensions.DataLake
{
    public interface IDataLakeClientFactory
    {
        public DataLakeFileSystemClient GetDataLakeClient(DataLakeConfig dataLakeConfig);
    }
}
