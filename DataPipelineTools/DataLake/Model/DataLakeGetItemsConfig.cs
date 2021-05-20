using System.Collections.Generic;
using SqlCollaborative.Azure.DataPipelineTools.Common;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public class DataLakeGetItemsConfig
    {
        public string Directory { get; set; }
        public bool IgnoreDirectoryCase { get; set; } = true;
        public bool Recursive { get; set; } = true;
        public string OrderByColumn { get; set; }
        public bool OrderByDescending { get; set; }
        public int Limit { get; set; } = 0;
        public IEnumerable<Filter<DataLakeItem>> Filters { get; set; } = new Filter<DataLakeItem>[0];
    }
}