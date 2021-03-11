using Azure.Datafactory.Extensions.Functions;
using System.Collections.Generic;

namespace Azure.Datafactory.Extensions.DataLake.Model
{
    public class DataLakeGetItemsConfig
    {
        public string Directory { get; set; }
        public bool IgnoreDirectoryCase { get; set; }
        public bool Recursive { get; set; }
        public string OrderByColumn { get; set; }
        public bool OrderByDescending { get; set; }
        public int Limit { get; set; }
        public IEnumerable<Filter<DataLakeFile>> Filters { get; set; }
    }
}