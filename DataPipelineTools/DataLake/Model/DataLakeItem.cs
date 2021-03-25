using System;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public struct DataLakeItem
    {
        public string Name { get; set; }
        public string Directory { get; set; }
        public string FullPath => $"{Directory}{(String.IsNullOrEmpty(Directory) ? "" : "/")}{Name}";

        public string Url { get; set; }
        public bool IsDirectory { get; set; }
        public long ContentLength { get; set; }
        public DateTimeOffset LastModified { get; set; }
    }
}