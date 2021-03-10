namespace Azure.Datafactory.Extensions.DataLake.Model
{
    public struct DataLakeFile
    {
        public string Name { get; set; }
        public string Directory { get; set; }
        public string FullPath { get; set; }
        public string Url { get; set; }
        public bool IsDirectory { get; set; }
        public long ContentLength { get; set; }
        public string LastModified { get; set; }
    }
}