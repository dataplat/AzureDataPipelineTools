namespace Azure.Datafactory.Extensions.DataLake.Model
{
    public class DataLakeConfig
    {
        public string AccountUri { get; set; }
        public string Container { get; set; }
        public string BaseUrl { get { return $"https://{AccountUri}.dfs.core.windows.net/{Container}"; } }
    }
}