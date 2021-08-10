namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public enum AuthType
    {
        FunctionsServicePrincipal,
        UserServicePrincipal,
        SasToken,
        AccountKey
    }
}