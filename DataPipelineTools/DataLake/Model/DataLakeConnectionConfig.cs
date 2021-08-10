using System;
using System.Drawing;

namespace SqlCollaborative.Azure.DataPipelineTools.DataLake.Model
{
    public class DataLakeConnectionConfig
    {
        public string Account { get; set; }
        public string Container { get; set; }
        public string ServicePrincipalClientId { get; set; }
        public string ServicePrincipalClientSecretPlaintext { get; set; }
        public string ServicePrincipalClientSecretKeyVault  { get; set; }
        public string SasTokenPlaintext  { get; set; }
        public string SasTokenKeyVault  { get; set; }
        public string AccountKeySecretPlaintext  { get; set; }
        public string AccountKeySecretKeyVault { get; set; }
        public string BaseUrl { get { return $"https://{Account}.dfs.core.windows.net/{Container}"; } }

        public string KeyVault { get; set; }

        public AuthType AuthType
        {
            get
            {
                if (!string.IsNullOrWhiteSpace(ServicePrincipalClientId) || !string.IsNullOrWhiteSpace(ServicePrincipalClientSecretPlaintext) || !string.IsNullOrWhiteSpace(ServicePrincipalClientSecretKeyVault))
                    return AuthType.UserServicePrincipal;

                if (!string.IsNullOrWhiteSpace(SasTokenPlaintext) || !string.IsNullOrWhiteSpace(SasTokenKeyVault))
                    return AuthType.SasToken;


                if (!string.IsNullOrWhiteSpace(AccountKeySecretPlaintext) || !string.IsNullOrWhiteSpace(AccountKeySecretKeyVault))
                    return AuthType.AccountKey;

                return AuthType.FunctionsServicePrincipal;
            }
        }
    }
}