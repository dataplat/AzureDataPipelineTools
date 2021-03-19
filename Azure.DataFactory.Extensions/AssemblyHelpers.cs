using System.Linq;
using System.Reflection;
using Newtonsoft.Json.Linq;

namespace Azure.Datafactory.Extensions.Functions
{
    public class AssemblyHelpers
    {
        public static JObject GetAssemblyVersionInfoJson()
        {
            var callingAssembly = Assembly.GetCallingAssembly();
            return GetAssemblyVersionInfoJson(callingAssembly);
        }

        public static JObject GetAssemblyVersionInfoJson(Assembly assembly)
        {
            string buildDate = assembly.GetCustomAttributes().OfType<AssemblyMetadataAttribute>().FirstOrDefault(a => a.Key == "BuildDate")?.Value;
            string informationalVersion = assembly.GetCustomAttributes().OfType<AssemblyInformationalVersionAttribute>().FirstOrDefault()?.InformationalVersion;

            var jobj = new JObject();
            if (buildDate != null)
                jobj.Add("buildDate", buildDate);

            if (informationalVersion != null)
                jobj.Add("informationalVersion", informationalVersion);

            return jobj;
        }

    }
}
