using System.Linq;
using System.Reflection;

namespace Azure.Datafactory.Extensions.Functions
{
    public class AssemblyHelpers
    {
        public static string GetAssemblyVersionInfoJson()
        {
            var callingAssembly = Assembly.GetCallingAssembly();
            return GetAssemblyVersionInfoJson(callingAssembly);
        }

        public static string GetAssemblyVersionInfoJson(Assembly assembly)
        {
            string buildDate = assembly.GetCustomAttributes().OfType<AssemblyMetadataAttribute>().FirstOrDefault(a => a.Key == "BuildDate")?.Value;
            string informationalVersion = assembly.GetCustomAttributes().OfType<AssemblyInformationalVersionAttribute>().FirstOrDefault()?.InformationalVersion;

            return "{" +
                  $"  \"buildDate\": \"{buildDate}\"," +
                  $"  \"informationalVersion\": \"{informationalVersion}\"" +
                   "}";
        }

    }
}
