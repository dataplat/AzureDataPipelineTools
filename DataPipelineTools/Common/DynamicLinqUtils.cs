using System.Collections.Generic;
using System.Linq.Dynamic.Core.CustomTypeProviders;
using System.Text.RegularExpressions;

namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    [DynamicLinqType]
    public static class DynamicLinqUtils
    {
        private static Dictionary<string, Regex> RegexObjectCache { get; } = new Dictionary<string, Regex>();

        public static bool IsRegexMatch(string value, string regex)
        {
            if (!RegexObjectCache.ContainsKey(regex))
                RegexObjectCache.Add(regex, new Regex(regex, RegexOptions.IgnoreCase));

            return RegexObjectCache[regex].Matches(value).Count > 0;
        }
    }
}
