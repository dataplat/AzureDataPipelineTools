using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;

[assembly: InternalsVisibleTo("DataPipelineTools.Tests")]
namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    public class Filter<T>
    {
        public string PropertyName { get; set; }
        public string Operator { get; set; }
        public string Value { get; set; }
        public bool IsValid { get; set; } = false;
        public string ErrorMessage { get; set; } = null;

        public string GetDynamicLinqString()
        {
            switch (Operator)
            {
                case "like":
                    var RegexMatchFunc = $"{nameof(DynamicLinqUtils)}.{nameof(DynamicLinqUtils.IsRegexMatch)}";
                    return $"{RegexMatchFunc}({PropertyName}, @0)";
                default:
                    return $"{PropertyName} {Operator} @0";
            }
        }

        public string GetDynamicLinqValue()
        {
            return Operator == "like" ? Value.Replace("*", ".+") : Value;
        }


        #region Newtonsoft.Json serialization methods
        public bool ShouldSerializeIsValid() => false;
        #endregion Newtonsoft.Json serialization methods
    }
}
