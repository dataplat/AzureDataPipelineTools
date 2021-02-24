using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Azure.Datafactory.Extensions.Functions
{
    public class Filter
    {
        public string PropertyName { get; set; }
        public string Operator { get; set; }
        public string Value { get; set; }
        public bool IsValid { get; set; } = false;
        public string ErrorMessage { get; set; } = null;





        private static IEnumerable<PropertyInfo> DataLakeFileProperties { get; } = typeof(DataLakeFile).GetProperties();
        private static String FilterableProperties { get; } = string.Join(", ", typeof(DataLakeFile).GetProperties().Select(p => p.Name).OrderBy(p => p));

        public static Filter ParseFilter(string columnName, string filter, ILogger log)
        {
            // Clean up the column name by removing the filter[...] parts
            var columnNameClean = columnName[7..^1];

            // Use the column name to find the target property
            var targetProperty = DataLakeFileProperties.FirstOrDefault(p => p.Name.ToLower() == columnNameClean.ToLower());
            DataLakeFileProperties.Select(p => p.Name.ToLower());
            // First validate the property exists on the target type
            if (targetProperty == null)
            {
                var error = $"The filter column '{columnNameClean}' does not exist. Filter columns must be one of the following: {FilterableProperties}.";
                log?.LogWarning(error);
                return new Filter
                {
                    PropertyName = columnNameClean,
                    ErrorMessage = error
                };
            }

            // Now validate and split the filter string
            var operatorRegex = new Regex("^(eq|ne|lt|gt|le|ge|like):(.+)$");
            var operatorMatches = operatorRegex.Matches(filter);
            if (operatorMatches.Count != 1 && operatorMatches.FirstOrDefault()?.Groups.Count != 3)
            {
                var error = $"The filter string '{filter}' for column '{columnNameClean}' is not valid. It should match the format '{operatorRegex}'";
                log?.LogWarning(error);
                return new Filter
                {
                    PropertyName = columnNameClean,
                    ErrorMessage = error
                };
            }

            var op = operatorMatches.FirstOrDefault().Groups[1].Value;
            var val = operatorMatches.FirstOrDefault().Groups[2].Value;

            // Now we check the filter string can be parsed into the correct type
            var propertyType = targetProperty.PropertyType.Name;
            var isValueParseable = false;
            switch (propertyType)
            {
                case nameof(String):
                    isValueParseable = true;
                    break;
                case nameof(Boolean):
                    bool boolVal;
                    isValueParseable = bool.TryParse(val, out boolVal);
                    break;
                case nameof(Int16):
                case nameof(Int32):
                case nameof(Int64):
                    long longVal;
                    isValueParseable = long.TryParse(val, out longVal);
                    break;
                case nameof(Decimal):
                case nameof(Double):
                    double doubleVal;
                    isValueParseable = double.TryParse(val, out doubleVal);
                    break;
                case nameof(DateTime):
                    DateTime dateTimeVal;
                    isValueParseable = DateTime.TryParse(val, out dateTimeVal);
                    break;
            }

            var parseError = isValueParseable ? null : $"The filter '{val}' cannot be applied to the property '{columnNameClean}' as it cannot be cast to an '{propertyType}'";
            if (!isValueParseable)
                log?.LogWarning(parseError);

            return new Filter
            {
                PropertyName = columnNameClean,
                Operator = op,
                Value = val,
                IsValid = isValueParseable,
                ErrorMessage = parseError
            };
        }

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
