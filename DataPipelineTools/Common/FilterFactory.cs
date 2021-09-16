using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;


namespace SqlCollaborative.Azure.DataPipelineTools.Common
{
    public class FilterFactory<T>
    {
        private static IEnumerable<PropertyInfo> TypeProperties => typeof(T).GetProperties();

        private static string FilterableProperties => string.Join(", ", TypeProperties.Select(p => p.Name).OrderBy(p => p));
        
        
        public static Filter<T> Create(string columnName, string filter, ILogger log)
        {
            

            // Use the column name to find the target property
            var targetProperty = TypeProperties.FirstOrDefault(p => p.Name.ToLower() == columnName?.ToLower());
            //TypeProperties.Select(p => p.Name.ToLower());
            // First validate the property exists on the target type
            if (targetProperty == null)
            {
                var error = $"The filter column '{columnName ?? "null"}' does not exist. Filter columns must be one of the following: {FilterableProperties}.";
                log?.LogWarning(error);
                return new Filter<T>
                {
                    PropertyName = columnName,
                    ErrorMessage = error
                };
            }

            // Now validate and split the filter string
            var operatorRegex = new Regex("^(eq|ne|lt|gt|le|ge|like):(.+)$");
            var operatorMatches = operatorRegex.Matches(filter ?? string.Empty);
            if (operatorMatches.Count != 1 && operatorMatches.FirstOrDefault()?.Groups.Count != 3)
            {
                var error = $"The filter string '{filter ?? "null"}' for column '{columnName}' is not valid. It should match the format '{operatorRegex}'";
                log?.LogWarning(error);
                return new Filter<T>
                {
                    PropertyName = columnName,
                    ErrorMessage = error
                };
            }

            var op = operatorMatches.FirstOrDefault().Groups[1].Value;
            var val = operatorMatches.FirstOrDefault().Groups[2].Value;

            // If the operator is like, but it's not a string type it's not a valid filter
            var propertyType = targetProperty.PropertyType;
            if (propertyType.Name != nameof(String) && op == "like")
            {
                var error = $"The filter column '{columnName}' is not a string, and cannot be use with the 'like' operator.";
                log?.LogWarning(error);
                return new Filter<T>
                {
                    PropertyName = columnName,
                    ErrorMessage = error
                };
            }

            // Now we check the filter string can be parsed into the correct type
            var isValueParseable = false;
            switch (propertyType.Name)
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
                case nameof(DateTimeOffset):
                    DateTimeOffset dateTimeOffsetVal;
                    isValueParseable = DateTimeOffset.TryParse(val, out dateTimeOffsetVal);
                    break;
            }

            var parseError = isValueParseable ? null : $"The filter '{val}' cannot be applied to the property '{columnName}' as it cannot be cast to a '{propertyType.Name}'";
            if (!isValueParseable)
                log?.LogWarning(parseError);

            return new Filter<T>
            {
                PropertyName = columnName,
                PropertyType = propertyType,
                Operator = op,
                Value = val,
                IsValid = isValueParseable,
                ErrorMessage = parseError
            };
        }
    }
}
