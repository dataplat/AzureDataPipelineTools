# Data Lake Functions

These functions are used to help interact with Azure Data lake in a more seamless manner.

## Generic Parameters
The following parameters are use by all data lake functions.
| Parameter | Type | Description |
| ----------- | ----------- | ----------- |
| account | String | The name of the Azure Storage Account. |
| container | String | The name of the Azure Data Lake Container. |
| path | String | The path to the file/folder within the data lake. For the root path use '/'.

---

## Authentication Parameters
The data lake functions support authenticating to the data lake using the following options.
- The Service Principal (SPN) of the deployed Azure Functions App
- A user specified Service Principal. The service principal must be in the same tenant as the Azure Functions App.
- A Shared Access Signature (SAS) token.
- The Azure Storage Account key

You must only specify the parameters required for one of these authentication types for each function call. Specifying multiple types will return an error. The types are in the table below.

>Note: You can optionally provide the parameter `keyVault`. This allows the name of the secret in a key vault to be passed instead of a secret value. For this to work, the deployed Azure Functions App service principal must be granted access to read the secrets from the specified key vault.


| Auth Type | Parameter | Required | Format | Description |
| - | - | - | - | - |
| Azure Functions SPN | N/A |  |  | To use the Azure Functions App Service Principal you do not need to provide any authentication parameters. |
| User SPN | spnClientId | Yes | GUID | The client id of the SPN you want to use to authenticate calls to the data lake. Must be in the same tenant as the Azure Functions application. |
|  | spnClientSecret | Yes | String | The client secret for the SPN. This is either the secret for the SPN, or, if the `keyVault` parameter is specified is the name of the secret value in that Azure Key Vault. |
|  | keyVault | No | String | The name of the Azure Key Vault that contains the secret specified by the parameter `spnClientSecret`. |
| SAS Token | sasToken | Yes | String | The SAS token for accessing storage account. Must be in the same tenant as the Azure Functions application. |
|  | keyVault | No | String | The name of the Azure Key Vault that contains the secret specified by the parameter `spnClientSecret`. |
| Storage Account key | accountKey | Yes | String | The storage account key. This is either the secret for the SPN, or, if the `keyVault` parameter is specified is the name of the secret value in that Azure Key Vault. |
|  | keyVault | No | String | The name of the Azure Key Vault that contains the secret specified by the parameter `spnClientSecret`. |

### Responses
Successful calls will return 200 (OK), failed calls will return 400 (Bad Request). In both cases the return body will be a JSON object with the result set or error details.

---

## CheckPathCase
`https://<YourAzureFunctionsApp>.azurewebsites.net/api/DataLake/CheckPathCase`

When dealing with metadata driven processing, it is easy for a mistake in the path case in metadata to cause errors when accessing the lake, because Azure Data Lake paths are case sensitive. This function can be used to validate a path. If the path does not exist, the function checks for all paths that could match but with different casing. If one is found, that is returned, or if none/multiple matches are found an error is returned.

### Parameters
The [generic](#generic-parameters) and [authentication](#authentication-parameters) parameters above are mandatory, and are the only parameters required.

### Return Values
Returned values JSON objects. Below is an example of a successful call:
```
{
    "invocationId": "37c34c08-bb41-4176-b542-8adc3617f28f",
    "debugInfo": {
        "informationalVersion": "1.0.0"
    },
    "storageContainerUrl": "https://<YourAzureFunctionsApp>.dfs.core.windows.net/myContainer",
    "authType": "FunctionsServicePrincipal",
    "parameters": {
        "Path": "TESTDATA"
    },
    "validatedPath": "TestData"
}
```

An example of a call with a mandatory parameter missing:
```
{
  "invocationId":"e28970da-13f2-46be-848e-c25de54539a1",
  "error": "Mandatory parameter 'account' was not provided."
}
```

An example of a call with a mandatory parameter missing:
```
{
  "invocationId":"e28970da-13f2-46be-848e-c25de54539a1",
  "error": "An error occurred, see the Azure Function logs for more details"
}
```
---

## GetItems
`https://<YourAzureFunctionsApp>.azurewebsites.net/api/DataLake/GetItems`

This function is intended as an improved version of the ADF *Get Metadata* activity. It can be called from ADF using the *Execute Function* activity, using the following parameters.

### Parameters
The [generic](#generic-parameters) and [authentication](#authentication-parameters) parameters above are mandatory. In addition the following optional parameters can be specified.

| Parameter | Type | Description |
| ----------- | ----------- | ----------- |
| ignoreDirectoryCase | Bool | This will call checkPathCase on the path parameter before getting the items. This means that if the path is incorrectly cased, but there is only one path that matches when looking case-insensitively, the function will return results. |
| limit | String | The number of results to return. |
| filter[PropertyName] | String | This allows filtering the results using the properties of items in the result set. Format is `operator:value` allowing flexibility building filters. Valid `PropertyName` options are any of the returned properties of a file or folder. The `like` operator matching supports full .Net style regular expressions. |
| orderBy | String | The property to order the result set by. Valid properties are those of the returned json for each object, eg `LastModified`. |
| orderByDesc | Bool | Sorts the results descending if true. Default when not specified is false. Used with ordering on `LastModified` and a limit of 1 will find the most recent file matching a filter. |
| recursive | Bool | Look through folders recursively. |

### Filter Types
When providing a filter parameter, there are a number of operators that can be used. The format is `filter[PropertyName]=operator:value`.
| Operator | Description |
| eq | Check if the property named `PropertyName' is equal to the value provided |
| ne | Check if the property named `PropertyName' is not equal to the value provided |
| lt | Check if the property named `PropertyName' is less than the value provided |
| gt | Check if the property named `PropertyName' is greater than to the value provided |
| le | Check if the property named `PropertyName' is less than or equal to the value provided |
| ge | Check if the property named `PropertyName' is greater than or equal to the value provided |
| like | Check if the property named `PropertyName' matches the pattern provided. You can use `*` for wildcards, but .Net sytle regular expressions are also supported. |

#### Examples:
Filter the results to return only files:
```
filter[IsDirectory]=eq:false
```

Filter the results to return only folders:
```
filter[IsDirectory]=eq:true
```

Filter the results to return files and folders modified since 2021-09-01 14:00:00:
```
filter[LastModified]=ge:2021-09-01 14:00:00
```

Filter the results to return only parquet files using a wildcard:
```
filter[Name]=like:*.parquet
```

Filter the results to return only files or folders starting with *'abc'* or *'xzy'* using a regular expression:
```
filter[Name]=like:(abc|xyz)*
```

We can also combine multiple filter using `&`. for example to find files modified in september 2021. We could add a orderBy to this too to allow processing files in a date range in order...
```
filter[IsDirectory]=eq:false&filter[LastModified]=ge:2021-09-01 00:00:00&filter[LastModified]=lt:2021-10-01 00:00:00
```


> Note: When using filters, you must URL encode any special characters when sending the request. This is especially important for regular expression filters.

### Return Values
If parameters are used incorrectly, the returned JSON will have the error details. All other errors return a simple, generic error message, but the Azure Functions app will have detailed logging available for the execution.


Returned values JSON objects. Below is an example of a successful call:
```
{
    "invocationId": "c21b69dc-9e76-42da-9953-ec63519f378a",
    "debugInfo": {
        "informationalVersion": "1.0.0"
    },
    "storageContainerUrl": "https://<YourAzureFunctionsApp>.dfs.core.windows.net/myContainer",
    "clientId": "f4b9d6e7-2753-44c6-a579-0bd77caa287d",
    "authType": "UserServicePrincipal",
    "parameters": {
        "Path": "/",
        "IgnoreDirectoryCase": false,
        "Recursive": true,
        "OrderByColumn": null,
        "OrderByDescending": false,
        "Limit": 0,
        "Filters": [
            {
                "PropertyName": "IsDirectory",
                "Operator": "eq",
                "Value": "true",
                "ErrorMessage": null
            }
        ]
    },
    "fileCount": 3,
    "files": [
        {
            "Name": "TestData",
            "Directory": "",
            "FullPath": "TestData",
            "Url": "https://<YourAzureDataLake>.dfs.core.windows.net/myContainer/TestData",
            "IsDirectory": true,
            "ContentLength": 0,
            "LastModified": "2021-09-09T17:23:14Z"
        },
        {
            "Name": "TestFolder1",
            "Directory": "TestData",
            "FullPath": "TestData/TestFolder1",
            "Url": "https://<YourAzureDataLake>.dfs.core.windows.net/myContainer/TestData/TestFolder1",
            "IsDirectory": true,
            "ContentLength": 0,
            "LastModified": "2021-09-09T17:23:14Z"
        },
        {
            "Name": "TestFolder2",
            "Directory": "TestData",
            "FullPath": "TestData/TestFolder2",
            "Url": "https://<YourAzureDataLake>.dfs.core.windows.net/myContainer/TestData/TestFolder2",
            "IsDirectory": true,
            "ContentLength": 0,
            "LastModified": "2021-09-09T17:23:14Z"
        }
    ]
}
```
> Note: If not files are returned, then the `fileCount` will be 0, and the `files` property will be an empty array.

An example of a call with a mandatory parameter missing:
```
{
  "invocationId":"e28970da-13f2-46be-848e-c25de54539a1",
  "error": "Mandatory parameter 'account' was not provided."
}
```

An example of a call with a mandatory parameter missing:
```
{
  "invocationId":"e28970da-13f2-46be-848e-c25de54539a1",
  "error": "An error occurred, see the Azure Function logs for more details"
}
```


