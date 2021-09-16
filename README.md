![Build Status](https://github.com/sqlcollaborative/AzureDataPipelineTools/actions/workflows/test_azure_devtest_labs_integration.yml/badge.svg)

# AzureDataPipelineTools
A collection of extensions and helpers to make development of Azure Data Factory simpler and faster. This project was created to allow the community to share reliable methods to extend data factory functionality using Azure Functions or Data Factory patterns.

---

## Functions available include:
- **DataLakeGetItems**: Recursive finding of files within Azure Data Lake folders, including filtering, sorting and limiting results (eg, latest file only)
- **DataLakeCheckPathCase**: Checking of Azure Data Lake paths for correct casing, and auto fixing case where duplicates do not exist.

---

## How to use
Deploy the latest release into your Azure Functions instance using the [package deployment method](https://docs.microsoft.com/en-us/azure/azure-functions/run-functions-from-deployment-package). Alternatively, build and publish using either Visual Studio or a CI tool.



### Configuration and Environment Variables
The following environment variables must be configures in the Azure Functions instance.
| Environment Variable | Description |
| - | - |
| TENANT_ID | The tenant id in which the Azure Functions app exists. Used internally for some authentication scenarios. |



### Permissions
Depending on the functionality you are using, you may want the Azure Functions app's service principal (SPN) to have access to some of your Azure resources.
| Azure Resource Type | Categories | Description |
| ----------- | ----------- | ----------- |
| Data Lake | [Data Lake](./Docs/DataLake.md#data-lake-functions) | Allow the functions app SPN to access the data lake directly, without providing additional authentication parameters. See the [Microsoft docs](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control-model) for more info. |
| Key Vault | [Data Lake](./Docs/DataLake.md#data-lake-functions) | Allow the functions app SPN to access secrets stored in a Key Vault instance for authenticating against resources. This library only uses secrets, not keys or certificates, so you can restrict access to only secrets. No functions in this library will return secrets to a user.   |

> Note: Allowing the Azure Functions SPN to access services directly means any user or service that can call the function can get the information about that service that the functions in this project can return.



### Testing
You can use a REST client such as Postman to test the functions have been deployed and configured correctly.

### Calling from Azure Data Factory
Use the [Azure Functions activity](https://docs.microsoft.com/en-us/azure/data-factory/control-flow-azure-function-activity) to call the function. You can build the target URL using an expression, and access the results through expressions in subsequent activities.
> Note: To simplify permissions, you can grant the Azure Data Factory Service Principal permission to call the functions. This means you do not have to add the standard Azure Function `code` param to each call with the functions app secret.


---

## Included Functions

The functions are listed in the table below, along with a link to the documentation for that service category, and to the documentation for each individual function.

| Category | Name | Description |
| ----------- | ----------- | ----------- |
| [Data Lake](./Docs/DataLake.md#data-lake-functions) | [CheckPathCase](./Docs/DataLake.md#checkpathcase) | Checks the case for a given data lake path. Returns a corrected path if a single file matches with case differences, or an error for no match / multiple matches. |
|  | [GetItems](./Docs/DataLake.md#getitems#data-lake-functions) | Gets items in a data lake folder. Supports recursive queries, filtering and sorting. |

---

## Troubleshooting

For simple configuration errors in calling the functions, a BadRequest(400) is returned with the JSON body showing the error message. For internal errors within the function, the BadRequest(400) request body gives a generic error. In both cases the body also has the invocationId of the function call, and this can be used to get the log for that call from the Azure Functions UI, or from Azure Log Analytics if it is setup. This means troubleshooting calls from Azure Data Factory is relatively simple. Look at the output of the Azure Functions Activity, and work from there.

---

## Building and Contributing
Coming soon...