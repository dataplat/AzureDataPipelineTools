# AzureDataPipelineTools
A collection of extensions and helpers to make development of Azure Data Factory simpler and faster. This project was created to allow the community to share reliable methods to extend data factory functionality using Azure Functions, or Data Factory patterns as reuable samples.

## Functions available include:
- **DataLakeGetItems**: Recursive finding of files within Azure Data Lake folders, including filtering, sorting and limiting results (eg, latest file only)
- **DataLakeCheckPathCase**: Checking of Azure Data Lake paths for correct casing, and auto fixing case where duplicates do not exist.



## How to use
Build and deploy the functions app in your Azure subscription. More details on this coming soon. Grant the functions app SPN access to the data lake resources you wish to access. Currently only list and read access is required.


## Functions Documentation

### DataLakeGetItems
This function is intended as an improved version of the ADF *Get Metadata* activity. I can be called from ADF using the *Execute Function* activity, using the following parameters.

| Parameter | Type | Description |
| ----------- | ----------- | ----------- |
| accountUri | String | The name of the Azure Storage Account. |
| container | String | The name of the Azure Data Lake Container. |
| directory | String | The directory to start from. |
| recursive | Bool | Look through folders recursively. |
| filter[Name] | String | This allows filtering the results by filename. Format is *(eq|ne|lt|gt|le|ge|like):\*.parquet*, allowing flexibility building filters. The like matching supports full .Net style regexes, but be careful that they are URL encoded correctly. |
| filter[IsDirectory] | Bool | This allows returning files or folder paths only. If not specified both files and folders are returned. |
| limit | String | The number of results to return. |
| orderBy | String | The propery to order the result set by. Valid properties are those of the returned json for each object, eg *LastModified*. |
| orderByDesc | Bool | Sorts the results descending if true. Defaullllt when not specified is false. Used with ordering on *LastModified* and a limit of 1 will find the most recent file matching a filter. |

If parameters are incorrect, the returned JSON will have the error details.


### DataLakeGetItems
This function is intended to help correct casing of ADF paths. Azure Data Lake paths are case sensitive. When dealing with metadata driven processing, it is easy for a mistake is the caseing in metadata to cause errors finding the correct file. This function helps by taking a path to a data lake item, and fiding matching paths with different casing. If only one path is found, the fuction returns the original path, and the corrected path. If multiple files are found, the original path is returned, along with an error message stating multiple mathing files exist.
