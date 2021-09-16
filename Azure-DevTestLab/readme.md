# Building Azure DevTest Labs Environments using Azure CLI

Make sure the DevTest Lab resource is created and the git repo is [added as a template repository](https://docs.microsoft.com/en-us/azure/devtest-labs/devtest-lab-create-environment-from-arm).

Azure CLI is required to create environments as PowerShell AZ only has a small subset of the DevTest Labs commands.

1) Login to azure  
```
az login
```

2) List artifact sources  
```
az lab artifact-source list --resource-group '{DevTest Labs Resorce Group Name}' `
                            --lab-name '{DevTest Labs Name}'
```

3) In the returned JSON look for the item in the array where the display name matches the name of the template repository you set up, or the uri matches the git url of that repo. Find the name of the node, it should be something like *privaterepoXXX* where *XXX* is a random number. We use this to list the available ARM templates. If an ARM template or the parameter file is not valid, it will not show up in the list, but also does not generate errors when the template repository is configured.  
```
az lab arm-template list --resource-group '{DevTest Labs Resorce Group Name}' `
                         --lab-name '{DevTest Labs Name}' `
                         --artifact-source-name '{privaterepoXXX}'
```

4) The name one each item in the returned JSON should match the folders in the folder setup as the *Azure Resource Manager template folder path* when the template repository was configured. The ARM template and parameters are shown. We can check the ARM template is as we expect, and then create a parameters file to pass to create an environment. An example parameters file is shown below. Save it as a JSON file.  
***Note:** the paramters file for the AZ CLI tools is a different format to the paramters file stored in github as part of the template definition.*  
```
[
    {
        "name": "Parameter_1",
        "value": "ABC"
    },
    {
        "name": "Parameter_2",
        "value": "DEF"
    }
]
```

5) Now we can create the environment.  
***Note:** Make sure the path for the parameters file is prefixed with @.*  
```
az lab environment create --resource-group '{DevTest Labs Resorce Group Name}' `
                          --lab-name '{DevTest Labs Name}' `
                          --artifact-source-name '{privaterepoXXX}' `
                          --arm-template '{ARM Template name}' `
                          --name '{Environement Name}' `
                          --parameter '@{Full Path to Parameters file}'
```

6) Once the environment is created we can use the ```az resource list``` command to list the resources created in the environment. If the devtest lab is configured to create environments in their own resource group, we can find that in the JSON output from creating the environment in the ```resourceGroupId``` property. If we create all resources in the same resource group as the DevTest lab, then we can use tags in the ARM template to give us an easy way to find those resources.  
***Note:** Using tags to find resources will find all resources you have reader access to, regardless of what resource group they are in (you can't mix the ```--tag``` and ```--resource-group``` parameters ). Becuase of this use tags that are unique to your labs*  
An example of each is below:  
```
az resource list --resource-group {Environment Specific Resource Group Name}
```
```
az resource list --tag tag1=abc --tag tag2=def
```

7) Removing environments is simple. We delete the environment, and Azure removes all of the associated resources for us automatically.
```
az lab environment delete --resource-group '{DevTest Labs Resorce Group Name}' `
                          --lab-name '{DevTest Labs Name}' `
                          --name '{Environement Name}'
```
