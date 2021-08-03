#!/bin/bash

RED='\033[0;31m'
NC='\033[0m' # No Color

helpFunction()
{
    printf "${NC}Description:"
    echo "  This script uses AZ CLI to connect to a Azure Dev Test Labs instance and deploy a lab using an ARM template"
    echo -e ""
    echo "Usage:"
    echo "  . deploy_dev_test_lab.sh --resource_group AzureDataPipelineTools_CI \\"
    echo "                           --lab AzureDataPipelineTools \\"
    echo "                           --arm_template sqlcollaborative_AzureDataPipelineTools \\"
    echo "                           --arm_template_params \$servicePrincipalInfoJson"
    echo -e ""
    echo "Parameters:"
    echo -e "  --resource_group"
    echo -e "    The Resource group name"
    echo -e ""
    echo -e "  --lab"
    echo -e "    The Azure Dev Test Labs name"
    echo -e ""
    echo -e "  --arm_template"
    echo -e "    The name of the ARM template. This must be in a git repository already registred with the lab as an artifact source"
    echo -e ""
    echo -e "  --arm_template_params"
    echo -e "    JSON params to pass to the ARM template"
    echo -e "    Example;"
    echo -e "    {"
    echo -e "      \"clientId\": \"<GUID>\","
    echo -e "      \"clientSecret\": \"<GUID>\","
    echo -e "      \"subscriptionId\": \"<GUID>\","
    echo -e "      \"tenantId\": \"<GUID>\","
    echo -e "    }"
}


#================================================================================================================================================================
# Parse input
#================================================================================================================================================================
while [ $# -gt 0 ]; do

    if [[ $1 == "--help" ]] || [[ $1 == "-?" ]] || [[ $1 == "--?" ]]; then
        helpFunction
        return
    elif [[ $1 == *"--"* ]]; then
        param="${1/--/}"
        declare $param="$2"
        # echo $1 $2 // Optional to see the parameter:value result
    fi

    shift
done

if [ -z "$resource_group" ]; then
    printf "${RED}Parameter --resource_group is required.\n"
fi

if [ -z "$lab" ]; then
    printf "${RED}Parameter --lab is required.\n"
fi

if [ -z "$arm_template" ]; then
    printf "${RED}Parameter --arm_template is required.\n"
fi

if [ -z "$arm_template_params" ]; then
    printf "${RED}Parameter --arm_template_params is required.\n"
fi

echo ""

if [ -z "$resource_group" ] || [ -z "$lab" ] || [ -z "$arm_template" ] || [ -z "$arm_template_params" ]; then
    helpFunction
    return
fi


#================================================================================================================================================================
# Do some stuff
#================================================================================================================================================================


# If all is good, do the work
echo "Helo world from deploy_dev_test_lab.sh"

