echo "========================================================================================================================================================================================================"
echo "Azure CLI Version: $(az version |  jq '."azure-cli"')"
echo
echo "Connection info:"
az account show | jq '. | {tenantId: .tenantId, subscriptionName: .name, userName: .user.name, userType: .user.type}'
echo

echo "Configuring variables for secrets:"
echo "========================================================================================================================================================================================================"
echo

# ls -a


# # Can we run a shell script to tidy this up?
# . ./.github/workflows/deploy_dev_test_lab.sh -?

# echo "========================================================================================================================================================================================================"


ARTIFACT_SOURCE_NAME=$(az lab artifact-source list --resource-group $RESOURCE_GROUP \
                                                    --lab-name $LAB_NAME \
                            | jq --raw-output \
                                '.[] | select( .uri == "https://github.com/sqlcollaborative/AzureDataPipelineTools.git" ) | .name' \
                    )

echo "Artifact Source Name: $ARTIFACT_SOURCE_NAME"

BRANCH_NAME=${GITHUB_REF#*refs/heads/}
echo "Branch Name: $BRANCH_NAME"

# We need the object id of the Enterprise Application created from the App Registration in order to set permissions in the ARM template. This is **not** the same as the app/client id
echo "Retriving service principal id for the logged in user..."
SERVICEPRINCIPALAPPID=$(az account show | jq --raw-output '.user.name')
echo "Service Principal App/Client Id: $SERVICEPRINCIPALAPPID"
SERVICEPRINCIPALID=$( az ad sp list --filter "appId eq '$SERVICEPRINCIPALAPPID' and servicePrincipalType eq 'Application'" --query [0].objectId --output tsv)
echo "Service Principal Object Id:     $SERVICEPRINCIPALID"


# Build a JSON snippet with the client/app id, object id and client secret for the devops SPN. This is used by the ARM template to grant permissions on resources so that the devops SPN
# can deploy code into them. The ARM template generates the required .runsettings file for the integration tests as an output, which reuses the devops SPN to access resources to test.
SERVICEPRINCIPALINFO=$( echo $SERVICEPRINCIPALCREDENTIALS | jq '{ tenantId, clientId, clientSecret, $clientObjectId }' --arg 'clientObjectId' $SERVICEPRINCIPALID -c )

echo "Service Principal Info:          $SERVICEPRINCIPALINFO"

echo "Building parameters file for ARM deployment..."
PARAMETERS_FILE="$(pwd)/azuredeploy.parameters.json"
echo $'[ { "name":"branch", "value":"'$BRANCH_NAME'" },' \
    '  { "name":"commit", "value":"'$GITHUB_SHA'" },' \
    '  { "name":"location", "value":"UK South" },' \
    '  { "name":"devopsServicePrincipalCredentials", "value":' $SERVICEPRINCIPALINFO ' }' \
    ']' \
| jq '.' > "$PARAMETERS_FILE"
#cat $PARAMETERS_FILE

ENVIRONMENT_INSTANCE_NAME='CI_Build___'"${BRANCH_NAME////__}"'___'"${GITHUB_SHA:0:8}"''
echo "Environment Instance Name: $ENVIRONMENT_INSTANCE_NAME"

echo "::set-output name=ENVIRONMENT_INSTANCE_NAME::$ENVIRONMENT_INSTANCE_NAME"

ENVIRONMENT_CREATE_OUTPUT=$(az lab environment create --resource-group $RESOURCE_GROUP \
                                                    --lab-name $LAB_NAME \
                                                    --name $ENVIRONMENT_INSTANCE_NAME \
                                                    --artifact-source-name $ARTIFACT_SOURCE_NAME \
                                                    --arm-template $ARM_TEMPLATE_NAME \
                                                    --parameter "@$PARAMETERS_FILE" \
                                                    --verbose \
                                | jq '.'
                            )

echo "Output from 'az lab environment create'"
echo $ENVIRONMENT_CREATE_OUTPUT

PROVISIONING_STATE=$(echo $ENVIRONMENT_CREATE_OUTPUT |  jq --raw-output '.provisioningState')
echo "Provisioning State: $PROVISIONING_STATE"

ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME=$(echo $ENVIRONMENT_CREATE_OUTPUT |  jq --raw-output '.resourceGroupId' | xargs basename)
echo "Resource Group Id: $ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME"

echo "::set-output name=ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME::$ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME"

if [ $PROVISIONING_STATE != "Succeeded" ]; then
echo "::error Error provisioning lab environment"
exit 1
fi

echo "========================================================================================================================================================================================================"
DEPLOYMENTOUTPUT=$(az deployment group list --resource-group $ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME --query '[0].properties.outputs')

# DEBUG: Use this to get the full deployment output JSON. If the ARM template outputs a full reference to a resource, we can find the bits we need easily.
# echo "::set-output name=DEPLOYMENTOUTPUT::$DEPLOYMENTOUTPUT"

echo "Deployment Outputs"
#echo "::set-output name=STORAGE_ACCOUNTCONNECTION_STRING::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.storageAccountConnectionString.value')"
echo "::set-output name=STORAGE_ACCOUNT_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.storageAccountName.value')"
echo "::set-output name=STORAGE_CONTAINER_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.storageContainerName.value')"
echo "::set-output name=FUNCTIONS_APP_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.functionsAppName.value')"
echo "::set-output name=FUNCTIONS_APP_URI::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.functionsAppUri.value')"
echo "::set-output name=KEY_VAULT_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.keyVaultName.value')"
#echo "::set-output name=FUNCTIONS_APP_KEY::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.functionsAppKey.value')"
echo "::set-output name=RUN_SETTINGS::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.runSettings.value')"

echo "========================================================================================================================================================================================================"
