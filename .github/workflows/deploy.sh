echo "========================================================================================================================================================================================================"
echo "Azure CLI Version: $(az version |  jq '."azure-cli"')"
echo
echo "Connection info:"
az account show | jq '. | {tenantId: .tenantId, subscriptionName: .name, userName: .user.name, userType: .user.type}'
echo
RUNTIMESTAMP=$(date +"%Y%m%d%H%M")

echo "========================================================================================================================================================================================================"

echo "Configuring variables for secrets:"

ARTIFACT_SOURCE_NAME=$(az lab artifact-source list --resource-group $RESOURCE_GROUP \
                                                    --lab-name $LAB_NAME \
                            | jq --raw-output \
                                '.[] | select( .uri == "https://github.com/sqlcollaborative/AzureDataPipelineTools.git" ) | .name' \
                    )

echo "Artifact Source Name: $ARTIFACT_SOURCE_NAME"

BRANCH_NAME=${GITHUB_REF#*refs/heads/}
echo "Branch Name: $BRANCH_NAME"

# Build some id's for the names of resources
if [ "$GITHUB_EVENT_NAME" == "workflow_dispatch" ]; then
    ACTOR_NAME='DEV_'"$(echo $GITHUB_ACTOR | sed "s/[^[:alpha:][:digit:]]//g")"
    ACTOR_SHORT=$(echo ${GITHUB_ACTOR} | sed "s/[^[:alpha:][:digit:]]//g" | cut -c -9)
    ENVIRONMENT=DEV
else
    ACTOR_NAME=GITHUB_CI_BUILD
    ACTOR_SHORT=CI
    ENVIRONMENT=CICD
fi


if [ "$GITHUB_EVENT_NAME" == "workflow_dispatch" ]; then
    REASON_TAG="Dev Environment for $GITHUB_ACTOR"
elif [ "$GITHUB_EVENT_NAME" == "pull_request" ]; then
    REASON_TAG="CI - ${GITHUB_SHA:0:7} pushed by $GITHUB_ACTOR for "
else
    REASON_TAG="CI - ${GITHUB_SHA:0:7} pushed by $GITHUB_ACTOR"
fi


echo "GitHub Actor: ${GITHUB_ACTOR}"
echo "Actor Short: ${ACTOR_SHORT}"
echo "Actor Name: ${ACTOR_NAME}"
echo "GitHub Run Number: ${GITHUB_RUN_NUMBER}"
echo "GitHub Run Id: ${GITHUB_RUN_ID}"
echo "GitHub Event Name: ${GITHUB_EVENT_NAME}"
echo "GitHub Workflow URL: ${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
echo "GitHub PR: ${GITHUB_PR_NUMBER}: $GITHUB_PR_TITLE"


# The name of the lab. Allows sorting by owner, timestamp, while showing the branch and commit it was built from
#ENVIRONMENT_INSTANCE_NAME='CI_Build___'"${BRANCH_NAME////__}"'___'"${GITHUB_SHA:0:8}"''
ENVIRONMENT_INSTANCE_NAME="${ACTOR_NAME}"'__'"${RUNTIMESTAMP}"'__'"${BRANCH_NAME////__}"'___'"${GITHUB_SHA:0:7}"''
echo "Environment Instance Name: $ENVIRONMENT_INSTANCE_NAME"

# Used for resource names, eg adlsnlangley1602c96a22d or adlsci09498d47f45. This should give enough uiniqueness to allow parallel environments, while showing the build reason and owner
RESOURCE_NAME_SUFFIX="_${ACTOR_SHORT}_${RUNTIMESTAMP:8:12}_${GITHUB_SHA:0:7}"
echo "Resource Name Suffix: $RESOURCE_NAME_SUFFIX"

# We need the object id of the Enterprise Application created from the App Registration in order to set permissions in the ARM template. This is **not** the same as the app/client id
echo "Retriving service principal id for the logged in user..."
SERVICEPRINCIPALAPPID=$(az account show | jq --raw-output '.user.name')
echo "Service Principal App/Client Id: $SERVICEPRINCIPALAPPID"
SERVICEPRINCIPALID=$( az ad sp list --filter "appId eq '$SERVICEPRINCIPALAPPID' and servicePrincipalType eq 'Application'" --query [0].objectId --output tsv)
echo "Service Principal Object Id:     $SERVICEPRINCIPALID"


# Build a JSON snippet with the client/app id, object id and client secret for the devops SPN. This is used by the ARM template to grant permissions on resources so that the devops SPN
# can deploy code into them. The ARM template generates the required .runsettings file for the integration tests as an output, which reuses the devops SPN to access resources to test.
SERVICE_PRINCIPAL_INFO=$( echo $SERVICE_PRINCIPAL_CREDENTIALS | jq '{ tenantId, clientId, clientSecret, $clientObjectId }' --arg 'clientObjectId' $SERVICEPRINCIPALID -c )
echo "Service Principal Info:          $SERVICE_PRINCIPAL_INFO"

echo "Building parameters file for ARM deployment..."
PARAMETERS_FILE="$(pwd)/azuredeploy.parameters.json"
echo $'[' \
    '  { "name":"branch", "value":"'$BRANCH_NAME'" },' \
    '  { "name":"environment", "value":"'$ENVIRONMENT'" },' \
    '  { "name":"environmentUser", "value":"'$ACTOR_NAME'" },' \
    '  { "name":"gitSha", "value":"'$GITHUB_SHA'" },' \
    '  { "name":"gitShaShort", "value":"'${GITHUB_SHA:0:7}'" },' \
    '  { "name":"githubPullRequest", "value":"PR: '${GITHUB_PR_NUMBER}': '$GITHUB_PR_TITLE'" },' \
    '  { "name":"resourceNameSuffix", "value":"'$RESOURCE_NAME_SUFFIX'" },' \
    '  { "name":"location", "value":"UK South" },' \
    '  { "name":"devopsServicePrincipalCredentials", "value":' $SERVICE_PRINCIPAL_INFO ' },' \
    '  { "name":"additionalPrincipals", "value":' "${ADDITIONAL_PRINCIPALS:=[]}" ' }' \
    ']' \
| jq '.' > "$PARAMETERS_FILE"
#cat $PARAMETERS_FILE


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

if [ "$PROVISIONING_STATE" != "Succeeded" ]; then
    echo "::error Error provisioning lab environment"
    exit 1
fi

ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME=$(echo $ENVIRONMENT_CREATE_OUTPUT |  jq --raw-output '.resourceGroupId' | xargs basename)
echo "Resource Group Id: $ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME"

echo "::set-output name=ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME::$ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME"


DEPLOYMENTOUTPUT=$(az deployment group list --resource-group $ENVIRONMENT_INSTANCE_RESOURCE_GROUP_NAME --query '[0].properties.outputs')

echo "Setting Job Outputs"
echo "========================================================================================================================================================================================================"


# These don't show in the output, but we can view then in a yaml step as below

# DEBUG: Use this to get the full deployment output JSON. If the ARM template outputs a full reference to a resource, we can find the bits we need easily.
#echo "::set-output name=DEPLOYMENTOUTPUT::$DEPLOYMENTOUTPUT"

echo "::set-output name=STORAGE_ACCOUNT_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.storageAccountName.value')"
echo "::set-output name=STORAGE_CONTAINER_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.storageContainerName.value')"
echo "::set-output name=FUNCTIONS_APP_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.functionsAppName.value')"
echo "::set-output name=FUNCTIONS_APP_URI::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.functionsAppUri.value')"
echo "::set-output name=KEY_VAULT_NAME::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.keyVaultName.value')"
echo "::set-output name=RUN_SETTINGS::$(echo $DEPLOYMENTOUTPUT | jq --raw-output '.runSettings.value')"

