#!/bin/bash

# Include functions
. functions.sh

function create_keyvault {
    KEYVAULT_RES_GROUP=${res_group}-keys
    KEYVAULT_NAME=${res_group}-kv

    executeAndLog "echo \"Setting up resource group for keyvault...\""
    executeAndLog "az group create -n $KEYVAULT_RES_GROUP -l $location"

    executeAndLog "echo \"Setting up keyvault...\""
    executeAndLog "az keyvault create -n $KEYVAULT_NAME -g $KEYVAULT_RES_GROUP -l $location"
}

function cerate_adls_key {
    az keyvault key show --vault-name $KEYVAULT_NAME -n $ADLS_NAME
    if [ $? -eq 1 ]; then
        executeAndLog "echo \"Creating keys...\""
        executeAndLog "az keyvault key create -p software --vault-name $KEYVAULT_NAME -n $ADLS_NAME"
    fi

    local ADLS_KEY_VERSION_ROW=$(az keyvault key show --vault-name ${KEYVAULT_NAME} -n ${ADLS_NAME} | jq -r '.key.kid')
    IFS='/' read -ra fields <<<"$ADLS_KEY_VERSION_ROW"
    ADLS_KEY_VERSION=${fields[-1]}
    executeAndLog "echo \"ADLS key ${ADLS_NAME} version is: '${ADLS_KEY_VERSION}'\""
}

################
##### MAIN #####
################
EXECUTIONDATE=$(date +%Y-%m-%d_%H%M%S)
SCRIPTNAME=$(basename $(readlink -f $0))
HOME_DIR=$(dirname $(readlink -f $0))
LOGDIR=

DL_CONFIG_FILE=

HELP_WANTED=0

while getopts ":c:l:h" opt
do
    case $opt in
        c)  DL_CONFIG_FILE=${OPTARG}
            ;;
        l)  LOGDIR=${OPTARG}
            ;;
        h)  HELP_WANTED=1
            ;;
       \?)  echo "ERROR: Non supported parameters"
            only_config_file_usage
            exit 1
            ;;
    esac
done

if [[ ${HELP_WANTED} -gt 0 ]]
then
    only_config_file_usage
    exit 0
fi

only_config_file_checkParams
DL_CONFIG_FILE=$(readlink -f ${DL_CONFIG_FILE})

eval $(parse_yaml ${DL_CONFIG_FILE})

cd $HOME_DIR

CONFIG_DIR=$(dirname ${DL_CONFIG_FILE})
CONFIG_FILE=$(basename ${DL_CONFIG_FILE})

#-------------------------------------------
# If LOGDIR is not given use the default one
#-------------------------------------------
if [ -z ${LOGDIR} ]
then
    LOGDIR=${CONFIG_DIR}/logs
else
    LOGDIR=$(readlink -f ${LOGDIR})
fi

LOGFILE=${LOGDIR}/${SCRIPTNAME}_${EXECUTIONDATE}.log

echo "Logging goes to ${LOGFILE}"

#-----------------------------------
# If LOGDIR does not exist create it
#-----------------------------------
if [ ! -d ${LOGDIR} ]
then
    errtxt=$(mkdir -p ${LOGDIR} 2>&1)

    if [[ $? -gt 0 ]]
    then
        echo "ERROR: ${SCRIPTNAME}: ${errtxt}"
        exit 1
    fi
fi

executeAndLog "echo \"Script home directory: '${HOME_DIR}'\""

cd $CONFIG_DIR
executeAndLog "echo \"Configuration directory for domain '${dl_domain}' and env '${dl_env}': '${CONFIG_DIR}'\""

executeAndLog "echo \"Generating SSH keys...\""

mkdir -p ssh_keys

if [ ! -f ssh_keys/id_rsa ]; then
    ssh-keygen -t rsa -q -N '' -C $admin_name -f ssh_keys/id_rsa | tee -a ${LOGFILE}
else
    executeAndLog "echo \"SSH key already exists, skipping creation.\""
fi

ADMIN_SSH_KEY=$(cat ssh_keys/id_rsa.pub)

# Find out the subscription we're using
get_subscription_id

ADLS_NAME=$(echo ${res_group} | tr -d '-')dls
executeAndLog "echo \"ADLS name: '${ADLS_NAME}'\""

#----------------------------------------------------------
# Provision the keyvault resource group and the keyvault
#----------------------------------------------------------

create_keyvault
cerate_adls_key

executeAndLog "echo \"Generating ARM Templates...\""

# This requires jinja2-cli to be present: pip install jinja2-cli
# https://github.com/mattrobenolt/jinja2-cli

executeAndLog "jinja2 $HOME_DIR/main.parameters.j2template.json $CONFIG_DIR/$CONFIG_FILE --strict -D admin_ssh_key=\"${ADMIN_SSH_KEY}\" -D key_vault_name=\"${KEYVAULT_NAME}\" -D adls_key_version=\"${ADLS_KEY_VERSION}\" > $CONFIG_DIR/main.parameters.json"

#----------------------------------------------------------
# Provision the main stuff only if this is a main data lake
#----------------------------------------------------------

executeAndLog "echo \"Setting up resource group...\""
executeAndLog "az group create -n $res_group -l $location"

if [ $dl_env = 'test' -o $dl_env = 'prod' ]; then
    executeAndLog "echo \"Setting up a Network Security Group for the Resource Group...\""
    executeAndLog "az network nsg create -g $res_group -n ${res_group}-nsg -l $location"

    executeAndLog "echo \"Creating subnet...\""
    executeAndLog "az network vnet subnet create -g $network_res_group --vnet-name $vnet_name -n ${res_group}-sn --address-prefix $subnet_cidr --network-security-group /subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${res_group}/providers/Microsoft.Network/networkSecurityGroups/${res_group}-nsg"

    executeAndLog "echo \"Deploying Integration and Management resources for DEVELOPMENT...\""
    executeAndLog "az group deployment create -n ${res_group}_main -g $res_group --template-file \"$HOME_DIR/main.resources.json\" --parameters \"@$CONFIG_DIR/main.parameters.json\" --verbose"
else
    executeAndLog "echo \"Setting up resource group...\""
    executeAndLog "az group create -n $network_res_group -l $location"

    executeAndLog "echo \"Creating network...\""
    executeAndLog "az network vnet create -n $vnet_name --address-prefix $vnet_cidr -g $network_res_group -l $location"

    executeAndLog "echo \"Setting up a Network Security Group for the Resource Group...\""
    executeAndLog "az network nsg create -g $res_group -n ${res_group}-nsg -l $location"

    executeAndLog "echo \"Creating subnet...\""
    executeAndLog "az network vnet subnet create -g $network_res_group --vnet-name $vnet_name -n ${res_group}-sn --address-prefix $subnet_cidr --network-security-group /subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${res_group}/providers/Microsoft.Network/networkSecurityGroups/${res_group}-nsg"

    executeAndLog "echo \"Deploying Integration and Management resources...\""
    executeAndLog "az group deployment create -n ${res_group}_main -g $res_group --template-file \"$HOME_DIR/dev/dev_main.resources.json\" --parameters \"@${CONFIG_DIR}/main.parameters.json\" --verbose"
fi

executeAndLog "echo \"Granting Access for Data Lake Store to KeyVault...\""
get_adls_keyvault_sp_id RN_${ADLS_NAME}
executeAndLog "az keyvault set-policy -n ${KEYVAULT_NAME} --object-id ${ADLS_KEYVAULT_SP_ID} --key-permissions encrypt decrypt get"

executeAndLog "echo \"Enabling Keyvault for Data Lake Store...\""
executeAndLog "az dls account enable-key-vault -n $ADLS_NAME"

executeAndLog "echo \"Execution logged to ${LOGFILE}\""
executeAndLog "echo \"Data Lake set up done.\""
