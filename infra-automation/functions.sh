#!/bin/bash

#--------------------------------------------------------------------------------
# executeAndLog
#--------------------------------------------------------------------------------
function executeAndLog() {
    local cmd=$1
    local logtime="[$(date '+%Y-%m-%d %H:%M:%S')]"
    echo -en "${logtime}\t" >> ${LOGFILE}
    eval ${cmd} | tee -a ${LOGFILE}
}

function debugLog() {
    local cmd=$1
    local logtime="[$(date '+%Y-%m-%d %H:%M:%S')]"
    echo ${cmd}
}

#--------------------------------------------------------------------------------
# parse_yaml
#
# Copied from:
# http://stackoverflow.com/questions/5014632/how-can-i-parse-a-yaml-file-from-a-linux-shell-script
#--------------------------------------------------------------------------------
function parse_yaml {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}


function get_service_principal_info {
    local sp_name=$1

    local SP_JSON=$(az ad sp show --id http://$sp_name)

    SP_ID=$(echo $SP_JSON | jq -r '.objectId')
    executeAndLog "echo \"Service Principal ID: '${SP_ID}'\""

    SP_APP_ID=$(echo $SP_JSON | jq -r '.appId')
    executeAndLog "echo \"Service Application ID: '${SP_APP_ID}'\""

    AD_TENANT_ID=$(az account show | jq -r '.tenantId')

    executeAndLog "echo \"AD Tenant ID = '${AD_TENANT_ID}'\""
}

function get_subscription_id {
    SUBSCRIPTION_ID=$(az account show | jq -r '.id')
    executeAndLog "echo \"Azure Subscription ID: '${SUBSCRIPTION_ID}'\""
}

function get_adls_keyvault_sp_id {
    local sp_name=$1
    ADLS_KEYVAULT_SP_ID=$(az ad sp list --display-name $sp_name | jq -r '.[0].objectId')
    executeAndLog "echo \"ADLS Keyvault SP ID: '${ADLS_KEYVAULT_SP_ID}'\""
}

#--------------------------------------------------------------------------------
# usage
#--------------------------------------------------------------------------------
function only_config_file_usage {
    cat <<EOF
========================================================================================
This script requires

1. jinja2-cli to be present: pip install jinja2-cli
   https://github.com/mattrobenolt/jinja2-cli

2. azure-cli 2.0 to be present: curl -L https://aka.ms/InstallAzureCli | bash
   https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
========================================================================================
Usage: ${SCRIPTNAME} -c <config_file> [-l <logdir>] [-h]
where
    <config_file>  Name with full path of the configuration file
    <logdir>       Directory where logfile is to be stored.
                   Default value: <config_file_dir>/logs
========================================================================================
EOF
}

#--------------------------------------------------------------------------------
# checkParams
#--------------------------------------------------------------------------------
function  only_config_file_checkParams {
    errors=0

    if [ -z ${DL_CONFIG_FILE} ]
    then
        echo "ERROR: Configuration file is mandatory"
        ((errors+=1))
    else
        if [ ! -f ${DL_CONFIG_FILE} ]
        then
            echo "ERROR: Configuration file does not exist: '${DL_CONFIG_FILE}'"
            ((errors+=1))
        fi
    fi

    if [[ ! $(az 2>/dev/null) ]]
    then
        echo "ERROR: azure-cli is missing"
        ((errors+=1))
    fi

    if [[ ! $(jinja2 2>/dev/null) ]]
    then
        echo "ERROR: jinja2-cli is missing"
        ((errors+=1))
    fi

    if [[ ${errors} -gt 0 ]]
    then
        only_config_file_usage
        exit 1
    fi
}
