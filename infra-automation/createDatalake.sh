#!/bin/bash

# Include functions
. functions.sh

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

executeAndLog "echo \"Creating management certificates...\""

mkdir -p certificates/$workspace_name
cd certificates/$workspace_name

if [ ! -f cert.pem ]; then
    openssl req -x509 -days 3650 -newkey rsa:2048 -out cert.pem -nodes -subj '/CN=$res_group'
    openssl pkcs12 -export -out $adls_sp_name.pfx -inkey privkey.pem -in cert.pem -password pass:$adls_sp_pwd
    base64 $adls_sp_name.pfx > $adls_sp_name.pfx.base64
    tr -d '\n' < $adls_sp_name.pfx.base64 > $adls_sp_name.pfx.base64.noln
fi

SP_CERTIFICATE=$(cat $adls_sp_name.pfx.base64.noln)

CERT=$(head -n-1 cert.pem | tail -n+2)

executeAndLog "echo \"Creating Azure Application and Service Principals...\""
executeAndLog "az ad sp create-for-rbac -n $adls_sp_name --cert \"$CERT\""

executeAndLog "echo \"Waiting 10secs for service principal to get created...\""
sleep 10s

# Get the basic SP and AD info
get_service_principal_info $adls_sp_name
# Find out the subscription we're using
get_subscription_id

ADLS_NAME=$(echo ${main_res_group} | tr -d '-')dls
executeAndLog "echo \"ADLS name: '${ADLS_NAME}'\""

executeAndLog "echo \"Generating ARM Templates...\""

# This requires jinja2-cli to be present: pip install jinja2-cli
# https://github.com/mattrobenolt/jinja2-cli
executeAndLog "jinja2 $HOME_DIR/datalake.parameters.j2template.json $CONFIG_DIR/$CONFIG_FILE --strict -D admin_ssh_key=\"${ADMIN_SSH_KEY}\" -D ad_tenant_id=$AD_TENANT_ID -D sp_application_id=$SP_APP_ID -D sp_certificate_content=\"${SP_CERTIFICATE}\" -D adls_name=$ADLS_NAME > $CONFIG_DIR/$workspace_name.parameters.json"

# Provisioning...

executeAndLog "echo \"Setting up Resource Group...\""
executeAndLog "az group create -n $res_group -l $location"

executeAndLog "echo \"Creating subnet...\""
executeAndLog "az network vnet subnet create -g $network_res_group --vnet-name $vnet_name -n ${res_group}-sn --address-prefix $subnet_cidr"

executeAndLog "echo \"Creating Azure Data Lake filesystems...\""
executeAndLog "az dls fs create -n $ADLS_NAME --path /clusters/$workspace_name --folder | tee -a ${LOGFILE}"

executeAndLog "echo \"Setting Azure Data Lake file permissions...\""
executeAndLog "az dls fs access set-entry -n $ADLS_NAME --path / --acl-spec user:$SP_ID:--x"
executeAndLog "az dls fs access set-entry -n $ADLS_NAME --path /clusters/ --acl-spec user:$SP_ID:--x"
executeAndLog "az dls fs access set-entry -n $ADLS_NAME --path /clusters/$workspace_name/ --acl-spec default:user:$SP_ID:rwx,user:$SP_ID:rwx"
executeAndLog "az dls fs create -n $ADLS_NAME --path /clusters/$workspace_name/storage/ --folder"

if [ $workspace_name = 'maindatalake' ]; then
    executeAndLog "az dls fs create -n $ADLS_NAME --path /clusters/$workspace_name/staging/ --folder"
fi

executeAndLog "echo \"Waiting 5secs for filesystems to be created and permissions propagated\""
sleep 5s

executeAndLog "echo \"Creating database for metadata...\""
executeAndLog "az sql db create -n $workspace_name -g $main_res_group -s ${main_res_group}-db"

executeAndLog "echo \"Creating Azure HDInsight for Data Lake...\""
executeAndLog "az group deployment create -n ${res_group}_$workspace_name -g $res_group --template-file \"$HOME_DIR/datalake.resources.json\" --parameters \"@${CONFIG_DIR}/$workspace_name.parameters.json\"  --verbose"

executeAndLog "echo \"Enable Network Security Group for Data Lake...\""
executeAndLog "az network vnet subnet update -g $network_res_group --vnet-name $vnet_name -n ${res_group}-sn --network-security-group /subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${res_group}/providers/Microsoft.Network/networkSecurityGroups/${res_group}-nsg"

executeAndLog "echo \"Execution logged to ${LOGFILE}\""
executeAndLog "echo \"Data Lake set up done.\""
