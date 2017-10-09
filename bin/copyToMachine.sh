#!/usr/bin/env bash
host_name=$1
# Absolute path to this script. /home/user/bin/foo.sh
SCRIPT=$(readlink -f $0)
# Absolute path this script is in. /home/user/bin
SCRIPT_PATH=`dirname ${SCRIPT}`
SOCIALITE_HOME=$(readlink -f "${SCRIPT_PATH}/..")
tar -zcf /tmp/socialite_partial.tar.gz -C ${SOCIALITE_HOME} ./out ./conf
scp /tmp/socialite_partial.tar.gz gengl@${host_name}:/tmp/
ssh gengl@${host_name} "tar -zxf /tmp/socialite_partial.tar.gz -C ${SOCIALITE_HOME}"
