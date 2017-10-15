#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`
. ${BIN}/common.sh
BASE_NAME=`basename "$SOCIALITE_PREFIX"`
mkdir -p $SOCIALITE_PREFIX/logs > /dev/null
cd ${SOCIALITE_PREFIX}/../
tar -zcf /tmp/${BASE_NAME}.tar.gz ${BASE_NAME}

while IFS='' read -r line || [[ -n "$line" ]]; do
    # if master as worker, start worker locally
    if [ ${line} != $HOSTNAME ]; then
        scp /tmp/${BASE_NAME}.tar.gz $USER@${line}:/tmp/
        ssh -n ${USER}@${line} "rm -rf ${SOCIALITE_PREFIX} 2> /dev/null && tar -zxf /tmp/${BASE_NAME}.tar.gz -C /home/$USER/"
    fi
done < "${SOCIALITE_PREFIX}/conf/slaves"
