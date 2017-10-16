#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh

cd $MAITER_HOME
tar -zcf /tmp/input.tar.gz input
while IFS='' read -r line || [[ -n "$line" ]]; do
    # if master as worker, start worker locally
    if [ ${line} != $HOSTNAME ]; then
        scp -r /tmp/input.tar.gz $USER@$line:/tmp/input.tar.gz
        ssh -n ${USER}@${line} "rm -rf ${MAITER_HOME}/input 2> /dev/null && tar -zxf /tmp/input.tar.gz -C $MAITER_HOME"
    fi
    echo ${line} >> $HADOOP_HOME/etc/hadoop/slaves
done < "${SOCIALITE_PREFIX}/conf/slaves"