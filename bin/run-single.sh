#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh

function run_single(){
    ${BIN}/kill-all.sh ${MACHINES}

    java -Xmx 28G \
    -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen \
    -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties \
    -cp ${CODE_CLASSPATH}:${JAR_PATH} \
    socialite.async.Entry $1
}


if [ "$#" == "1" ]; then
    CODE_CLASSPATH=${SOCIALITE_PREFIX}/classes/socialite.jar
    run_single $1
else
    echo "please specify [Datalog_Program]"
    exit 1
fi