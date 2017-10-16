#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh

function run(){
    ${BIN}/kill-all.sh ${MACHINES}

    mpjrun.sh -Xmx28G \
    -machinesfile ${MACHINES} -np $((MACHINES_NUM+1)) -dev native \
    -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen \
    -Dsocialite.port=50100 \
    -Dsocialite.master=${MASTER_HOST} \
    -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties \
    -cp ${CODE_CLASSPATH}:${JAR_PATH} \
    socialite.async.Entry ${SOCIALITE_PREFIX}/$1
#    mpirun -machinefile ${MACHINES} -np $((MACHINES_NUM+1)) \
#    java -Djava.library.path=$MPJ_HOME/lib \
#    -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen \
#    -Dsocialite.port=50100 \
#    -Dsocialite.master=${MASTER_HOST} \
#    -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties \
#    -cp $MPJ_HOME/lib/mpj.jar:${CODE_CLASSPATH}:${JAR_PATH} \
#    socialite.async.Entry \
#    0 0 native ${SOCIALITE_PREFIX}/$1
}



if [ "$#" == "1" ]; then
    if [ "$1" == "-copy-jar" ]; then
        CODE_CLASSPATH=${SOCIALITE_PREFIX}/classes/socialite.jar
        while IFS='' read -r line || [[ -n "$line" ]]; do
            if [ ${line} == ${MASTER_HOST} ]; then
                continue
            fi
            ssh -n ${USER}@${line} "mkdir $SOCIALITE_PREFIX/classes 2> /dev/null"
            scp ${CODE_CLASSPATH} ${USER}@${line}:"$SOCIALITE_PREFIX/classes/"
        done < "${MACHINES}"
    elif [ "$1" == "-copy-classes" ]; then
        CODE_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite
        cd ${SOCIALITE_PREFIX}
        tar -zcf /tmp/out.tar.gz -C ${SOCIALITE_PREFIX} out conf examples
        while IFS='' read -r line || [[ -n "$line" ]]; do
            if [ ${line} == ${MASTER_HOST} ]; then
                continue
            fi
            scp /tmp/out.tar.gz ${USER}@${line}:"/tmp/out.tar.gz"
            ssh -n ${USER}@${line} "rm -rf ${SOCIALITE_PREFIX}/out 2> /dev/null && tar -zxf /tmp/out.tar.gz -C ${SOCIALITE_PREFIX}/ && rm /tmp/out.tar.gz"
        done < "${MACHINES}"
    else
        echo "please specify -copy-jar or -copy-classes"
        exit 1
    fi
elif [ "$#" == "2" ]; then
    if [ "$1" == "-run" ]; then
        CODE_CLASSPATH=${SOCIALITE_PREFIX}/classes/socialite.jar

    elif [ "$1" == "-debug" ]; then
        CODE_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite
    else
        echo "please specify [-run/-debug] [Datalog_Program]"
        exit 1
    fi
    run $2
else
    echo "please specify [-copy-jar/-copy-classes] or [-run/-debug] [Datalog_Program]"
    exit 1
fi