#!/usr/bin/env bash


TEST_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite

./kill-all.sh ${MACHINES}

tar -zcf /tmp/out.tar.gz -C ${SOCIALITE_PREFIX} out conf examples
while IFS='' read -r line || [[ -n "$line" ]]; do
    if [ ${line} == ${MASTER_HOST} ]; then
        continue
    fi
    scp /tmp/out.tar.gz ${USER}@${line}:"/tmp/out.tar.gz"
    ssh -n -f ${USER}@${line} "rm -rf ${SOCIALITE_PREFIX}/out 2> /dev/null && tar -zxf /tmp/out.tar.gz -C ${SOCIALITE_PREFIX}/ && rm /tmp/out.tar.gz"
done < "${MACHINES}"

#-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
mpjrun.sh -Xmx28G \
-machinesfile ${MACHINES} -np ${MACHINES_NUM} -dev niodev \
-Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen \
-Dsocialite.worker.num=32 \
-Dsocialite.port=50100 \
-Dsocialite.master=${MASTER_HOST} \
-Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties \
-cp ${TEST_CLASSPATH}:${JAR_PATH} \
socialite.async.Entry ${SOCIALITE_PREFIX}/$1

#./kill-all.sh machines
