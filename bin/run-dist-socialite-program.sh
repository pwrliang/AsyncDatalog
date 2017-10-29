#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh
CODE_CLASSPATH=${SOCIALITE_PREFIX}/classes/socialite.jar
#CODE_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite
PROG="java -Xmx220G"
PROG+=" -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen"
PROG+=" -Dsocialite.port=50100"
PROG+=" -Dsocialite.master=${MASTER_HOST}"
PROG+=" -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties"
PROG+=" -cp ${CODE_CLASSPATH}:${JAR_PATH}"
PROG+=" $1"

function start-node(){
    ${BIN}/kill-all.sh ${MACHINES}

    MASTER_CMD="java -Xmx28G"
    MASTER_CMD+=" -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen"
    MASTER_CMD+=" -Dsocialite.port=50100"
    MASTER_CMD+=" -Dsocialite.master=${MASTER_HOST}"
    MASTER_CMD+=" -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties"
    MASTER_CMD+=" -cp $1:${JAR_PATH}"
    MASTER_CMD+=" socialite.dist.master.MasterNode"

    WORKER_CMD="java -Xmx6G"
    WORKER_CMD+=" -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen"
    WORKER_CMD+=" -Dsocialite.port=50100"
    WORKER_CMD+=" -Dsocialite.master=${MASTER_HOST}"
    WORKER_CMD+=" -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties"
    WORKER_CMD+=" -cp $1:${JAR_PATH} socialite.dist.worker.WorkerNode"
    # start master
    nohup ${MASTER_CMD} >> ${SOCIALITE_PREFIX}/logs/master.log 2>&1 &

    while IFS='' read -r line || [[ -n "$line" ]]; do
        # if master as worker, start worker locally
        if [ ${line} == ${MASTER_HOST} ]; then
            nohup ${WORKER_CMD} >> ${SOCIALITE_PREFIX}/logs/master.log 2>&1 &
        else
            ssh -n $USER@${line} "sh -c 'cd $SOCIALITE_PREFIX; nohup $WORKER_CMD > /dev/null 2>&1 &'"
        fi
    done < "${SOCIALITE_PREFIX}/conf/slaves"
}

# NOTE!!! for dist log ----> logs/master.log

# PageRank
#single threadnum  node-count edge-path   iter-num
#$PROG socialite.test.PageRank single 32 19991625 /clueweb/PageRank/clueweb_20M/edge_pair.txt 42

#dist   node-count   edge-path iter-num
#start-node ${CODE_CLASSPATH}
#sleep 10
#$PROG socialite.test.PageRank dist 875712 hdfs://master:9000/Datasets/PageRank/Google/edge.txt 42
#${BIN}/kill-all.sh "${SOCIALITE_PREFIX}/conf/machines"


# SSSP
# single      thread-num      node-num        edge-path
#$PROG socialite.test.SSSP single 32 19991625 /clueweb/SSSP/clueweb_20M/edge_pair.txt
#$PROG socialite.test.SSSP single 32 12150976 /vol/Datasets/SSSP/Wikipedia_link_en/edge_pair.txt

# dist         node-num         edge-path
start-node ${CODE_CLASSPATH}
sleep 10
$PROG socialite.test.SSSP dist 685230 hdfs://master:9000/Datasets/SSSP/BerkStan/edge.txt
${BIN}/kill-all.sh "${SOCIALITE_PREFIX}/conf/machines"

# CC
# single threadnum      node-count   node-path   edge-path
#$PROG socialite.test.CC single 64 19991625 /clueweb/CC/clueweb_20M/node.txt /clueweb/CC/clueweb_20M/edge_pair.txt

# dist node-count      node-path     edge-path
#${BIN}/start-nodes.sh -copy-classes
#sleep 10
#$PROG socialite.test.CC dist 685230 hdfs://master:9000/Datasets/CC/BerkStan/node.txt hdfs://master:9000/Datasets/CC/BerkStan/edge.txt
#${BIN}/kill-all.sh "${SOCIALITE_PREFIX}/conf/machines"

#$PROG socialite.test.COST single 32 30000000 /vol/Datasets/COST/30M/basic_30000000.txt /vol/Datasets/COST/30M/assb_30000000_soc.txt 41
#for i in `seq 1 5`;
#do
#	bin/start-nodes.sh -copy-jar
#	sleep 2
#	$PROG
#	sleep 2
#	bin/kill-all.sh "${SOCIALITE_PREFIX}/conf/slaves"
#	sleep 2
#done
