#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh
#CODE_CLASSPATH=${SOCIALITE_PREFIX}/classes/socialite.jar
CODE_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite
PROG="java -Xmx28G"
PROG+=" -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen"
PROG+=" -Dsocialite.port=50100"
PROG+=" -Dsocialite.master=${MASTER_HOST}"
PROG+=" -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties"
PROG+=" -cp ${CODE_CLASSPATH}:${JAR_PATH}"
PROG+=" $1"

# NOTE!!! for dist log ----> logs/master.log

# PageRank
#single threadnum  node-count edge-path   iter-num
#$PROG socialite.test.PageRank single 4 4 hdfs://master:9000/examples/prog2_edge.txt 10

#dist   node-count   edge-path iter-num
#${BIN}/start-nodes.sh -copy-classes
#sleep 10
#$PROG socialite.test.PageRank dist 4 hdfs://master:9000/examples/prog2_edge.txt 10
#${BIN}/kill-all.sh "${SOCIALITE_PREFIX}/conf/machines"


# SSSP
# single      thread-num      node-num        edge-path
#$PROG socialite.test.SSSP single 4 685230 hdfs://master:9000/Datasets/SSSP/BerkStan/edge.txt

# dist         node-num         edge-path
#${BIN}/start-nodes.sh -copy-classes
#sleep 10
#$PROG socialite.test.SSSP dist 685230 hdfs://master:9000/Datasets/SSSP/BerkStan/edge.txt
#${BIN}/kill-all.sh "${SOCIALITE_PREFIX}/conf/machines"

# CC
# single threadnum      node-count   node-path   edge-path
$PROG socialite.test.CC single 4 685230 hdfs://master:9000/Datasets/CC/BerkStan/node.txt hdfs://master:9000/Datasets/CC/BerkStan/edge.txt

# dist node-count      node-path     edge-path
#${BIN}/start-nodes.sh -copy-classes
#sleep 10
#$PROG socialite.test.CC dist 685230 hdfs://master:9000/Datasets/CC/BerkStan/node.txt hdfs://master:9000/Datasets/CC/BerkStan/edge.txt
#${BIN}/kill-all.sh "${SOCIALITE_PREFIX}/conf/machines"


#for i in `seq 1 5`;
#do
#	bin/start-nodes.sh -copy-jar
#	sleep 2
#	$PROG
#	sleep 2
#	bin/kill-all.sh "${SOCIALITE_PREFIX}/conf/slaves"
#	sleep 2
#done