#!/usr/bin/env bash
BIN=`dirname "$0"`
BIN=`cd "$BIN"; pwd`

. ${BIN}/common.sh

PROG="java -Xmx28G"
PROG+=" -Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen"
PROG+=" -Dsocialite.port=50100"
PROG+=" -Dsocialite.master=${MASTER_HOST}"
PROG+=" -Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties"
PROG+=" -cp $1:${JAR_PATH}"
PROG+=" $1"
for i in `seq 1 5`;
do
	bin/start-nodes.sh -copy-classes
	sleep 2
	$PROG
	sleep 2
	./kill-all.sh -copy-classes
	sleep 2
done