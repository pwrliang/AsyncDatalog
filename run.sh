#!/usr/bin/env bash
SOCIALITE_PREFIX=/home/gengl/socialite-before-yarn
HADOOP_HOME=/home/gengl/hadoop


EXT="${SOCIALITE_PREFIX}/ext"
HADOOP_COMMON="${HADOOP_HOME}/share/hadoop/common"
HADOOP_HDFS="${HADOOP_HOME}/share/hadoop/hdfs"
HADOOP_YARN="${HADOOP_HOME}/share/hadoop/yarn"

JAR_PATH=${EXT}/ST-4.0.7.jar
JAR_PATH=${JAR_PATH}:${EXT}/guava-18.0.jar;
JAR_PATH=${JAR_PATH}:${EXT}/trove-3.0.3.jar;
JAR_PATH=${JAR_PATH}:${EXT}/log4j-1.2.16.jar;
JAR_PATH=${JAR_PATH}:${EXT}/antlrworks-1.5.jar;
JAR_PATH=${JAR_PATH}:${EXT}/annotations-5.1.jar;
JAR_PATH=${JAR_PATH}:${EXT}/antlrworks-1.4.3.jar;
JAR_PATH=${JAR_PATH}:${EXT}/commons-lang-2.6.jar;
JAR_PATH=${JAR_PATH}:${EXT}/commons-lang3-3.1.jar;
JAR_PATH=${JAR_PATH}:${EXT}/RoaringBitmap-0.5.18.jar
JAR_PATH=${JAR_PATH}:${EXT}/commons-logging-1.1.1.jar
JAR_PATH=${JAR_PATH}:${EXT}/commons-collections-3.2.1.jar
JAR_PATH=${JAR_PATH}:${EXT}/commons-configuration-1.6.jar
JAR_PATH=${JAR_PATH}:${EXT}/commons-logging-api-1.0.4.jar
JAR_PATH=${JAR_PATH}:${EXT}/concurrent-prim-map-1.0.0.jar
JAR_PATH=${JAR_PATH}:${EXT}/antlr-3.5.2-complete-no-st3.jar
JAR_PATH=${JAR_PATH}:${EXT}/concurrent-prim-map-1.0.0.jar
JAR_PATH=${JAR_PATH}:${SOCIALITE_PREFIX}/jython/jython.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/minlog-1.3.0.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/libthrift-0.9.3.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/objenesis-2.5.1.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/slf4j-api-1.7.13.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/kryo-shaded-4.0.1.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/protobuf-java-2.5.0.jar
JAR_PATH=${JAR_PATH}:${EXT}/serialize/slf4j-log4j12-1.7.13.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/hadoop-common-2.7.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/zookeeper-3.4.6.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-configuration-1.6.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jsr305-3.0.0.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/asm-3.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/protobuf-java-2.5.0.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-net-3.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jaxb-api-2.2.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/htrace-core-3.1.0-incubating.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jsch-0.1.42.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jetty-util-6.1.26.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/stax-api-1.0-2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jsp-api-2.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-codec-1.4.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jackson-xc-1.9.13.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-io-2.4.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jetty-6.1.26.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-httpclient-3.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jaxb-impl-2.2.3-1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/apacheds-kerberos-codec-2.0.0-M15.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jackson-core-asl-1.9.13.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/curator-client-2.7.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/avro-1.7.4.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/api-util-1.0.0-M20.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/hadoop-annotations-2.7.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/httpclient-4.2.5.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/servlet-api-2.5.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/gson-2.2.4.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/netty-3.6.2.Final.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-cli-1.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-beanutils-core-1.8.0.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-compress-1.4.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-collections-3.2.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jersey-server-1.9.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jets3t-0.9.0.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/xmlenc-0.52.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/paranamer-2.3.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/curator-framework-2.7.1.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/slf4j-api-1.7.10.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/hamcrest-core-1.3.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jersey-json-1.9.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/snappy-java-1.0.4.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jersey-core-1.9.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-beanutils-1.7.0.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-logging-1.1.3.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/httpcore-4.2.5.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-math3-3.1.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-digester-1.8.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jackson-jaxrs-1.9.13.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/mockito-all-1.8.5.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/api-asn1-api-1.0.0-M20.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/junit-4.11.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jackson-mapper-asl-1.9.13.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/apacheds-i18n-2.0.0-M15.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/slf4j-log4j12-1.7.10.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/curator-recipes-2.7.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/log4j-1.2.17.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/xz-1.0.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/commons-lang-2.6.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/java-xmlbuilder-0.4.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/jettison-1.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/activation-1.1.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/guava-11.0.2.jar
JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/lib/hadoop-auth-2.7.2.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_YARN}/hadoop-yarn-common-2.7.2.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_YARN}/hadoop-yarn-registry-2.7.2.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_YARN}/hadoop-yarn-client-2.7.2.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_YARN}/hadoop-yarn-api-2.7.2.jar
#JAR_PATH=${JAR_PATH}:${HADOOP_COMMON}/zookeeper-3.4.6.jar
JAR_PATH=${JAR_PATH}:${HADOOP_HDFS}/hadoop-hdfs-2.7.2.jar

TEST_CLASSPATH=${SOCIALITE_PREFIX}/out/production/socialite


MASTER_HOST=master

./kill-all.sh machines

tar -zcf /tmp/out.tar.gz -C ${SOCIALITE_PREFIX} out conf examples
while IFS='' read -r line || [[ -n "$line" ]]; do
    if [ ${line} == ${MASTER_HOST} ]; then
        continue
    fi
    scp /tmp/out.tar.gz gengl@${line}:"/tmp/out.tar.gz"
    ssh -n -f gengl@${line} "rm -rf ${SOCIALITE_PREFIX}/out 2> /dev/null && tar -zxf /tmp/out.tar.gz -C ${SOCIALITE_PREFIX}/ && rm /tmp/out.tar.gz"
done < "machines"

#-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 \
mpjrun.sh -Xmx28G \
-machinesfile ${SOCIALITE_PREFIX}/machines -np 5 -dev niodev \
-Dsocialite.output.dir=${SOCIALITE_PREFIX}/gen \
-Dsocialite.worker.num=32 \
-Dsocialite.port=50100 \
-Dsocialite.master=master \
-Dlog4j.configuration=file:${SOCIALITE_PREFIX}/conf/log4j.properties \
-cp ${TEST_CLASSPATH}:${JAR_PATH} \
socialite.async.Entry ${SOCIALITE_PREFIX}/examples/prog6_dist.dl

#./kill-all.sh machines
