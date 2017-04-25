#!/bin/sh
PRE_PATH=/app/hadoop/application
DEPENDENCE_HOME=$PRE_PATH/bdp/bdp-wash/lib
lib='.'
for jar in `ls $DEPENDENCE_HOME/*.jar`
do
    lib=$lib","$jar
done
DATE=$(date +%Y%m%d)

/app/hadoop/spark/bin/spark-submit --class com.mucfc.bdp.wash.scala.main.BdpBackUpMain \
--master yarn-client --num-executors 50 --executor-cores 4 --driver-memory 3g --executor-memory 3g \
--queue group1 \
--conf "spark.driver.extraJavaOptions=-XX:PermSize=512m -XX:MaxPermSize=512m  -XX:+CMSClassUnloadingEnabled -XX:MaxTenuringThreshold=31 -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=10 -XX:+UseCompressedOops -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -verbose:gc -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$PRE_PATH/bdp/bdp-wash/logs/ -Xloggc:$PRE_PATH/bdp/bdp-wash/logs/gc1.log" \
--conf "spark.executor.extraJavaOptions=-XX:NewSize=512m -XX:MaxNewSize=512m -XX:PermSize=512m -XX:MaxPermSize=512m  -XX:+CMSClassUnloadingEnabled -XX:MaxTenuringThreshold=31 -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=10 -XX:+UseCompressedOops -XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintHeapAtGC -XX:+PrintGCApplicationConcurrentTime -verbose:gc -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$PRE_PATH/bdp/bdp-wash/logs/ -Xloggc:$PRE_PATH/bdp/bdp-wash/logs/gc.log" \
--conf spark.default.parallelism=1000 \
--conf spark.storage.memoryFraction=0.1 \
--conf spark.shuffle.memoryFraction=0.7 \
--jars=$lib,/app/hadoop/spark-1.6.1-bin-2.5.0-cdh5.2.0/lib/spark-assembly-1.6.1-hadoop2.5.0-cdh5.2.0.jar $PRE_PATH/bdp/bdp-wash/lib/bdp-wash-0.0.1-SNAPSHOT.jar $@  >$PRE_PATH/bdp/bdp-wash/logs/bdp-wash.${DATE}.log 2>&1
