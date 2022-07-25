#!/bin/bash
masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`


appName="RealTimeFollowScala"
seconds=5
kafkaBrokers="bigdata01:9092,bigdata02:9092,bigdata03:9092"
groupId="con_1"
topic="user_follow"
boltUrl="bolt://bigdata04:7687"
username="neo4j"
password="admin"



yarnCommonLib="hdfs://bigdata01:9000/yarnCommonLib"

spark-submit --master ${master} \
--name ${appName} \
--deploy-mode ${deployMode} \
--queue default \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 2 \
--class com.imooc.spark.RealTimeFollowScala \
--jars ${yarnCommonLib}/fastjson-1.2.68.jar,${yarnCommonLib}/spark-streaming-kafka-0-10_2.11-2.4.3.jar,${yarnCommonLib}/neo4j-java-driver-4.1.1.jar,${yarnCommonLib}/kafka-clients-2.4.1.jar,${yarnCommonLib}/reactive-streams-1.0.3.jar \
/data/soft/video_recommend/jobs/real_time_follow-1.0-SNAPSHOT.jar ${masterUrl} ${appName} ${seconds} ${kafkaBrokers} ${groupId} ${topic} ${boltUrl} ${username} ${password}
