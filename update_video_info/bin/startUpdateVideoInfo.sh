#!/bin/bash

# 获取最近一个月的文件目录
#filepath=""
#for((i=1;i<=30;i++))
#do
#    filepath+="hdfs://bigdata01:9000/data/video_info/"`date -d "$i days ago" +"%Y%m%d"`,
#done


#默认获取昨天时间
dt=`date -d "1 days ago" +"%Y%m%d"`
if [ "x$1" != "x" ]
then
dt=$1
fi

#HDFS输入数据路径
filePath="hdfs://bigdata01:9000/data/video_info/${dt}"

masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`


appName="UpdateVideoInfoScala"`date +%s`
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
--class com.imooc.spark.UpdateVideoInfoScala \
--jars ${yarnCommonLib}/fastjson-1.2.68.jar,${yarnCommonLib}/neo4j-java-driver-4.1.1.jar,${yarnCommonLib}/reactive-streams-1.0.3.jar \
/data/soft/video_recommend/jobs/update_video_info-1.0-SNAPSHOT.jar ${masterUrl} ${appName} ${filePath} ${boltUrl} ${username} ${password}

#验证任务执行状态
appStatus=`yarn application -appStates FINISHED -list | grep ${appName} | awk '{print $7}'`
if [ "${appStatus}" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信或者邮件
else
    echo "任务执行成功"
fi