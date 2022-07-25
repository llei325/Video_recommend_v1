#!/bin/bash
#默认获取上周一的时间
dt=`date -d "7 days ago" +"%Y%m%d"`
if [ "x$1" != "x" ]
then
    dt=`date -d "7 days ago $1" +"%Y%m%d"`
fi

masterUrl="yarn-cluster"
master=`echo ${masterUrl} | awk -F'-' '{print $1}'`
deployMode=`echo ${masterUrl} | awk -F'-' '{print $2}'`


appName="GetRecommendListScala"`date +%s`
boltUrl="bolt://bigdata04:7687"
username="neo4j"
password="admin"
#获取上周一的时间戳(单位：毫秒)
timestamp=`date --date="${dt}" +%s`000
#粉丝列表关注重合度
duplicateNum=2
#主播等级
level=4

#输出结果数据路径
outputPath="hdfs://bigdata01:9000/data/recommend_data/${dt}"


yarnCommonLib="hdfs://bigdata01:9000/yarnCommonLib"

spark-submit --master ${master} \
--name ${appName} \
--deploy-mode ${deployMode} \
--queue default \
--driver-memory 1g \
--executor-memory 1g \
--executor-cores 1 \
--num-executors 2 \
--class com.imooc.spark.GetRecommendListScala \
--jars ${yarnCommonLib}/neo4j-java-driver-4.1.1.jar,${yarnCommonLib}/reactive-streams-1.0.3.jar,${yarnCommonLib}/neo4j-spark-connector-2.4.5-M1.jar \
/data/soft/video_recommend/jobs/get_recommend_list-1.0-SNAPSHOT.jar ${masterUrl} ${appName} ${boltUrl} ${username} ${password} ${timestamp} ${duplicateNum} ${level} ${outputPath}

#验证任务执行状态
appStatus=`yarn application -appStates FINISHED -list | grep ${appName} | awk '{print $7}'`
if [ "${appStatus}" != "SUCCEEDED" ]
then
    echo "任务执行失败"
    # 发送短信或者邮件
else
    echo "任务执行成功"
fi