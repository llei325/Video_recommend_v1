package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
 * 任务3：
 * 每天定时更新主播等级
 * Created by xuwei
 */
object UpdateUserLevelScala {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local"
    var appName = "UpdateUserLevelScala"
    var filePath = "hdfs://bigdata01:9000/data/cl_level_user/20260201"
    var boltUrl = "bolt://bigdata04:7687"
    var username = "neo4j"
    var password = "admin"
    if(args.length > 0){
      masterUrl = args(0)
      appName = args(1)
      filePath = args(2)
      boltUrl = args(3)
      username = args(4)
      password = args(5)
    }

    //获取SparkContext
    val conf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
    val sc = new SparkContext(conf)

    //读取用户等级数据
    val linesRDD = sc.textFile(filePath)

    //校验数据准确性
    val filterRDD = linesRDD.filter(line => {
      val fields = line.split("\t")
      //判断每一行的列数是否正确，以及这一行是不是表头
      if (fields.length == 8 && !fields(0).equals("id")) {
        true
      } else {
        false
      }
    })

    //处理数据
    filterRDD.foreachPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
      //开启一个会话
      val session = driver.session()
      it.foreach(line=>{
        val fields = line.split("\t")
        //添加等级
        session.run("merge(u:User {uid:'"+fields(1).trim+"'}) set u.level = "+fields(3).trim)
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
    })

  }

}
