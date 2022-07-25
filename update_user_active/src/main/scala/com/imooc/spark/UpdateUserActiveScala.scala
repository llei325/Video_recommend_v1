package com.imooc.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.{AuthTokens, GraphDatabase}

/**
 * 任务4：
 * 每天定时更新用户活跃时间
 * Created by xuwei
 */
object UpdateUserActiveScala {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local"
    var appName = "UpdateUserActiveScala"
    var filePath = "hdfs://bigdata01:9000/data/user_active/20260201"
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

    //读取用户活跃数据
    val linesRDD = sc.textFile(filePath)

    //处理数据
    linesRDD.foreachPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
      //开启一个会话
      val session = driver.session()
      it.foreach(line=>{
        val jsonObj = JSON.parseObject(line)
        val uid = jsonObj.getString("uid")
        val timeStamp = jsonObj.getString("UnixtimeStamp")
        //添加用户活跃时间
        session.run("merge(u:User {uid:'"+uid+"'}) set u.timestamp = "+timeStamp)
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
    })
  }

}
