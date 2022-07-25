package com.imooc.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.slf4j.LoggerFactory

/**
 * 任务5：
 * 每周一计算最近一个月主播视频评级
 * 把最近几次视频评级在3B+或2A+的主播，在neo4j中设置flag=1
 *
 * 注意：在执行程序之前需要先把flag=1的重置为0
 * Created by xuwei
 */
object UpdateVideoInfoScala {
  val logger = LoggerFactory.getLogger("UpdateVideoInfoScala")

  def main(args: Array[String]): Unit = {
    var masterUrl = "local"
    var appName = "UpdateVideoInfoScala"
    var filePath = "hdfs://bigdata01:9000/data/video_info/20260201"
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

    //在Driver端执行此代码，将flag=1的重置为0
    //获取neo4j的连接
    val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
    //开启一个会话
    val session = driver.session()
    session.run("match(a:User) where a.flag =1 set a.flag = 0")
    //关闭会话
    session.close()
    //关闭连接
    driver.close()


    //获取SparkContext
    val conf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
    val sc = new SparkContext(conf)

    //读取视频评级数据
    val linesRDD = sc.textFile(filePath)

    //解析数据中的uid，rating，timestamp
    val tup3RDD = linesRDD.map(line => {
      try {
        val jsonObj = JSON.parseObject(line)
        val uid = jsonObj.getString("uid")
        val rating = jsonObj.getString("rating")
        val timestamp: Long = jsonObj.getLong("timestamp")
        (uid, rating, timestamp)
      } catch {
        case ex: Exception => logger.error("json数据解析失败：" + line)
          ("0", "0", 0L)
      }
    })

    //过滤异常数据
    val filterRDD = tup3RDD.filter(_._2 != "0")

    //获取用户最近3场直播(视频)的评级信息
    val top3RDD = filterRDD.groupBy(_._1).map(group => {
      //获取最近3次开播的数据，使用制表符拼接成一个字符串
      //uid,rating,timestamp \t uid,rating,timestamp \t uid,rating,timestamp
      val top3 = group._2.toList.sortBy(_._3).reverse.take(3).mkString("\t")
      (group._1, top3)
    })

    //过滤出来满足3场B+的数据
    val top3BRDD = top3RDD.filter(tup => {
      var flag = false
      val fields = tup._2.split("\t")
      if (fields.length == 3) {
        //3场B+，表示里面没有出现C和D
        val tmp_str = fields(0).split(",")(1) + "," + fields(1).split(",")(1) + "," + fields(2).split(",")(1)
        if (!tmp_str.contains("C") && !tmp_str.contains("D")) {
          flag = true
        }
      }
      flag
    })

    //把满足3场B+的数据更新到neo4j中，增加一个字段flag，flag=1表示是视频评级满足条件的主播，允许推荐给用户
    //注意：针对3场B+的数据还需要额外再限制一下主播等级，主播等级需要>=15，这样可以保证筛选出来的主播尽可能是一些优质主播
    top3BRDD.foreachPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
      //开启一个会话
      val session = driver.session()
      it.foreach(tup=>{
        session.run("match (a:User {uid:'"+tup._1+"'}) where a.level >=15 set a.flag = 1")
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
    })

    //过滤出来满足2场A+的数据
    val top2ARDD = top3RDD.filter(tup => {
      var flag = false
      val fields = tup._2.split("\t")
      if (fields.length >= 2) {
        //2场A+，获取最近两场直播评级，里面不能出现B、C、D
        val tmp_str = fields(0).split(",")(1) + "," + fields(1).split(",")(1)
        if (!tmp_str.contains("B") && !tmp_str.contains("C") && !tmp_str.contains("D")) {
          flag = true
        }
      }
      flag
    })

    //把满足2场A+的数据更新到neo4j中，设置flag=1
    //注意：针对2场A+的数据还需要额外限制一下主播等级，主播等级需要>=4 这样可以保证筛选出来的主播尽可能是一些优质主播
    top2ARDD.foreachPartition(it=>{
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
      //开启一个会话
      val session = driver.session()
      it.foreach(tup=>{
        session.run("match (a:User {uid:'"+tup._1+"'}) where a.level >=4 set a.flag = 1")
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
    })

  }

}
