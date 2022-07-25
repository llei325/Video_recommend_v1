package com.imooc.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.neo4j.spark.Neo4j

import scala.collection.mutable.ArrayBuffer

/**
 * 任务6：
 * 每周一计算最近一周内主活主播的三度关系列表
 * 注意：
 * 1：待推荐主播最近一周内活跃过
 * 2：待推荐主播等级>4
 * 3：待推荐主播最近1个月视频评级满足3B+或2A+(flag=1)
 * 4：待推荐主播的粉丝列表关注重合度>2
 * Created by xuwei
 */
object GetRecommendListScala {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local"
    var appName = "GetRecommendListScala"
    var boltUrl = "bolt://bigdata04:7687"
    var username = "neo4j"
    var password = "admin"
    var timestamp = 0L //过滤最近一周内是否活跃过
    var duplicateNum = 2 //粉丝列表关注重合度
    var level = 4 //主播等级
    var outputPath = "hdfs://bigdata01:9000/data/recommend_data/20260201"
    if(args.length > 0){
      masterUrl = args(0)
      appName = args(1)
      boltUrl = args(2)
      username = args(3)
      password = args(4)
      timestamp = args(5).toLong
      duplicateNum = args(6).toInt
      level = args(7).toInt
      outputPath = args(8)
    }


    //获取SparkContext
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)
      .set("spark.driver.allowMultipleContexts","true")//允许创建多个context
      .set("spark.neo4j.url",boltUrl)//bolt的地址
      .set("spark.neo4j.user",username)//neo4j用户名
      .set("spark.neo4j.password",password)//neo4j密码
    val sc = new SparkContext(conf)

    //获取一周内主活的主播 并且主播等级大于4的数据
    var params = Map[String,Long]()
    params += ("timestamp"->timestamp)
    params += ("level"->level)
    val neo4j: Neo4j = Neo4j(sc).cypher("match (a:User) where a.timestamp >= {timestamp} and a.level >= {level} return a.uid").params(params)

    //将从neo4j中查询出来的数据转换为rowRDD
    //val rowRDD = neo4j.loadRowRdd
    //repartition 这里的repartition是为了把数据分为7份，这样下面的mapPartitions在执行的时候就有7个线程
    //这7个线程并行查询neo4j数据库
    val rowRDD = neo4j.loadRowRdd.repartition(7)

    //一次处理一批
    //过滤出粉丝关注重合度>2的数据，并且对关注重合度倒序排列
    //最终的数据格式是：主播id,待推荐的主播id
    val mapRDD = rowRDD.mapPartitions(it => {
      //获取neo4j的连接
      val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
      //开启一个会话
      val session = driver.session()
      //保存计算出来的结果
      val resultArr = ArrayBuffer[String]()
      it.foreach(row => {
        val uid = row.getString(0)
        //计算一个用户的三度关系(主播的二度关系)
        //注意：数据量大了之后，这个计算操作是非常耗时
        //val result = session.run("match (a:User {uid:'" + uid + "'}) <-[:follow]- (b:User) -[:follow]-> (c:User) return a.uid as auid,c.uid as cuid,count(c.uid) as sum order by sum desc limit 30")
        //对b、c的主活时间进行过滤，以及对c的level和flag值进行过滤
        val result = session.run("match (a:User {uid:'" + uid + "'}) <-[:follow]- (b:User) -[:follow]-> (c:User) " +
          "where b.timestamp >= " + timestamp + " and c.timestamp >= " + timestamp + " and c.level >= " + level + " and c.flag = 1 " +
          "return a.uid as auid,c.uid as cuid,count(c.uid) as sum order by sum desc limit 30")
        while (result.hasNext) {
          val record = result.next()
          val sum = record.get("sum").asInt()
          if (sum > duplicateNum) {
            resultArr += record.get("auid").asString() + "\t" + record.get("cuid").asString()
          }
        }
      })
      //关闭会话
      session.close()
      //关闭连接
      driver.close()
      resultArr.iterator
    }).persist(StorageLevel.MEMORY_AND_DISK) //把RDD数据缓存起来

    //把数据转成tuple2的形式
    val tup2RDD = mapRDD.map(line => {
      val splits = line.split("\t")
      (splits(0), splits(1))
    })
    //根据主播id进行分组，可以获取到这个主播的待推荐列表
    val reduceRDD = tup2RDD.reduceByKey((v1, v2) => {
      v1 + "," + v2
    })

    //最终把结果组装成这种形式
    //1001 1002,1003,1004
    reduceRDD.map(tup=>{
      tup._1+"\t"+tup._2
    }).repartition(1).saveAsTextFile(outputPath)
  }

}
