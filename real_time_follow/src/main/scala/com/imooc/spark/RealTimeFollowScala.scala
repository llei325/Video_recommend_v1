package com.imooc.spark

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.neo4j.driver.{AuthTokens, GraphDatabase, Transaction, TransactionWork}

/**
 * 任务2：
 * 实时维护粉丝关注数据
 * Created by xuwei
 */
object RealTimeFollowScala {
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    var appName = "RealTimeFollowScala"
    var seconds = 5
    var kafkaBrokers = "bigdata01:9092,bigdata02:9092,bigdata03:9092"
    var groupId = "con_1"
    var topic = "user_follow"
    var boltUrl = "bolt://bigdata04:7687"
    var username = "neo4j"
    var password = "admin"
    if(args.length > 0){
      masterUrl = args(0)
      appName = args(1)
      seconds = args(2).toInt
      kafkaBrokers = args(3)
      groupId = args(4)
      topic = args(5)
      boltUrl = args(6)
      username = args(7)
      password = args(8)
    }

    //创建StreamingContext
    val conf = new SparkConf().setMaster(masterUrl).setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(seconds))

    //指定kafka的配置信息
    val kafkaParams = Map[String,Object](
      //kafka的broker地址信息
      "bootstrap.servers"->kafkaBrokers,
      //key的序列化类型
      "key.deserializer" ->classOf[StringDeserializer],
      //value的序列化类型
      "value.deserializer" ->classOf[StringDeserializer],
      //消费者组id
      "group.id" -> groupId,
      //消费策略
      "auto.offset.reset" -> "latest",
      //自动提交offset
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //指定要读取的topic的名称
    val topics = Array(topic)

    //获取消费kafka的数据流
    val kafkaDstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //处理数据
    //首先将kafkaDstream转换为rdd，然后就可以调用rdd中的foreachPartition了
    kafkaDstream.foreachRDD(rdd=>{
      //一次处理一个分区的数据
      rdd.foreachPartition(it=>{
        //获取neo4j的连接
        val driver = GraphDatabase.driver(boltUrl, AuthTokens.basic(username, password))
        //开启一个会话
        val session = driver.session()
        it.foreach(record=>{
          //获取粉丝关注相关数据
          val line = record.value()
          //解析数据
          val userFollowObj = JSON.parseObject(line)
          //获取数据类型，关注 or  取消关注
          val followType = userFollowObj.getString("desc")
          //获取followeruid
          val followeruid = userFollowObj.getString("followeruid")
          //获取followuid
          val followuid = userFollowObj.getString("followuid")

          if("follow".equals(followType)){
            //添加关注：因为涉及多条命令，所以需要使用事务
            session.writeTransaction(new TransactionWork[Unit] (){
              override def execute(tx: Transaction): Unit = {
                try{
                  tx.run("merge (:User {uid:'"+followeruid+"'})")
                  tx.run("merge (:User {uid:'"+followuid+"'})")
                  tx.run("match(a:User {uid:'"+followeruid+"'}),(b:User {uid:'"+followuid+"'}) merge (a) -[:follow]-> (b)")
                  //提交事务
                  tx.commit()
                }catch {
                  case ex: Exception => tx.rollback()
                }
              }
            })
          }else{
            //取消关注
            session.run("match (:User {uid:'"+followeruid+"'}) -[r:follow]-> (:User {uid:'"+followuid+"'}) delete r")
          }
        })
        //关闭会话
        session.close()
        //关闭连接
        driver.close()
      })
    })
    //启动任务
    ssc.start()
    //等待任务停止
    ssc.awaitTermination()
  }

}
