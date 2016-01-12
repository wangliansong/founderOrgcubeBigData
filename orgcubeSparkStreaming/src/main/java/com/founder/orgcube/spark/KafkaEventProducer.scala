package com.founder.orgcube.spark

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.log4j.Logger

import scala.util.Random

/**
 * Created by wls on 2015/11/17.
 * 模拟kafka消息类
 */
object KafkaEventProducer {

  val logger = Logger.getLogger(KafkaEventProducer.getClass)
  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf")
    /*"068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")*/

  private val url = Array(
    "http://item.yhd.com/item/37837059?tc=3.0.5.37837059.16&tp=51.%E7%9B%B4%E9%87%87.124.0.20.Kltwhpq-11-5F3Ab&tracker_u=97",
    "http://www.yhd.com/?tracker_u=98",
    "http://www.yhd.com/?tracker_u=59",
    "http://cms.yhd.com/sale/68346?ref=ad.15073_297_7552_20838_0_0_1_21_1_0_6_3&tc=ad.0.6.7552-20838.1&tracker_u=68"
    )

  private val session = Array(
    "TPRU5Q9TRX3M36QVFGYA1H4C1JH8RN6B", "TPRU5Q9TRX3M36QVFGYA1H4C1JH8RN6A",
    "TPRU5Q9TRX3M36QVFGYA1H4C1JH8RN6C", "TPRU5Q9TRX3M36QVFGYA1H4C1JH8RN6B"
    )

  private val username = Array(
    "wls", "zhao",
    "lili", "laopo"
  )
  private val ip = Array(
    "182.86.66.191", "222.90.239.229",
    "210.28.0.78", "61.234.209.16")
  private val random = new Random()

  private var pointer = 1
  private var sesssion_pointer = 1
  private var url_pointer = 1
  private var ip_pointer = 1

  def getUserID(): String = {
    pointer =pointer + random.nextInt(3)
    if(pointer >= users.length){
      pointer = 0
      users(pointer)
    }else{
      users(pointer)
    }
  }

  def getUserName(): String = {
    val username_pointer =random.nextInt(3)
    username(username_pointer)
  }

  def getSessionID(): String = {
    sesssion_pointer =sesssion_pointer + random.nextInt(3)
    if(sesssion_pointer >= session.length){
      sesssion_pointer = 0
      session(sesssion_pointer)
    }else{
      session(sesssion_pointer)
    }
  }

  def getUrlID(): String = {
    url_pointer =url_pointer + random.nextInt(3)
    if(url_pointer >= url.length){
      url_pointer = 0
      url(url_pointer)
    }else{
      url(url_pointer)
    }
  }

  def getIP(): String = {
    ip_pointer =ip_pointer + random.nextInt(3)
    if(ip_pointer >= ip.length){
      ip_pointer = 0
      ip(ip_pointer)
    }else{
      ip(ip_pointer)
    }
  }
  def click():Double = {
    random.nextInt(10)
  }

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning

  def main(args : Array[String]): Unit = {
    val topic = "FounderOrgcubeBigDataTopic1"
    //val brokers = "192.168.1.230:9092,192.168.1.231:9092,192.168.1.232:9092"
    val brokers = "172.19.10.92:9092,172.19.10.93:9092,172.19.10.94:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1");
    props.put("advertised.host.name", "hadoop-spark01");
    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String,String](kafkaConfig)
    while(true){
      var i =1
      val event="#begin{\"clientVersion\":\"2.1.1.20150525_fcd\",\"userRole\":0,\"appID\":\"com.founder.Class\",\"userID\":\"3FC1F854-501D-CF46-82F2-B2D97E266883\",\"osVersion\":\"8.1.1\",\"schoolID\":\"140004990\",\"hardwareMode\":\"iPad\",\"udid\":\"OpenUDIDcom.founder.Class_9ba89efcd5ccb5b5922dae79aaa53e8a5917f6e5\"}@{\"dpubArr\":[{\"E91DC89D-F191-872D-8EDE-15CDA527DD57\":[{\"updateTimestamp\":1448953999.710972,\"behaviorType\":1,\"duration\":0,\"timestamp\":1448953999.710972,\"durationType\":0},{\"pageNum\":2,\"updateTimestamp\":1448954006.874299,\"behaviorType\":2,\"duration\":0,\"timestamp\":1448954006.874299,\"durationType\":0},{\"eBehavior\":{\"eType\":{\"touchMode\":-1,\"playMode\":0},\"touchMode\":-1,\"playMode\":0},\"behaviorType\":3,\"pageNum\":3,\"timestamp\":1448954007},{\"behaviorType\":5,\"btType\":14,\"timestamp\":1448954010,\"pageNum\":3},{\"behaviorType\":5,\"btType\":14,\"timestamp\":1448954010,\"pageNum\":3},{\"behaviorType\":5,\"btType\":9,\"timestamp\":1448954012,\"pageNum\":3},{\"behaviorType\":5,\"btType\":9,\"timestamp\":1448954010,\"pageNum\":4}]}]}@{\"activityUserArr\":[{\"duration\":0,\"updateTimestamp\":1448953995.956099,\"durationType\":0,\"timestamp\":1448953995.956099}]}@{\"exceptionArr\":[]}"
      /*val event = new JSONObject()
      event.put("id",getUserID)
      event.put("username",getUserName)
      event.put("url",getUrlID())
      event.put("referer","referenceurl")
      event.put("session_id",getSessionID())
      event.put("ip",getIP())
      event.put("os_type","Android")
      event.put("create_time",System.currentTimeMillis().toString)
      event.put("location","北京")*/
      producer.send(new KeyedMessage[String,String](topic,event.toString))
      logger.info("@begin" +event)
      println("Message sent "+(i)+":" +event)
      i=i+1
      Thread.sleep(2000)    }

  }
}
