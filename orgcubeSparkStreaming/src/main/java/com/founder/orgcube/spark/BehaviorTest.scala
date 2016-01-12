package com.founder.orgcube.spark

import java.util.Properties

import com.founder.orgcube.redis.RedisClient
import com.founder.orgcube.util.HbaseUtil
import kafka.serializer.StringDecoder
import net.sf.json.{JSONArray, JSONObject}
import org.apache.log4j.Logger
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by wls on 2016/1/7.
 */
object BehaviorTest {
  val logger = Logger.getLogger(BehaviorTest.getClass)
  case class BehaviorType5(behaviorType:Int,btType:Int,timestamp: Long,pageNum:Int)
  case class BaseInfo(clientVersion:String,userRole:Int,appID:String,userID:String,osVersion:String,schoolID:String,hardwareMode:String,udid:String)

  val tableName = "orgcube_behavior"
  val columnFamily = "info"
  def behaviorBaseInfo(infoArr : ArrayBuffer[String]) {
    for(i <- 0 until infoArr.length){
      val info = infoArr(i).split("@")
      //解析出基本信息
      val binfo = JSONObject.fromObject(info(0))
      val dpubarrfo = JSONObject.fromObject(info(1))
      val dpubArr = JSONArray.fromObject(dpubarrfo.getString("dpubArr"))
      parseDiffBehavior(binfo,dpubArr)

    }
  }

  /**
   * 解析dpubArr，dpubArr中包括>=0个资源的相关信息，把每个资源中不同用户行为的信息分开，
   * 存储到Map<resourceId,List<用户行为json对象>>
   * 主要解析出用户行为behaviorType为1：dpubreading；2：pagereading；5：activitybutton
   * @param dpubArr 多个资源对应的dpub信息
   *
   */
  def parseDiffBehavior(binfo:JSONObject,dpubArr:JSONArray): Unit ={
    for(i <- 0 until dpubArr.size()){
      val jsonObject =JSONObject.fromObject(dpubArr.get(i).toString)
      val keys = jsonObject.keys()
      while(keys.hasNext){
        val resourceId = keys.next()
        val jsonArray = JSONArray.fromObject(jsonObject.getString(resourceId.toString))
        for(j <- 0 until jsonArray.size()){
          val dpubRowkey = binfo.getString("schoolID") + "#" + binfo.getString("userID")+"#"+resourceId.toString+"#"+System.currentTimeMillis()
          val jobject = JSONObject.fromObject(jsonArray.get(j).toString)
          val ojkeys = jobject.keys()
          var dpubMap: Map[String, String] = Map()
          dpubMap += ("resourceId" -> resourceId.toString)
          dpubMap += ("clientVersion" -> binfo.getString("clientVersion"))
          dpubMap += ("userRole" -> binfo.getString("userRole"))
          dpubMap += ("appID" -> binfo.getString("appID"))
          dpubMap += ("userID" -> binfo.getString("userID"))
          dpubMap += ("osVersion" -> binfo.getString("osVersion"))
          dpubMap += ("schoolID" -> binfo.getString("schoolID"))
          dpubMap += ("hardwareMode" -> binfo.getString("hardwareMode"))
          dpubMap += ("udid" -> binfo.getString("udid"))
          while(ojkeys.hasNext){
            val ojkey = ojkeys.next()
            if("updateTimestamp".equals(ojkey)){
              dpubMap += ("updateTimestamp" -> jobject.get(ojkey).toString)
            }
            if("behaviorType".equals(ojkey)){
              dpubMap += ("behaviorType" -> jobject.get(ojkey).toString)
            }
            if("duration".equals(ojkey)){
              dpubMap += ("duration" -> jobject.get(ojkey).toString)
            }
            if("timestamp".equals(ojkey)){
              dpubMap += ("timestamp" -> jobject.get(ojkey).toString)
            }
            if("durationType".equals(ojkey)){
              dpubMap += ("durationType" -> jobject.get(ojkey).toString)
            }
            if("pageNum".equals(ojkey)){
              dpubMap += ("pageNum" -> jobject.get(ojkey).toString)
            }
            if("eBehavior".equals(ojkey)){
              dpubMap += ("eBehavior" -> jobject.get(ojkey).toString)
            }
            if("btType".equals(ojkey)){
              dpubMap += ("btType" -> jobject.get(ojkey).toString)
            }
          }
          HbaseUtil.save(tableName, dpubRowkey, columnFamily, dpubMap)
        }
      }
    }
  }

  def processBehavior(bt:String,addInfoToMap:mutable.HashMap[String,Int],behaviorType5:BehaviorType5): Unit ={
    val key = bt+"#"+"pageNum:"+behaviorType5.pageNum//note#PageNum:3
    if(addInfoToMap.contains(key)){
      val count = addInfoToMap.get(key).get+1
      addInfoToMap +=(key -> count)
    }else
      addInfoToMap +=(key -> 1)
  }
  def main(args : Array[String]): Unit ={
    var masterUrl = "local[3]"
    if(args.length>0){
      masterUrl = args(0)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("BehaviorAnalysis")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    val topics = Set("FounderOrgcubeBigDataTopic1")
    val groups = "FounderOrgcubeBigDataGroup1"
    val props = new Properties()
    val in = BehaviorAnalysis.getClass.getClassLoader.getResourceAsStream("kafka.properties")
    props.load(in)
    val brokers = props.getProperty("metadata.broker.list")
    val a_zookeeper = props.getProperty("zookeeper.connect")
    val serializer = props.getProperty("serializer.class")

    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> brokers,
      "zookeeper.connect" -> a_zookeeper,
      "serializer.class" -> serializer,
      "group.id" -> groups
    )

    val kafkaStream = KafkaUtils.createDirectStream
      [String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    val events = kafkaStream.flatMap(line =>{
      //val data = JSONObject.fromObject(line._2)
      val behaviorRecordArr:Array[String] = line._2.split("#begin")
      val brArr = ArrayBuffer[String]()
      brArr ++=behaviorRecordArr
      brArr.remove(0,1)
      Some(brArr)
    })
    events.map(x =>x ).foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(infoArr => {

          //将源数据解析后，放到hbase数据库
          behaviorBaseInfo(infoArr)

          //分析，保存到redis
          //println(infoArr)
          for(info <- infoArr){
           /* val baseInfo = info.split("@").apply(0)
            val behaviorInfo = info.split("@").apply(1)
            val activityInfo = info.split("@").apply(2)
            println("baseInfo-----"+baseInfo)
            println("behaviorInfo---"+behaviorInfo)
            println("activityInfo---"+activityInfo)*/
            var baseInfo:BaseInfo = null
            info.split("@").foreach{
              x => val obj = JSONObject.fromObject(x)
                val keys = obj.keys()
                if(keys.hasNext){
                  keys.next() match {
                    case "clientVersion" =>
                      println("baseInfo-----"+obj.toString)
                      baseInfo = new BaseInfo(obj.getString("clientVersion"),obj.getInt("userRole"),obj.getString("appID"),
                                                  obj.getString("userID"),obj.getString("osVersion"),obj.getString("schoolID"),
                                                  obj.getString("hardwareMode"),obj.getString("udid"))

                    case "dpubArr" =>
                      println("behaviorInfo-----"+obj.getString("dpubArr"))
                      val dpubArr = JSONArray.fromObject(obj.get("dpubArr")).toArray()
                      for(dpub <- dpubArr){
                        val dpubObj = JSONObject.fromObject(dpub)
                        println("dpubObj----"+dpubObj)
                        val dpubKeys = dpubObj.keys()
                        if(dpubKeys.hasNext){
                          val resourceId = dpubKeys.next()
                          val behaviorArr = JSONArray.fromObject(dpubObj.get(resourceId)).toArray()
                          val behaviorType5s = new ListBuffer[BehaviorType5]
                          val addInfoToMap:mutable.HashMap[String,Int] = mutable.HashMap()
                          for(behavior <- behaviorArr){
                            val behaviorObj = JSONObject.fromObject(behavior)
                            behaviorObj.getInt("behaviorType") match {
                              case 1 =>
                                println("behaviorType===1"+behaviorObj.toString())
                              case 2 =>
                                println("behaviorType===2"+behaviorObj.toString())
                              case 3 =>
                                println("behaviorType===3"+behaviorObj.toString())
                              case 4 =>
                                println("behaviorType===4"+behaviorObj.toString())
                              case 5 =>
                                println("behaviorType===5"+behaviorObj.toString())
                                behaviorType5s +=BehaviorType5(5,behaviorObj.getInt("btType"),behaviorObj.getLong("timestamp"),behaviorObj.getInt("pageNum"))
                                println("behaviorType5s"+behaviorType5s.size)
                              case _ =>
                                println("behaviorType===_"+behaviorObj.toString())
                            }
                          }

                          for(behaviorType5 <- behaviorType5s){
                              behaviorType5.btType match{
                                case //截屏
                                  2 => processBehavior("screencapture",addInfoToMap,behaviorType5)
                                case //拍照
                                  3 => processBehavior("photograph",addInfoToMap,behaviorType5)
                                case //批注
                                  6 => processBehavior("postil",addInfoToMap,behaviorType5)
                                case //截图
                                  9 => processBehavior("screenshot",addInfoToMap,behaviorType5)
                                case //便签
                                  14 => processBehavior("note",addInfoToMap,behaviorType5)
                                case //静音录屏
                                  13 => processBehavior("screenCAP",addInfoToMap,behaviorType5)
                                case //录屏
                                  15 => processBehavior("screenCAP",addInfoToMap,behaviorType5)
                                case _ =>
                                  None
                              }
                          }
                          val dbIndex = 2
                          val time = System.currentTimeMillis()
                          addInfoToMap.keys.foreach{key =>
                            val field = key.split("#").apply(0)
                            val pageNum = key.split("#").apply(1)//pageNum:3
                            var jedis:Jedis = null
                            /**
                             * 下面的程序首先通过pool.getResource()获得一个Jedis实例，
                             * 然后利用这个Jedis实例向Redis服务器发送相关的指令操作，最后调用Jedis类的close方法，将这个Jedis实例归还给JedisPool。
                             */
                            try{
                              jedis =RedisClient.pool.getResource
                              jedis.select(dbIndex)
                              println(baseInfo.schoolID)
                              val jedisKey = "area:beijing:schoolId:"+baseInfo.schoolID+":classId:"+"1ban"+":userId:"+baseInfo.userID+":"+pageNum+":time:"+time
                              jedis.hset(jedisKey,field,addInfoToMap(key).toString)
                            }catch{
                              case e:Exception => logger.error("return back jedis failed, will fore close the jedis.", e)
                            }finally {
                             jedis.close()
                            }
                          }
                        }
                      }
                    case "activityUserArr" =>
                      println("activityInfo-----"+obj.toString)
                    case "exceptionArr" =>
                      println("exceptionInfo-----"+obj.toString)
                    case _ =>
                      None
                  }
                }
            }
          }
        })
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
