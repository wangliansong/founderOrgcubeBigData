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

import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
/**
 * Created by wls on 2015年12月28日09:36:16.
 * 从kafka中读取用户阅读行为，并通过sparkstreaming进行分析，然后保存到hbase数据库
 */
object BehaviorAnalysis {

  val logger = Logger.getLogger(BehaviorAnalysis.getClass)

 /* case class BaseInfo(clientVersion:String,userRole:String,appID:String,userID:String,
                      osVersion:String,schoolID:String,hardwareMode:String,udid:String)

  case class DpubArrInfo(resourceId:String,updateTimestamp:String,behaviorType:String, duration:String,
                         timestamp:String,durationType:String,pageNum:String,eBehavior:String,btType:String)*/
  val tableName = "orgcube_behavior"
  val columnFamily = "info"
  var buttonMap = new HashMap[String,ListBuffer[JSONObject]]
  var pageNoteMap = new HashMap[Int, Int]
  var pageNotesMap = new HashMap[Int, Int]
  var pageScreenRecordMap = new HashMap[Int, Int]
  var pageScreenMap = new HashMap[Int, Int]
  var pageCameraMap = new HashMap[Int, Int]
  var pageMarkBtnMap = new HashMap[Int, Int]
  var pagePrintScreenMap = new HashMap[Int, Int]
  var pageNumMap = new HashMap[Int, Int]
  //val columnFamilydpub = "dpubInfo"
  def behaviorBaseInfo(infoArr : ArrayBuffer[String]) {
    /*val bf = BaseInfo(binfo.getString("clientVersion"),binfo.getString("userRole"),
      binfo.getString("appID"),binfo.getString("userID"),binfo.getString("osVersion"),
      binfo.getString("schoolID"),binfo.getString("hardwareMode"),binfo.getString("udid"))*/

    for(i <- 0 until infoArr.length){
      val info = infoArr(i).split("@")
      //解析出基本信息
      val binfo = JSONObject.fromObject(info(0))
      val dpubarrfo = JSONObject.fromObject(info(1))
      val dpubArr = JSONArray.fromObject(dpubarrfo.getString("dpubArr"))
      //val rk = binfo.getString("schoolID") + "#" + binfo.getString("userID")
      //val rowkey = binfo.getString("schoolID") + "#" + binfo.getString("userID")+"#"+System.currentTimeMillis()
      parseDiffBehavior(binfo,dpubArr)
     /* var infoMap: Map[String, String] = Map()
      infoMap += ("clientVersion" -> binfo.getString("clientVersion"))
      infoMap += ("userRole" -> binfo.getString("userRole"))
      infoMap += ("appID" -> binfo.getString("appID"))
      infoMap += ("userID" -> binfo.getString("userID"))
      infoMap += ("osVersion" -> binfo.getString("osVersion"))
      infoMap += ("schoolID" -> binfo.getString("schoolID"))
      infoMap += ("hardwareMode" -> binfo.getString("hardwareMode"))
      infoMap += ("udid" -> binfo.getString("udid"))
      HbaseUtil.save(tableName, rowkey, columnFamilybase, infoMap)*/
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


  //保存到redis
  def behaviorRedis(infoArr : ArrayBuffer[String]) {
    /*val bf = BaseInfo(binfo.getString("clientVersion"),binfo.getString("userRole"),
      binfo.getString("appID"),binfo.getString("userID"),binfo.getString("osVersion"),
      binfo.getString("schoolID"),binfo.getString("hardwareMode"),binfo.getString("udid"))*/

    for(i <- 0 until infoArr.length){
      val info = infoArr(i).split("@")
      //解析出基本信息
      val binfo = JSONObject.fromObject(info(0))
      val dpubarrfo = JSONObject.fromObject(info(1))
      val dpubArr = JSONArray.fromObject(dpubarrfo.getString("dpubArr"))
      //val rk = binfo.getString("schoolID") + "#" + binfo.getString("userID")
      //val rowkey = binfo.getString("schoolID") + "#" + binfo.getString("userID")+"#"+System.currentTimeMillis()
      dataDeal(binfo,dpubArr)


      /* var infoMap: Map[String, String] = Map()
       infoMap += ("clientVersion" -> binfo.getString("clientVersion"))
       infoMap += ("userRole" -> binfo.getString("userRole"))
       infoMap += ("appID" -> binfo.getString("appID"))
       infoMap += ("userID" -> binfo.getString("userID"))
       infoMap += ("osVersion" -> binfo.getString("osVersion"))
       infoMap += ("schoolID" -> binfo.getString("schoolID"))
       infoMap += ("hardwareMode" -> binfo.getString("hardwareMode"))
       infoMap += ("udid" -> binfo.getString("udid"))
       HbaseUtil.save(tableName, rowkey, columnFamilybase, infoMap)*/
    }
  }

  def dataDeal(binfo:JSONObject,dpubArr:JSONArray): Unit ={
    parseDiffBehaviorRedis(dpubArr)
    val resultMap:HashMap[String, JSONObject] = behaviorCalculate(binfo,buttonMap)
    insertToRedis(resultMap)
   /* for(i <- 0 until dpubArr.size()){
      val jsonObject =JSONObject.fromObject(dpubArr.get(i).toString)
      val keys = jsonObject.keys()
      while(keys.hasNext){
        //var dpubMap: Map[String, List[JSONObject]] = Map()
        pageNoteMap.empty
        pageNotesMap.empty
        pageScreenRecordMap.empty
        pageScreenMap.empty
        pageCameraMap.empty
        pageMarkBtnMap.empty
        pagePrintScreenMap.empty
        pageNumMap.empty
        val resourceId = keys.next()
        val jsonArray = JSONArray.fromObject(jsonObject.getString(resourceId.toString))
        val buttonList:List[JSONObject]  = List()
        for(j <- 0 until jsonArray.size()){
          val dpubRowkey = binfo.getString("schoolID") + "#" + binfo.getString("userID")+"#"+resourceId.toString+"#"+System.currentTimeMillis()
          val jobject = JSONObject.fromObject(jsonArray.get(j).toString)
          val ojkeys = jobject.keys()

          val behaviorType = jobject.getInt("behaviorType")
          behaviorTypeMatch(behaviorType,buttonList,jobject)
          //HbaseUtil.save(tableName, dpubRowkey, columnFamily, dpubMap)
        }
        //dpubMap +=(resourceId.toString -> buttonList)
        calculateBehavior(buttonList)
        //生成结果json对象
        val resultListJson:List[JSONObject]= pageInfoJson(resourceId.toString)
      }
    }*/
  }

  /**
   * 解析dpubArr，dpubArr中包括>=0个资源的相关信息，把每个资源中不同用户行为的信息分开，
   * 存储到Map<resourceId,List<用户行为json对象>>
   * 主要解析出用户行为behaviorType为1：dpubreading；2：pagereading；5：activitybutton
   * @param dpubArr 多个资源对应的dpub信息
   *
   */
  def parseDiffBehaviorRedis(dpubArr:JSONArray): Unit ={
    buttonMap.clear()
    for(i <- 0 until dpubArr.size()){
      val jobject = JSONObject.fromObject(dpubArr.get(i).toString)
      val ojkeys = jobject.keys()
      while(ojkeys.hasNext){
        val resourceId = ojkeys.next().toString
        val behaviorArr:JSONArray = JSONArray.fromObject(jobject.getString(resourceId).toString)
        //val behaviorType = jobject.getInt("behaviorType").toString
        //behaviorTypeMatch(behaviorType,resourceId,buttonMap,jobject)
        val buttonList = new ListBuffer[JSONObject]
        for(j <- 0 until behaviorArr.size()){
          val behaviorJson:JSONObject = JSONObject.fromObject(behaviorArr.get(j))
          behaviorJson.getInt("behaviorType") match{
            case 5 =>
              println("behaviorType==="+behaviorJson.getInt("behaviorType"))
              buttonList +=(behaviorJson)

            case 1 =>
              println("behaviorType==="+behaviorJson.getInt("behaviorType"))
            case 2 =>
              println("behaviorType==="+behaviorJson.getInt("behaviorType"))
            case 3 =>
              println("behaviorType==="+behaviorJson.getInt("behaviorType"))
            case _ =>
              println("behaviorType==="+behaviorJson.getInt("behaviorType"))

          }
        }
        buttonMap.put(resourceId,buttonList)
      }
      //HbaseUtil.save(tableName, dpubRowkey, columnFamily, dpubMap)
    }
  }

  /**
   * 用户行为统计，生成最终的Map<rowkey,页面行为json>
   * 目前只统计了用户行为2和5的，1的未用到，暂时未统计
   * 最终统计结果存储到一个resultMap<resourceId#schoolId#classId#pageNum#userId,List<JSONObject>> 中
   * 通过pageReadingService，计算用户行为behaviorType为2（停留时长）的页面阅读次数及阅读时长，返回List<JSONObject>
   * 	JSONObject对象包括resourceId，pageNum，r，sd信息
   * 通过activityBtnService，计算用户行为behaviorType为5（各种功能按钮-->截图、拍照等）的页面交互信息，放好List<JSONObject>
   * 	JSONObject对象包括resourceId,pageNum,n,re,p,ph,c,ps,m信息
   * 生成最终的Map<rowkey,页面行为json>:
   * 	1,先根据pageReading获取的pageReadingList<JSONObject>计算出resultMap<rowkey,页面行为json>
   * 		页面行为json包括r,sd,n,re,p,ph,c,ps,m;此时json对象中只有r,sd有值，其他默认为0
   * 	2,根据activityBtn获取的activityBtnList<JSONObject>计算出resultMap<rowkey,页面行为json>
   * 		通过activityBtnList生成rowkey，在resultMap中查找是否存在，若存在则修改此rowkey对应的json对象值；
   * 		若不存在，创建一个json对象，r、sd值给个默认值，再将其他值添加到json对象中
   * @return
   */
  def behaviorCalculate(binfo:JSONObject,buttonMap:HashMap[String,ListBuffer[JSONObject]]):HashMap[String, JSONObject]={
    val resultMap = new HashMap[String, JSONObject]
    val buttonList = new ListBuffer[JSONObject]
    //TODO http请求
    val userId = binfo.getString("userID")
    val schoolId = binfo.getString("schoolID")
    val classId = ""
    //获取页面按钮信息
    val activityBtn:ListBuffer[JSONObject] =calculateBehavior(buttonMap)
    getActivityBtnInfo(userId, schoolId, classId, resultMap, activityBtn)
    resultMap
  }

  /**
   * 解析并计算页面交互行为：便签、录屏、批注、拍照、截屏、截图、书签；
   *  返回值为json对象的list数组
   * 每个json对象中包含资源ID键值对、页码键值对、便签键值对、录屏键值对、批注键值对、拍照键值对、截屏键值对、截图键值对、书签键值对；
   * 如{"resourceId":1,"pageNum":1,"n":0,"re":0,"p":1,"ph":0,"c":0,"ps":1,"m":0}
   * @param dpubMap 此条记录中用户行为为5的资源ID和json对象数值，如：{\"behaviorType\":5,\"btType\":6,\"timestamp\":1429608210,\"pageNum\":0}
   * @return List<JSONObject> 返回值为json对象的list数组
   */
    def calculateBehavior(dpubMap:HashMap[String,ListBuffer[JSONObject]]): ListBuffer[JSONObject] = {
    var resultListJson = new ListBuffer[JSONObject]
    pageNoteMap.clear()
    pageNotesMap.clear()
    pageScreenRecordMap.clear()
    pageScreenMap.clear()
    pageCameraMap.clear()
    pageMarkBtnMap.clear()
    pagePrintScreenMap.clear()
    pageNumMap.clear()

    dpubMap.keys.foreach {key =>
      val resourceId = key
      resultListJson = calculateSameDpubPageInfo(dpubMap(key), resourceId);
    }
    resultListJson
  }

  /**
   * 获取同一资源ID，各个页面的便签、录屏、批注、拍照、截屏、截图、书签
   * 返回值为json对象的list数组
   * 每个json对象中包含资源ID键值对、页码键值对、便签键值对、录屏键值对、批注键值对、拍照键值对、截屏键值对、截图键值对、书签键值对；
   * 如{"resourceId":1,"pageNum":1,"n":0,"re":0,"p":1,"ph":0,"c":0,"ps":1,"m":0}
   * @param pageInfoList 用户行为为5的json对象,原始json
   * @param resourceId 资源ID
   * @return
   *
   */

  def calculateSameDpubPageInfo( pageInfoList:ListBuffer[JSONObject],resourceId:String):ListBuffer[JSONObject]={
    //val resultListJson:List[JSONObject]=List();
    //解析json对象
    parsePageInfo(pageInfoList)
    //生成结果json对象
    val resultListJson:ListBuffer[JSONObject] = pageInfoJson(resourceId)
    for(r <- resultListJson){
      println("resultListJson-------------"+r.toString)
    }
    resultListJson
    //return resultListJson;
  }

  /**
   * 解析原始json对象：
   * 	1，获取每个用户行为json对象中的pageNum，由于客户端提供的pageNum从0开始，所以获取到的pageNum+1
   * 	2，重新赋值maxPageNum，minPageNum，用于pageInfoJson方法中的循环变量
   * 		将此次list中的最大pageNum赋值给maxPageNum，最小pageNum赋值给minPageNum
   * 	3，获取每个json对象中的btType，不同btType分别存储到对应的Map<pageNum,interactiveCount>中
   * 		如果同一页码在map中已存在，则map中对应页面的value值加1
   * @param pageInfoList 原始json对象
   *
   */
  def parsePageInfo(pageInfoList:ListBuffer[JSONObject]){
    for (jsonObject <- pageInfoList) {
      try {
        val pageNum = jsonObject.getInt("pageNum") + 1;
        pageNumMap.put(pageNum, pageNum);
        (jsonObject.getInt("btType")) match {
          case //截屏
            2 => addInfoToMap(pageScreenMap, pageNum)
          case //拍照
            3 => addInfoToMap(pageCameraMap, pageNum)
          case //批注
            6 => addInfoToMap(pageNotesMap, pageNum)
          case //截图
            9 => addInfoToMap(pagePrintScreenMap, pageNum)
          case //便签
            14 => addInfoToMap(pageNoteMap, pageNum)
          case //静音录屏
            13 => println("静音录屏")
          case //录屏
            15 => addInfoToMap(pageScreenRecordMap, pageNum)
          case //书签
            22 => addInfoToMap(pageMarkBtnMap, pageNum)
        }

      }
    }

  }
    /**
     * 计算各页码各种交互行为的次数
     * @param infoMap 存储各种交互行为的map
     * @param pageNum 此交互行为的pageNum
     */
    def  addInfoToMap(infoMap:HashMap[Int, Int], pageNum:Int ) {
      if(infoMap.isEmpty || !infoMap.contains(pageNum)){
        infoMap += (pageNum -> 1);
      } else if(infoMap.contains(pageNum)) {
        //infoMap.put(pageNum, infoMap.get(pageNum)+1);
        //infoMap += (pageNum -> infoMap(pageNum)+1)
        infoMap(pageNum)=infoMap(pageNum)+1
      }
    }
    /**
     * 统计生成最终页面各种交互信息json对象：
     * 1,通过maxPageNum和minPageNum循环，获取同一资源ID对应的不同pageNum的json对象
     * 2,将json对象添加到List数组中，返回
     *  每个json对象中包含资源ID键值对、页码键值对、便签键值对、录屏键值对、批注键值对、拍照键值对、截屏键值对、截图键值对、书签键值对；
     * 	如{"resourceId":1,"pageNum":1,"n":0,"re":0,"p":1,"ph":0,"c":0,"ps":1,"m":0}
     * @param resourceId
     * @return
     *
     */
    def  pageInfoJson(resourceId:String):ListBuffer[JSONObject]= {
      val resultListJson=new ListBuffer[JSONObject]
      pageNumMap.keys.foreach{pageNum =>
        val resultJson = new JSONObject()
        resultJson.put("resourceId", resourceId)
        resultJson.put("pageNum",pageNum)
        resultJson.put("c", pageScreenMap.getOrElse(pageNum,0))
        resultJson.put("ph",pageCameraMap.getOrElse(pageNum,0))
        resultJson.put("p", pageNotesMap.getOrElse(pageNum,0))
        resultJson.put("n", pageNoteMap.getOrElse(pageNum,0))
        resultJson.put("re",pageScreenRecordMap.getOrElse(pageNum,0))
        resultJson.put("ps",pagePrintScreenMap.getOrElse(pageNum,0))
        resultJson.put("m",pageMarkBtnMap.getOrElse(pageNum,0))
        resultListJson +=(resultJson)
      }

      resultListJson;
    }


  /**
   * 获取页面按钮信息,即用户行为为5
   * 获取结果最终存储到Map<resourceId#schoolId#classId#pageNum#userId,JSONObject>中；
   * 存储时首先判断map中是否含有相同key值，如果有，在此key的JSONObject对象增加key，value对；
   * 如果没有，new一个新的JSONObject对象，将阅读次数及阅读时长键值对增加到JSONObject中，然后再增加按钮相关信息
   * JSONObject对象最终包括阅读次数、阅读时长、便签、录屏、批注、拍照、截屏、截图、书签，如：{"r":1,"sd":32.09,"n":0,"re":0,"p":1,"ph":0,"c":0,"ps":1,"m":0}
   * @param userId
   * @param schoolId
   * @param classId
   * @param resultMap
   * @param activityBtn
   */
  def getActivityBtnInfo( userId:String,  schoolId:String,
                          classId:String , resultMap:HashMap[String,JSONObject] ,
                          activityBtn:ListBuffer[JSONObject] ) {
    for (btnJsonObject <- activityBtn) {
      try {
        val resourceId = btnJsonObject.getString("resourceId")
        val pageNum = btnJsonObject.getInt("pageNum");
        val key ="schoolId:"+schoolId+":classId:"+classId+":userId:"+userId+":time:"+System.currentTimeMillis()

        /*var jsonObject = resultMap.getOrElse(key);
        if(jsonObject == null){
          jsonObject = new JSONObject();
          jsonObject.put("r", 0);
          jsonObject.put("sd", 0.0);
        }*/
        val jsonObject = new JSONObject()
        jsonObject.put("pageNum", btnJsonObject.getInt("pageNum")); //那一页
        jsonObject.put("n", btnJsonObject.getInt("n")); //便签
        jsonObject.put("re", btnJsonObject.getInt("re")); //录屏+静音录屏
        jsonObject.put("p", btnJsonObject.getInt("p")); //批注
        jsonObject.put("ph", btnJsonObject.getInt("ph")); //拍照
        jsonObject.put("c", btnJsonObject.getInt("c")); //截屏
        jsonObject.put("ps", btnJsonObject.getInt("ps")); //截图
        jsonObject.put("m", btnJsonObject.getInt("m")); //书签

        resultMap.put(key, jsonObject);

      } catch  {
        case e:Exception =>e.printStackTrace();
      }
    }
  }

  def insertToRedis(resultMap:HashMap[String, JSONObject]): Unit ={

    //分析后的结果，保存到redis
  /*  object InternalRedisClient extends Serializable{
      @transient private var pool: JedisPool = null
      def makePool(redisHost:String,redisPort:Int,redisTimeout:Int,
                   maxTotal:Int,maxIdle:Int,minIdle:Int): Unit ={
        makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
      }
      def makePool(redisHost:String,redisPort:Int,
                   redisTimeout:Int,maxTotal:Int,maxIdle:Int,minIdle:Int,
                   testOnBorrow:Boolean,testOnReturn:Boolean,maxWaitMillis:Long): Unit ={
        if(pool==null){
          val poolConfig = new GenericObjectPoolConfig()
          poolConfig.setMaxTotal(maxTotal)
          poolConfig.setMaxIdle(maxIdle)
          poolConfig.setMinIdle(minIdle)
          poolConfig.setTestOnBorrow(testOnBorrow)
          poolConfig.setTestOnReturn(testOnReturn)
          poolConfig.setMaxWaitMillis(maxWaitMillis)
          //pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
          pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)
          val hook = new Thread{
            override def run = pool.destroy()
          }
          sys.addShutdownHook(hook.run)
        }
      }

      def getPool:JedisPool = {
        assert(pool !=null)
        pool
      }
    }*/
      //redis 配置
     /* val maxTotal = 10
      val maxIdle = 10
      val minIdle = 1
      val redisHost = "hadoop-spark01"
      val redisPort = 6379
      val redisTimeout = 30000*/
      val dbIndex = 1
      //InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
      resultMap.keys.foreach{key =>
        var remap:Map[Array[Byte],Array[Byte]] = Map()
        val jobje=resultMap(key)
        //remap += ("r".getBytes() -> jobje.getString("r").getBytes())
        remap += ("n".getBytes() -> jobje.getString("n").getBytes())
        remap += ("re".getBytes() -> jobje.getString("re").getBytes())
        remap += ("p".getBytes() -> jobje.getString("p").getBytes())
        remap += ("ph".getBytes() -> jobje.getString("ph").getBytes())
        remap += ("c".getBytes() -> jobje.getString("c").getBytes())
        remap += ("ps".getBytes() -> jobje.getString("ps").getBytes())
        remap += ("m".getBytes() -> jobje.getString("m").getBytes())
        val jedis =RedisClient.pool.getResource
        jedis.select(dbIndex)
        //jedis.hset(key,"r",jobje.getString("r"))
        jedis.hset(key,"n",jobje.getString("n"))
        jedis.hset(key,"re",jobje.getString("re"))
        jedis.hset(key,"p",jobje.getString("p"))
        jedis.hset(key,"ph",jobje.getString("ph"))
        jedis.hset(key,"c",jobje.getString("c"))
        jedis.hset(key,"ps",jobje.getString("ps"))
        jedis.hset(key,"m",jobje.getString("m"))
        RedisClient.pool.returnResource(jedis)
        //jedis.hmset(key.getBytes,remap)
        //InternalRedisClient.getPool.returnResource(jedis)
      }

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
      val brArr = ArrayBuffer[String]();
      brArr ++=behaviorRecordArr
      brArr.remove(0,1)
      //val data = JSONObject.fromObject(behaviorRecordArr)
     /* for(i <- 0 until behaviorRecordArr.length){
        val behaviorItem:Array[String] = behaviorRecordArr(i).split("@")
        val baseInfo = JSONObject.fromObject(behaviorItem(0))
        val dpubArr = JSONObject.fromObject(behaviorItem(1))
      }*/
      //val data = JSONObject.fromObject(line._2.split("#begin").apply(1).trim)
      //logger.info("data-----111----"+behaviorRecordArr.toString)
      Some(brArr)
    })


    val behavior = events.map(x =>x )foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(infoArr => {
          //将源数据解析后，放到hbase数据库
          behaviorBaseInfo(infoArr)

          //分析，保存到redis
          behaviorRedis(infoArr)

        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
