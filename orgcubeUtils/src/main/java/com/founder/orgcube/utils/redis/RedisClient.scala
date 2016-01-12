package com.founder.orgcube.utils.redis

import java.io.Serializable
import java.util

import net.sf.json.JSONObject
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import redis.clients.jedis.JedisPool
import redis.clients.jedis.exceptions.{JedisDataException, JedisConnectionException, JedisException}

import scala.collection.mutable


/**
  * Created by wls on 2015/12/15.
  */
object RedisClient extends Serializable{

   val logger = Logger.getLogger(RedisClient.getClass)
   val redisHost = "172.19.10.92"
   val redisPort = 6379
   val redisTimeout = 30000
   lazy val pool = new JedisPool(new GenericObjectPoolConfig(),redisHost,redisPort,redisTimeout)

     lazy val hook = new Thread{
       override def run = {
         println("Execute hook thread: " + this)
         pool.destroy()
       }
     }
   sys.addShutdownHook(hook.run)

   /**
    * 异常处理
    * @param jedisException
    * @return boolean
    */
   def handleJedisException(jedisException:JedisException ):Boolean={
     if(jedisException.isInstanceOf[JedisConnectionException]){
       logger.error("Redis connection  lost .", jedisException)
     }else if(jedisException.isInstanceOf[JedisDataException]){
       if(jedisException.getMessage!=null && jedisException.getMessage.indexOf("READONLY") != -1){
         logger.error("Redis connection are read-only slave.", jedisException)
       }else{
         false
       }
     }else{
       logger.error("Jedis exception happen.", jedisException)
     }
     true
   }

   //通过http请求，得到云平台发送的老师、班级、学生等数据，并将结果解析后，保存到redis
  /* def saveBaseInfoToRedis(): Unit ={
     var paramsMap = new HashMap[String,String]
     paramsMap +=("method" -> "getUserInfo")
     paramsMap +=("userID" -> "1212")
     val result =HttpUtil.get("","GET",paramsMap)
     val reObject = JSONObject.fromObject(result)
     //{"schoolId:":}
   }*/

   def getBehaviorToRedisByKey(dbIndex:Int,key:String): java.util.Map[String,String] ={
     val reMap:java.util.Map[String,String] = new java.util.HashMap[String,String]
     val jedis =RedisClient.pool.getResource
     jedis.select(dbIndex)
     //val key = "schoolId:140004990classId:userId:3FC1F854-501D-CF46-82F2-B2D97E266883time1452068617074"
     //val keys = jedis.keys("schoolId:140004990classId:userId:3FC1F854-501D-CF46-82F2-B2D97E266883*")
     val keys = jedis.keys(key)
     println(keys)
     val values = jedis.hmget(key,"n","re")
     reMap.put("n",values.get(0))
     reMap.put("re",values.get(1))
     reMap
     //println(values)
     //values
   }
   def main(args:Array[String]): Unit ={
     val dbIndex = 1
     val key = "schoolId:140004990classId:userId:3FC1F854-501D-CF46-82F2-B2D97E266883*"
     getBehaviorToRedisByKey(dbIndex,key)
   }
 }
