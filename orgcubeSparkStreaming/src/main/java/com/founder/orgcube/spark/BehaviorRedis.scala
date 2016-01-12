package com.founder.orgcube.spark

import java.text.SimpleDateFormat
import java.util.Calendar

import com.founder.orgcube.hive.HiveConnection
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by wls on 2016/1/5.
 */
object BehaviorRedis {

  def main(args:Array[String]): Unit ={
    var masterUrl = "local[3]"
    if(args.length>0){
      masterUrl = args(0)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("BehaviorAnalysis")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(2))
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cl = Calendar.getInstance()
    //得到上个月的今天
    cl.set(Calendar.HOUR_OF_DAY, cl.get(Calendar.HOUR_OF_DAY) - 1)
    //val BeforeDate = cl.getTime
    //val BeforeDate = cl.getTimeInMillis
    //从hive中读取值
    val schoolId=""
    val classId=""
    val resourceId=""
    val userID=""
    val low=cl.getTimeInMillis
    val up=System.currentTimeMillis()
    println("low---"+low+"up-------"+up)
    val btLists = HiveConnection.findBtTypeCount(schoolId,classId,resourceId,userID,low,up)
    Some(btLists)

  }
}
