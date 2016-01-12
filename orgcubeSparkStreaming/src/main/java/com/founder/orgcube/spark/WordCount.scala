package com.founder.orgcube.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by wls on 2016/1/11.
 */
object WordCount {


  def main(args:Array[String]): Unit ={
    /**
     * 第一步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
     * 例如说通过setMaster来设置程序要连接的Spark集群的Master的URL，如果设置
     * 为local，则代表Spark程序在本地运行。
     */
    val conf = new SparkConf()//创建SparkConf对象
    conf.setAppName("Word Count")//设置应用程序的名称，在程序运行的监控界面可以看到此名称
    conf.setMaster("local[4]")//此时，程序在本地运行，不需要安装Spark集群，4代表4核
    /**
     * 第二步：创建SparkContext对象
     */
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\bigdatalib\\spark-1.5.2-bin-hadoop2.6\\README.md",1)//读取本地文件，并设置为1个partition
    val words = lines.flatMap{line => line.split(" ")}
    //val words = lines.map{line => line.split(" ")}
    val pairs = words.map{word => (word,1)}
    val wordCounts = pairs.reduceByKey(_+_).foreach(x => print(x))
    sc.stop()
  }
}
