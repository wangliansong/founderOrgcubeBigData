package com.founder.orgcube.util

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * Created by wls on 2015年12月16日19:36:42
 * 操作hbase的工具类
 */
object HbaseUtil {

  val conf =HBaseConfiguration.create()
  val zk_list = "hadoop-spark01,hadoop-spark02,hadoop-spark03";
  conf.set("hbase.zookeeper.quorum", zk_list);
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  lazy val connection = ConnectionFactory.createConnection(conf)

  //创建一张表
  //注意scala 的String数组的声明，不是用string[]，而是使用Array[String]
  def createTable(tableName:String,columnFamilys:Array[String]): Unit ={
    val admin = connection.getAdmin
    try{
      if(admin.tableExists(TableName.valueOf(tableName.getBytes()))){
        println("表已存在，无须创建")
      }else{
        //对列族的处理
        val tableDesc = new HTableDescriptor(TableName.valueOf(tableName.getBytes()))
        for(i <- 0 until columnFamilys.length){
          val cfDesc = new HColumnDescriptor(columnFamilys(i).getBytes())
          cfDesc.setMaxVersions(3)
          tableDesc.addFamily(cfDesc)
        }
        admin.createTable(tableDesc)
        println("创建成功")
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally{
      try {
        if (admin != null) admin.close()
      }catch{
        case e:Exception => e.printStackTrace()
      }
    }
  }
  //一次保存一列数据
  def save(put:Put,tableName:String): Unit ={
    val table = connection.getTable(TableName.valueOf(tableName))
    try{
      table.put(put)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      try {
        if (table != null) table.close()
      }catch{
        case e:Exception => e.printStackTrace()
      }
    }
  }
  //一次保存一行数据（多列，但是rowkey相同）
  def save(tableName:String,rowKey:String,columnFamily:String,columns:Map[String,String]): Unit ={
    val table = connection.getTable(TableName.valueOf(tableName))
    try{
      val put = new Put(rowKey.getBytes())
      //循环遍历map
      columns.keys.foreach(key => {
        val mykey = key
        val myvalue = columns(key)
        put.addColumn(columnFamily.getBytes(),mykey.getBytes(),myvalue.getBytes())
      })
      table.put(put)
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      try{
        if(table!=null)table.close()
      }catch {
        case e:Exception => e.printStackTrace()
      }

    }
  }
}
