package com.founder.orgcube.hive

import java.sql.DriverManager

/**
 * Created by wls on 2015/12/16.
 */
object HiveConnection {

  def main(args: Array[String]): Unit ={
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection("jdbc:hive2://hadoop-spark01:10000/orgcubedb", "", "")
    try{
      val statement = conn.createStatement
      val rs = statement.executeQuery("select count(*) c from t_orgcube_behavior")
      while (rs.next){
        val c = rs.getString("c")
        //val key = rs.getString("key")
        //val id = rs.getString("id")
        //println("key = %s, id = %s".format(key, id))
        println("c------------"+c)
      }
    }catch{
      case e: Exception => e.printStackTrace
    }
    conn.close()
  }

}
