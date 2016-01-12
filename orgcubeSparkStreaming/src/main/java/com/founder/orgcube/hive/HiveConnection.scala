package com.founder.orgcube.hive

import java.sql.DriverManager

import com.founder.orgcube.entity.BehaviorResult.BtTypeCount

/**
 * Created by wls on 2015/12/16.
 */
object HiveConnection {
  val driverName = "org.apache.hive.jdbc.HiveDriver"
  val url = "jdbc:hive2://hadoop-spark01:10000/orgcubedb"
  val user = ""
  val password = ""
  Class.forName("org.apache.hive.jdbc.HiveDriver")
  val conn = DriverManager.getConnection(url, "", "")
 /* private static String sql = "";
  private static ResultSet res;
  private static Connection connection = null;*/
 //得到某个学生某本书的某个时间段的---便签14、批注 6、录屏 15 + 静音录屏 13、截屏 2、截图 9、拍照 3、书签 22
  def findBtTypeCount(schoolId:String,classId:String,resourceId:String,userID:String,low:Long,up:Long) : List[BtTypeCount] ={
   val btTypeCounts:List[BtTypeCount] =List()
   val sql ="""select info.userID,info.btType,count(info.btType) btTypeCount from t_orgcube_behavior
                where info.userID=? and info.schoolID=? and info.resourceId=?
                and (timestamp >=? and timestamp <=?)
                group by info.userID,info.btType""";
   val pstm = conn.prepareStatement(sql)
   try{
     pstm.setString(1, userID)
     pstm.setString(2, schoolId)
     pstm.setString(3, resourceId)
     pstm.setLong(4,low)
     pstm.setLong(5,up)
     val rss = pstm.executeQuery()
     while(rss.next()){
       val bt = new BtTypeCount(userID,rss.getString("btType"),rss.getInt("btTypeCount"))
       btTypeCounts.+:(bt)
     }
   }catch{
     case e: Exception => e.printStackTrace
   }finally {
     conn.close()
   }
   btTypeCounts
 }
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
