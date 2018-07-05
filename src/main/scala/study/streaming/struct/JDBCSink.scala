package study.streaming.struct

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * 自定义 jdbc sink
  * 参考：https://toutiao.io/posts/8vqfdo/preview
  * Created by YMY on 18/4/10.
  */
class JDBCSink(url : String,user :String, pwd:String) extends ForeachWriter[Row]{

  val driverClass = "com.mysql.jdbc.Driver"
  var connection : Connection = _
  var statement :Statement = _

  override def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driverClass)
    connection = DriverManager.getConnection(url,user,pwd)
    statement = connection.createStatement()
    true
  }

  override def process(value: Row): Unit = {
    statement.executeUpdate("insert into NewTable(name,age) values(\'"+value(0)+"\',"+value(1)+")")
  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }

}
