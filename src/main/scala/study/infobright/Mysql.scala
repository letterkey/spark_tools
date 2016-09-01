package study.infobright

import java.io.InputStream
import java.sql.{PreparedStatement, ResultSet, DriverManager, Connection}

/**
 * Created by root on 15-1-27.
 */
object Mysql {
  def excuteLoadData(data:InputStream) {
    val c = DriverManager.getConnection("jdbc:mysql://db-server:3306/ymy_test?user=zhaohaijun&password=676892")
    var p:PreparedStatement = null
    var result = 0
    try {
      print("------------------------------------------------------------------------------")
      var sql = "load data infile '/opt/tmp/test.txt' into table scala fields terminated by '\t'  lines terminated by '\n'"
      p = c.prepareStatement(sql)
//      if(p.isWrapperFor(Class[com.mysql.jdbc.Statement])){
         val ps:com.mysql.jdbc.PreparedStatement = p.unwrap(classOf[com.mysql.jdbc.PreparedStatement])
        ps.setLocalInfileInputStream(data)
        result = ps.executeUpdate()
//      }
    } finally {
      try {
        if (p != null)
          p.close();
        if (c != null)
          c.close();
      }
    }
    result > 0;
    }
}
