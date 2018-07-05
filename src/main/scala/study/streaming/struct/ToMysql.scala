package study.streaming.struct

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * Created by YMY on 18/4/26.
  */
object ToMysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()
    import spark.implicits._

    val data = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load
      .as[String]
      .flatMap(_.split(" "))
//

    val params = Map[String,String](
      "user"->"root",
      "password" ->"pwd")
    val p = new Properties()
    val to_mysql = data.writeStream
//      .options(params)
        .option("user","report")
        .option("password","report_123456")
        .option("url","jdbc:mysql://10.93.19.32:8088/report")
        .option("table","test")
        .option("driver","")
      .format("mysql")
      .outputMode(OutputMode.Complete())
      .start()

    to_mysql.awaitTermination()
  }
}
