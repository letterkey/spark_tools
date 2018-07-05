package study.streaming.struct

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * 自定义 jdbc sink
  * Created by YMY on 18/2/11.
  */
object StreamingToMySQL {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
        .master("local")
      .appName("structed_streaming_to_mysql")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()
    val data = lines.as[String].flatMap(_.split(" " )).groupBy($"value").count()
    data.printSchema()

    val mysql_sink = new JDBCSink("jdbc:mysql://10.93.19.32:8088/operation","care","care_123456")
    val insert = data.writeStream
        .foreach(mysql_sink)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .trigger(Trigger.Once())
      .start

    insert.awaitTermination()
  }
}
