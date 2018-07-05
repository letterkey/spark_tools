package study.streaming

import org.apache.spark.sql.SparkSession

/**
  * note：structed streaming 支持kafka v0.10 +版本
  * Created by YMY on 18/3/20.
  */
object StructuredStreaming_kafka_to_mysql {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StructedString_YMY")
      .master("local")
      .getOrCreate()

    val inputstream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr("CAST(value AS STRING)")

    import spark.implicits._
    val test = inputstream.as[String]
        .writeStream
        .outputMode("append")
        .format("console")
        .start()
//    val query = inputstream.select($"key", $"value")
//      .as[(String, String)].map(kv => kv._1 + " " + kv._2).as[String]
//      .writeStream
//      .outputMode("append")
//      .format("console")
//      .start()

    test.awaitTermination()
  }

}
