package study.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by didi on 17/8/22.
  */
object TestStreaming_NC {

  def main(args: Array[String]): Unit = {
    val schemaExp = StructType(
        StructField("name", StringType, false) ::
        StructField("city", StringType, true)
        :: Nil
    )


    val spark = SparkSession.builder().master("local").appName("test_new_streaming").getOrCreate()

    import spark.implicits._
    val lines = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    words.printSchema()
    val wordCounts = words.groupBy("value").count()
    println(wordCounts)

    val query = wordCounts.writeStream.outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
