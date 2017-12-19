package study.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by didi on 17/8/22.
  */
object TestStreaming {

  def main(args: Array[String]): Unit = {
    val schemaExp = StructType(
        StructField("name", StringType, false) ::
        StructField("city", StringType, true)
        :: Nil
    )

    val spark = SparkSession.builder().master("local").appName("test_new_streaming").getOrCreate()
    val logs = spark.readStream.format("json").schema(schemaExp).load("file:///Users/didi/git/spark_tools/data/json/streaming")
    val wordCounts = logs.groupBy("name").count()
    val query = wordCounts.writeStream.outputMode("complete")
        .format("console")
      .trigger(ProcessingTime(5000))
      .start()
    import spark.implicits._
    val kafkaDataset = spark.read
    query.awaitTermination()
  }
}
