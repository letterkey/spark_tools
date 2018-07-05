package study.streaming

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.KafkaUtils

/**
  * Created by YMY on 18/3/20.
  */
object KafkaCustomer {

  def main(args: Array[String]): Unit = {
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount_YMY")

    val mysqlUrl="jdbc:mysql://10.93.19.32:8088/report?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&autoReconnect=true"
    val mysqlUsername = "report"
    val mysqlPwd = "report_123456"

    val properties = new Properties
    properties.put("user",mysqlUsername)
    properties.put("password",mysqlPwd)
    properties.put("url",mysqlUrl)

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sql = new SQLContext(ssc.sparkContext)
    import sql.implicits._
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
//    val words = lines.map(_.toString).foreachRDD(rdd =>{
//      rdd.toDF("xxx").write.mode(SaveMode.Append).jdbc(properties.getProperty("url"),"TEST",properties)
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
