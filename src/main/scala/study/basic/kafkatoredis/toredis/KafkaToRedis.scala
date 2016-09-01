package study.basic.kafkatoredis.toredis

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.json4s.MappingException

import redis.clients.jedis.JedisPool
import org.json4s.jackson.JsonMethods._

/**
 * kafka的生产者模拟数据流
 * Created by root on 15-7-14.
 */
object KafkaToRedis {
  case class UserAction(userId: String,total: String)
  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    if (args.length > 0) {
      masterUrl = args(0)
    }

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("user_events")
    val brokers = "10.10.4.126:9092,10.10.4.127:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder")

    val dbIndex = 1
    val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {

      val data = parse(line._2)

      Some((data.extract[UserAction].userId,
        data.extract[UserAction].total))
    })

    // Compute user click times
    val userClicks = events.map(x => (x._1, x._2)).reduceByKey(_ + _)
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val uid = pair._1
          val clickCount = pair._2.toLong
          val pool = createRedisPool("host",6379,"pwd")
          val jedis = pool.getResource
          jedis.select(dbIndex)
          // jedis累加数值
          jedis.hincrBy(clickHashKey, uid, clickCount)
          pool.returnResource(jedis)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  // 获得redis连接池
  def createRedisPool(host: String, port: Int, pwd: String): JedisPool = {
    val pc = new GenericObjectPoolConfig()
    pc.setMaxIdle(5)
    pc.setMaxTotal(5)
    new JedisPool(pc, host, port, 10000, pwd)
  }
}
