package study.basic.kafkatoredis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, MappingException}
import redis.clients.jedis.JedisPool

class KafKaToRedis {
  //执行spark任务
  // ./spark-submit --class "study.basic.KafKaToRedis" --master "spark://master:7077" --jars $(echo /all_lib/*.jar | tr ' ' ',') --total-executor-cores 4  /root/wangzhen/spark_app/spark_app.jar master:2181/cloud/kafka 1 app_api 2
}

/**
 * 实时计算ip对应的地域并入库
 * 从kafka获得信息并将处理后的消息存储到redis
 */
object KafKaToRedis {
  def logger = Logger.getLogger(KafKaToRedis.getClass.getName)

  implicit val formats = DefaultFormats
  case class Mailserver(track_deviceid: String,lat: String, lon: String,ip: String)


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaApp")
    // 创建StreamingContext，5秒一个批次

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //获取客户端传递的参数映射到变量
    val Array(zkQuorum, group, topics, numThreads) = args

    val topicMap = Map(topics -> 1)
    //获取多个receiver union后的数据
    val kafkaDStreams = {
      //获取kafka输入的dstream，并按照分区启动numThreads个receivers
      val streams = (1 to numThreads.toInt).map { _ =>
        KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      }
      val unionDStream = ssc.union(streams)//进行union操作，即将多个receivers的记过union
      val sparkProcessingParallelism = 1
      unionDStream.repartition(sparkProcessingParallelism)
    }

    logger.info("开始执行")
    val result=kafkaDStreams.map(line =>{
      try {
        val json = parse(line)
        (json.extract[Mailserver].track_deviceid,json.extract[Mailserver].lat, json.extract[Mailserver].lon, json.extract[Mailserver].ip)
      }catch{
        case e:MappingException => (0,0,0,0)
      }
    })


    //将按照分区进行数据的输出，输出到redis实例
    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>{
        partitionOfRecords.foreach(pair =>{

          val deviceid=pair._1
          val lat=pair._2
          val lon=pair._3
          val ip=pair._4
          val cityName = {
            if (ip.toString.equals("0")){
              "0"
            }else{
              // 根据ip获得城市名称 TODO
              "城市名称"
            }
          }

          if (!lat.equals("") && !lon.equals("")){
            //logger.info("记录存储……，deviceid:"+deviceid)
            try{
              val pool = createRedisPool("host",6379,"pwd")
              val jedis= pool.getResource
              jedis.hset("app_open",deviceid.toString,lat.toString+"|"+lon.toString+"|"+cityName.toString)
              pool.returnResource(jedis)
            }
          }
        })
      })
    })
    ssc.start()            // 开始
    ssc.awaitTermination()  // 计算完毕退出
  }
  // 获得redis连接池
  def createRedisPool(host: String, port: Int, pwd: String): JedisPool = {
    val pc = new GenericObjectPoolConfig()
    pc.setMaxIdle(5)
    pc.setMaxTotal(5)
    new JedisPool(pc, host, port, 10000, pwd)
  }
 }
