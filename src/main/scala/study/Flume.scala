package study

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{StreamingContext, Milliseconds}

/**
 * Created by Administrator on 2014/12/12.
 */
object Flume {
  def main (args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()
  val Array(host, port) = args

    val batchInterval = Milliseconds(5000)
//    val host = "localhost"
//    val port = 33333

    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
//      sparkConf.setMaster("spark://localhost:7077")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

    val bodys = stream.map(e => e.event.getBody)

    ssc.start()
    ssc.awaitTermination()
//    ssc.stop(true,true)
  }
}
