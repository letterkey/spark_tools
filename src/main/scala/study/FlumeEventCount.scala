/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package study

import java.nio.ByteBuffer
import java.nio.charset.Charset
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer


/**
 *  step1:bin/flume-ng agent --conf conf -f conf/avro-spark.conf --name a1 -Dflume.root.logger=INFO,console

    step2:bin/flume-ng avro-client -H localhost -p 44444 -F README -Dflume.root.logger=DEBUG,console

    step3:bin/spark-submit --class study.FlumeEventCount ./flumecount.jar
 */

object FlumeEventCount {

  var list = ArrayBuffer[String]()

  def main(args: Array[String]) {

    StreamingExamples.setStreamingLogLevels()

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
//    sparkConf.setMaster("spark://localhost:7077")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, "localhost", 33333, StorageLevel.MEMORY_ONLY_SER_2)

    val bodys = stream.map(e => byteBufferToString(e.event.getBody))
    val results = bodys.map(line => toMysql(line))
    results.count.print
//
//    if(list.size >= 200){
//      print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
//      val cache = ssc.sparkContext.parallelize(list,1)
//
//      cache.saveAsTextFile("hdfs://localhost:9000/cache/"+System.currentTimeMillis())
//      list.clear()
//    }
    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 将ByteBuffer数据转换为string
   * @param buffer
   * @return
   */
  def byteBufferToString(buffer: ByteBuffer): String = {
    var re = ""
    try {
      val charset = Charset.forName("UTF-8")
      val decoder = charset.newDecoder()
      re = decoder.decode(buffer).toString

    } catch {
      case ex : Exception => print("字符转换错误")
    }
    re
  }

  def toMysql(line:String)={
    list.append(line)
    println("-------------------------------buffer size("+list.size+") --------------------------------")
    if(list.size >= 100) {
//      utils.MySql.batchInsert(list.toArray)
      list.clear()
    }
  }

  def toMysql(data:Array[String])={
//    utils.MySql.batchInsert(data.toArray)
    list.clear()

  }

  def toArray(line:String)={
    list.append(line)
    println("-------------------------------buffer size("+list.size+") --------------------------------")
  }
}
