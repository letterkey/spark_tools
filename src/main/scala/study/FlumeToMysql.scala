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

import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.lib.db.DBOutputFormat

import org.apache.hadoop.mapreduce.lib.db.DBConfiguration

object FlumeToMysql {

  var buffer = new ArrayBuffer[String]()

  def main(args: Array[String]) {


    val batchInterval = Milliseconds(2000)
    val conf = new Configuration()

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
    //    sparkConf.setMaster("spark://localhost:7077")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, "localhost", 33333, StorageLevel.MEMORY_ONLY_SER_2)

    val cache = stream.map(e => byteBufferToString(e.event.getBody)).map(line => toMysql(line)).count().print()


    stream.map(e => byteBufferToString(e.event.getBody)).map((1,_)).reduceByKey(_ + _).saveAsNewAPIHadoopFiles("","",classOf[Any],classOf[Any],classOf[org.apache.hadoop.mapred.lib.db.DBOutputFormat[Any,Any]],conf)


    val words = stream.map(e=>e.event.toString()).filter(_.split("\\|").length > 23).filter(_.contains("|2014")).map(_.split("\\|") (14)).map((_,1)).reduceByKey(_ + _)


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

  def toArray(line:String): Unit ={
    buffer.+(line)
  }
  def toMysql(line :String)={
    if(line != ""){
      val array = line.split("\t")
//      utils.MySql.insert(1,line)
    }
  }
}
